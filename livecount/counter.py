#
# Copyright 2011 Greg Bayer <greg@gbayer.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""
  Livecount is a memcache-based counter api with asynchronous writes to
  persist counts to datastore.

  Semantics:
   - Write-Behind
   - Read-Through
"""

from datetime import datetime, timedelta
import logging
import time

from google.appengine.api import memcache
from google.appengine.api import taskqueue
from google.appengine.ext import ndb
import webapp2 as webapp


class PeriodType(object):
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    YEAR = "year"
    ALL = "all"

    @staticmethod
    def find_scope(period_type, period):
        if period_type == PeriodType.SECOND:
            # 2011-06-13 18:11:32
            return str(period)[0:19]
        elif period_type == PeriodType.MINUTE:
            # 2011-06-13 18:11
            return str(period)[0:16]
        elif period_type == PeriodType.HOUR:
            # 2011-06-13 18
            return str(period)[0:13]
        elif period_type == PeriodType.DAY:
            # 2011-06-13
            return str(period)[0:10]
        elif period_type == PeriodType.WEEK:
            if not isinstance(period, datetime):
                period = PeriodType.str_to_datetime(period)
            # 2011-06-13week; use Monday as marker
            return str(period - timedelta(period.weekday()))[0:10] + "week"
        elif period_type == PeriodType.MONTH:
            # 2011-06
            return str(period)[0:7]
        elif period_type == PeriodType.YEAR:
            # 2011
            return str(period)[0:4]
        else:
            return "all"

    @staticmethod
    def str_to_datetime(datetime_str):
        time_format = "%Y-%m-%d %H:%M:%S"
        return datetime.fromtimestamp(time.mktime(
            time.strptime(datetime_str.split('.')[0], time_format)))


class LivecountCounter(ndb.model.Model):
    #namespace is replaced with parent
    #namespace = db.StringProperty(default="default")
    period_type = ndb.StringProperty(default=PeriodType.ALL)
    period = ndb.StringProperty()
    name = ndb.StringProperty()
    count = ndb.IntegerProperty()

    @staticmethod
    def get_key(parent_key, name, period_type, period):
        key_id = LivecountCounter.get_key_id(name, period_type, period)
        return ndb.Key(LivecountCounter, key_id, parent=parent_key)

    @staticmethod
    def get_key_id(name, period_type, period):
        scoped_period = PeriodType.find_scope(period_type, period)
        # I prefer name in front, since this is kind of the sort order I prefer
        # return period_type + "|" + scoped_period + "|" + name
        return name + '|' + period_type + "|" + scoped_period

    @staticmethod
    def parent_to_ns(parent_key):
        return parent_key.urlsafe() if parent_key else None

    @staticmethod
    def ns_to_parent(namespace):
        return ndb.Key(urlsafe=namespace) if namespace else None

    @classmethod
    def get(cls, parent_key, name, period_type=PeriodType.ALL,
            period=PeriodType.ALL):
        key = cls.get_key(parent_key, name, period_type, period)
        return key.get()

    @classmethod
    def counters_query(cls, parent_key, name, period_type, period=None):
        return cls.query(ancestor=parent_key).filter(
            cls.name == name,
            cls.period == period,
            cls.period_type == period_type)


def load_and_get_count(name, period_type='all', period=datetime.now(),
                       parent_key=None):
    # Try memcache first
    key_id = LivecountCounter.get_key_id(name, period_type, period)
    namespace = LivecountCounter.parent_to_ns(parent_key)
    count = memcache.get(key_id, namespace=namespace)

    # Not in memcache
    if count is None:
        # See if this counter already exists in the datastore
        full_key = LivecountCounter.get_key(parent_key, name, period_type,
                                            period)
        record = full_key.get()
        # If counter exists in the datastore, but is not currently in
        # memcache, add it
        if record:
            count = record.count
            memcache.add(key_id, count, namespace=namespace)

    return count


def load_and_increment_counter(name, period=None,
                               period_types=None, parent_key=None,
                               delta=1, batch_size=None):
    """
    Setting batch size allows control of how often a writeback worker is
    created. By default, this happens at every increment to ensure maximum
    durability. If there is already a worker waiting to write the value of a
    counter, another will not be created.
    """
    # Warning: There is a race condition here. If two processes try to load
    # the same value from the datastore, one's update may be lost.
    # TODO: Think more about whether we care about this...

    namespace = LivecountCounter.parent_to_ns(parent_key)
    if period_types is None:
        period_types = [PeriodType.ALL]
    if period is None:
        period = datetime.now()

    for period_type in period_types:
        current_count = None

        key_id = LivecountCounter.get_key_id(name, period_type, period)
        if delta >= 0:
            result = memcache.incr(key_id, delta, namespace=namespace)
        else:
        # Since increment by negative number is not supported,
            # convert to decrement
            result = memcache.decr(key_id, -delta, namespace=namespace)

        if result is None:
            # See if this counter already exists in the datastore
            full_key = LivecountCounter.get_key(parent_key, name, period_type,
                                                period)
            record = full_key.get()
            if record:
                # Load last value from datastore
                new_counter_value = record.count + delta
                # To match behavior of memcache.decr(), don't allow
                # negative values
                if new_counter_value < 0:
                    new_counter_value = 0
                memcache.add(key_id, new_counter_value, namespace=namespace)
                if batch_size:
                    current_count = record.count
            else:
                # Start new counter
                memcache.add(key_id, delta, namespace=namespace)
                if batch_size:
                    current_count = delta
        else:
            current_count = result
            # if batch_size:
            #     current_count = memcache.get(key_id, namespace=namespace)

        # If batch_size is set, only try creating one worker per batch
        if not batch_size or (batch_size and current_count % batch_size == 0):
            if memcache.add(key_id + '_dirty', delta, namespace=namespace):
                logging.info(
                    "Adding task to taskqueue. counter value = " + str(
                        memcache.get(key_id, namespace=namespace)))
                params = {'name': name, 'period': period,
                          'period_type': period_type}
                if namespace:
                    params['namespace'] = namespace
                taskqueue.add(queue_name='livecount-writebacks',
                              url='/livecount/worker',
                              params=params)


def load_and_decrement_counter(name, period=None,
                               period_types=None, parent_key=None,
                               delta=1, batch_size=None):
    load_and_increment_counter(name, period, period_types, parent_key, -delta,
                               batch_size)


def GetMemcacheStats():
    stats = memcache.get_stats()
    return stats


class LivecountCounterWorker(webapp.RequestHandler):
    def post(self):
        namespace = self.request.get('namespace') or None
        parent_key = LivecountCounter.ns_to_parent(namespace)
        period_type = self.request.get('period_type')
        period = self.request.get('period')
        name = self.request.get('name')

        key_id = LivecountCounter.get_key_id(name, period_type, period)

        memcache.delete(key_id + '_dirty', namespace=namespace)
        cached_count = memcache.get(key_id, namespace=namespace)
        if cached_count is None:
            logging.error('LivecountCounterWorker: Failure for partial key=%s',
                          key_id)
            return

        # add new row in datastore
        scoped_period = PeriodType.find_scope(period_type, period)
        LivecountCounter(parent=parent_key, id=key_id, period_type=period_type,
                         period=scoped_period, name=name,
                         count=cached_count).put()


# class WritebackAllCountersHandler(webapp.RequestHandler):
#     """
#     Writes back all counters from memory to the datastore
#     NOTE: currently not working - in_memory_counter is not there
#     """
#
#     def get(self):
#         namespace = self.request.get('namespace')
#         delete = self.request.get('delete')
#
#         logging.info(
#             "Writing back all counters from memory to the datastore. "
#             "Namespace=%s. Delete from memory=%s." % (
#                 str(namespace), str(delete)))
#         result = False
#         while not result:
#             result = in_memory_counter.WritebackAllCounters(
#                         namespace, delete)
#
#         self.response.out.write(
#             "Done. WritebackAllCounters succeeded = " + str(result))
#
#
# class ClearEntireCacheHandler(webapp.RequestHandler):
#     """
#     Clears entire memcache
#     NOTE: currently not working - in_memory_counter is not there
#     """
#
#     def get(self):
#         logging.info(
#             "Deleting all counters in memcache. Any counts not previously "
#             "flushed will be lost.")
#
#         result = in_memory_counter.ClearEntireCache()
#         self.response.out.write(
#             "Done. ClearEntireCache succeeded = " + str(result))
#

class RedirectToCounterAdminHandler(webapp.RequestHandler):
    """
    For convenience / demo purposes, redirect to counter admin page.
    """

    def get(self):
        self.redirect('/livecount/counter_admin')


app = webapp.WSGIApplication([
    ('/livecount/worker', LivecountCounterWorker),
    #('/livecount/writeback_all_counters', WritebackAllCountersHandler),
    #('/livecount/clear_entire_cache', ClearEntireCacheHandler),
    #('/', RedirectToCounterAdminHandler),
])
