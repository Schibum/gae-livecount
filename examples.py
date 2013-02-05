import logging
from datetime import datetime

#from google.appengine.ext import webapp
import webapp2 as webapp
#from google.appengine.ext.webapp import util

from livecount import counter
#from livecount.counter import LivecountCounter
from livecount.counter import PeriodType


def count(name):
    counter.load_and_increment_counter(name)


def advanced_count(name):
    counter.load_and_increment_counter(name, datetime.now(), period_types=[PeriodType.DAY, PeriodType.WEEK], delta=1)


class MainHandler(webapp.RequestHandler):
    def get(self):
        try:
            name = "visitor"
            counter.load_and_increment_counter(name)
        except Exception, e:
            logging.info(repr(e))
        self.response.out.write('Visitor: ' + str(counter.load_and_get_count(name)))


app = webapp.WSGIApplication(
[
    ('/examples', MainHandler),
], debug=True)

