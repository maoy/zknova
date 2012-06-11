import sys
import logging

logging.basicConfig(level=logging.DEBUG)
import eventlet

from nova.common import evzookeeper
from nova.membership import membership
#from nova.common.evzookeeper import membership
  
class ZKTestser:
    
    def __init__(self):
        self._session= evzookeeper.ZKSession("localhost:2181", zklog_fd=sys.stderr)
        self._monitor = membership.MembershipMonitor(self._session, "/test3")
        print 'after monitor'
        self._monitor.subscribe_to_changes(self.print_members)
        
    def print_members(self, members):
        print members
        
    def getAll(self):
        self._monitor.get_all()
    

tester = ZKTestser()

print "All members = " + str(tester.getAll())

#def print_members(members):
#    print members
    
#monitor.subscribe_to_changes(print_members)
#print 'after subscribe_to_changes'

#membership = membership.Membership(session, "/test3", "a1")

#print 'after membership'
eventlet.sleep(20)
print "All members = " + str(tester.getAll())
eventlet.sleep(200)
