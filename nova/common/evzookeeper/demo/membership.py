# Copyright (c) 2011-2012 Yun Mao <yunmao at gmail dot com>.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import eventlet
import logging
import sys

from nova.common import evzookeeper
from nova.common.evzookeeper import membership
from nova.common.evzookeeper import ZKSession


logging.basicConfig(level=logging.DEBUG)


class NodeManager(object):

    def __init__(self, name, session):
        self.name = name
        self.membership = membership.Membership(session,
                                                "/basedir", name)
class NodeMonitor(object):
    def __init__(self, session):
        self.mon = membership.MembershipMonitor(session,
                                                "/basedir",
                                                cb_func=self.monitor)

    def monitor(self, members):
        print "in monitoring thread", members


def demo():
    session = ZKSession("localhost:2181", recv_timeout=4000,
                        zklog_fd=sys.stderr)
    _n = NodeMonitor(session)
    if len(sys.argv) > 1:
        _nm = NodeManager(sys.argv[1], session)
        eventlet.sleep(1000)
    else:
        _nm1 = NodeManager("node1", session)
        _nm2 = NodeManager("node2", session)
        eventlet.sleep(5)
        _nm3 = NodeManager("node3", session)
        eventlet.sleep(60)
        _nm4 = NodeManager("node4", session)
        eventlet.sleep(1000)

if __name__ == "__main__":
    demo()
