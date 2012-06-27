# Copyright (c) 2011-2012 Alexey Roytman <roytman at il dot ibm dot com>.
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
from nova import flags
from nova import membership
from nova.openstack.common import cfg
from nova import test

FLAGS = flags.FLAGS


class ZKMembershipTestCase(test.TestCase):

    def setUp(self):
        super(ZKMembershipTestCase, self).setUp()
        self.flags(membership_driver='nova.membership.zk_driver.ZK_Driver')
        FLAGS.zk_servers = 'localhost:2181'
        FLAGS.zk_log_file = './zk.log'
        FLAGS.zk_conn_refresh = 15
        FLAGS.zk_recv_timeout = 6000
        self.membership_api = membership.API()

    def testJOIN_is_up(self):
        service_id = {'topic': 'unittest', 'host': 'serviceA'}
        print 'a'
        self.membership_api.join(service_id['host'],
                                 service_id['topic'], None)
        eventlet.sleep(5)
        self.assertTrue(self.membership_api.service_is_up(service_id))
        self.membership_api.leave(None, service_id)
        eventlet.sleep(5)
        self.assertFalse(self.membership_api.service_is_up(service_id))

    def testSubscribeToChanges(self):
        self.membership_api.subscribe_to_changes(['unittest'])
        eventlet.sleep(5)
        service_id = {'topic': 'unittest', 'host': 'serviceA'}
        self.membership_api.join(service_id['host'],
                                 service_id['topic'], None)
        eventlet.sleep(5)
        self.assertTrue(self.membership_api.service_is_up(service_id))

    def testStop(self):
        service_id = {'topic': 'unittest', 'host': 'serviceA'}
        pulse = self.membership_api.join(service_id['host'],
                                         service_id['topic'], None)
        eventlet.sleep(5)
        self.assertTrue(self.membership_api.service_is_up(service_id))
        pulse.stop()
        eventlet.sleep(5)
        self.assertFalse(self.membership_api.service_is_up(service_id))
