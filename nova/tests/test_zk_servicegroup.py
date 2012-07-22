# Copyright (c) IBM 2012 Alexey Roytman <roytman at il dot ibm dot com>.
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
from nova.openstack.common import cfg
from nova import servicegroup
from nova import test

try:
    import zookeeper
    _zk_installed = True
except ImportError:
    _zk_installed = False
_zk_installed = True
FLAGS = flags.FLAGS


class ZKServiceGroupTestCase(test.TestCase):

    def setUp(self):
        super(ZKServiceGroupTestCase, self).setUp()
        servicegroup.API._driver = None
        self.flags(servicegroup_driver='nova.servicegroup.zk_driver.ZK_Driver')
        FLAGS.zk_servers = 'localhost:2181'
        FLAGS.zk_log_file = './zk.log'
        FLAGS.zk_conn_refresh = 15
        FLAGS.zk_recv_timeout = 6000

    @test.skip_unless(_zk_installed, 'Zookeeper is not supported')
    def testJOIN_is_up(self):
        self.servicegroup_api = servicegroup.API()
        service_id = {'topic': 'unittest', 'host': 'serviceA'}
        self.servicegroup_api.join(service_id['host'],
                                 service_id['topic'], None)
        eventlet.sleep(5)
        self.assertTrue(self.servicegroup_api.service_is_up(service_id))
        self.servicegroup_api.leave(None, service_id)
        eventlet.sleep(5)
        self.assertFalse(self.servicegroup_api.service_is_up(service_id))

    @test.skip_unless(_zk_installed, 'Zookeeper is not supported')
    def testSubscribeToChanges(self):
        self.servicegroup_api = servicegroup.API()
        self.servicegroup_api.subscribe_to_changes(['unittest'])
        eventlet.sleep(5)
        service_id = {'topic': 'unittest', 'host': 'serviceA'}
        self.servicegroup_api.join(service_id['host'],
                                 service_id['topic'], None)
        eventlet.sleep(5)
        self.assertTrue(self.servicegroup_api.service_is_up(service_id))

    @test.skip_unless(_zk_installed, 'Zookeeper is not supported')
    def testStop(self):
        self.servicegroup_api = servicegroup.API()
        service_id = {'topic': 'unittest', 'host': 'serviceA'}
        pulse = self.servicegroup_api.join(service_id['host'],
                                         service_id['topic'], None)
        eventlet.sleep(5)
        self.assertTrue(self.servicegroup_api.service_is_up(service_id))
        pulse.stop()
        eventlet.sleep(5)
        self.assertFalse(self.servicegroup_api.service_is_up(service_id))
