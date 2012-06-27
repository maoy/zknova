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
from nova import context
from nova import db
from nova import flags
from nova import membership
from nova import service
from nova import test
from nova import utils

FLAGS = flags.FLAGS


class DBMembershipTestCase(test.TestCase):

    def setUp(self):
        super(DBMembershipTestCase, self).setUp()
        FLAGS.service_down_time = 3
        self.flags(enable_new_services=True)
        self.membership_api = membership.API()
        self._host = 'foo'
        self._binary = 'nova-fake'
        self._topic = 'unittest'
        self._ctx = context.get_admin_context()

    def test_DB_driver(self):
        serv = service.Service(self._host,
                               self._binary,
                               self._topic,
                               'nova.tests.test_service.FakeManager',
                               1, 1)
        serv.start()
        serv.report_state()

        service_ref = db.service_get_by_args(self._ctx,
                                             self._host,
                                             self._binary)

        self.assertTrue(self.membership_api.service_is_up(service_ref))
        eventlet.sleep(5)
        service_ref = db.service_get_by_args(self._ctx,
                                             self._host,
                                             self._binary)

        self.assertTrue(self.membership_api.service_is_up(service_ref))
        serv.stop()
        eventlet.sleep(5)
        service_ref = db.service_get_by_args(self._ctx,
                                             self._host,
                                             self._binary)
        self.assertFalse(self.membership_api.service_is_up(service_ref))
