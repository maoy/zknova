# Copyright (c) IBM 2012 Pavel Kravchenco <kpavel at il dot ibm dot com>
#                        Alexey Roytman <roytman at il dot ibm dot com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from nova import db
from nova import exception
from nova import flags
from nova import log as logging
from nova.membership import api
from nova import utils


FLAGS = flags.FLAGS
LOG = logging.getLogger(__name__)


class DB_Driver(api.MemberShipDriver):

    def join(self, ctxt, host, group, binary, report_interval):
        """Join the given service with it's group"""
        LOG.debug(_('DB_Driver: join new membership member %(id)s to the \
%(gr)s group report_interval = %(ri)d'),
                  {'id': host, 'gr': group, 'ri': report_interval})
        print 'IN MEMBERSHIP_DB_JOIN'
        if report_interval:
            pulse = utils.LoopingCall(self._report_state, ctxt,
                                      host, binary, group)
            pulse.start(interval=report_interval,
                        initial_delay=report_interval)
            return pulse

    def subscribe_to_changes(self, groups):
        """
        Subscribe to changes under given groups: no need in the db_backend
        """
        return

    def is_up(self, service_ref):
        """moved from nova.utils
            Check whether a service is up based on last heartbeat.
        """
        last_heartbeat = service_ref['updated_at'] or service_ref['created_at']
        # Timestamps in DB are UTC.
        elapsed = utils.total_seconds(utils.utcnow() - last_heartbeat)
        return abs(elapsed) <= FLAGS.service_down_time

    #TODO(roytman) check the next 2 methods
    def _report_state(self, ctxt, host, binary, group):
        """Update the state of this service in the datastore."""
        zone = FLAGS.node_availability_zone
        state_catalog = {}
        service_ref = self._get_service_ref(ctxt, host, binary, group)
        state_catalog['report_count'] = service_ref['report_count'] + 1
        if zone != service_ref['availability_zone']:
            state_catalog['availability_zone'] = zone

        db.service_update(ctxt, service_ref['id'], state_catalog)

    def _get_service_ref(self, context, host, binary, topic):
        """"""
        try:
            service_ref = db.service_get_by_args(context,
                                                 host,
                                                 binary)
        except exception.NotFound:
            zone = FLAGS.node_availability_zone
            service_ref = db.service_create(context,
                                        {'host': host,
                                         'binary': binary,
                                         'topic': topic,
                                         'report_count': 0,
                                         'availability_zone': zone})

        return service_ref
