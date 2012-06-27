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

    def join(self, member_id, group, service=None):
        """Join the given service with it's group"""
        LOG.debug(_('DB_Driver: join new membership member %(id)s to the \
%(gr)s group, service= %(sr)s'),
                  {'id': member_id, 'gr': group, 'sr': str(service)})
        if service is None:
            raise RuntimeError(_('service is a mandatory argument for DB based\
membership driver'))
        report_interval = service.report_interval
        if report_interval:
            pulse = utils.LoopingCall(self._report_state, service)
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
        LOG.debug('DB_Driver.is_up last_heartbeat = %(lhb)s elapsed = %(el)s',
                  {'lhb': str(last_heartbeat), 'el': str(elapsed)})
        return abs(elapsed) <= FLAGS.service_down_time

    def _report_state(self, service):
        service.report_state()
