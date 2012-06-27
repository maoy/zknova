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

from nova.common import evzookeeper
from nova.common.evzookeeper import get_session
from nova.common.evzookeeper import membership
from nova import log as logging
from nova.membership import api
from nova.utils import LoopingCall

LOG = logging.getLogger(__name__)


class ZK_Driver(api.MemberShipDriver):

    _memberships = {}
    _monitors = {}

    def join(self, member_id, group, service=None):
        """Join the given service with it's group"""
        LOG.debug(_('ZK_Driver: join new membership member %(id)s to the \
%(gr)s group, service= %(sr)s'),
                  {'id': member_id, 'gr': group, 'sr': str(service)})
        if not group.startswith('/'):
            group = '/' + group
        ms = membership.Membership(get_session(), group, member_id)
        self._memberships[group + '/' + member_id] = ms
        LOG.debug('exit ZK_Driver.api.join. Membership: %s' +
                  str(self._memberships))
        #TODO(roytman) add notification about disconnect mode
        return FakeLoopingCall(self, member_id, group)

    def subscribe_to_changes(self, groups):
        LOG.debug('ZK_Driver.subscribe_to_changes on groups %s ', str(groups))
        sesion = get_session()
        for group in groups:
            group = '/' + group
            self._monitors[group] = membership.MembershipMonitor(sesion, group)
        LOG.debug('exit ZK_Driver.subscribe_to_changes on groups Monitors: %s',
                   str(self._monitors))
        #TODO(roytman) add notification about disconnect mode

    def leave(self, host, group):
        """ Remove the given member from the membership monitoring
        """
        if not group.startswith('/'):
            group = '/' + group
        LOG.debug('ZK_Driver.leave %s from group %s ', str(host), str(group))
        key = group + '/' + host
        membership = self._memberships[key]
        if membership is not None:
            membership.leave()
            del self._memberships[key]
        else:
            LOG.debug('ZK_Driver.leave %(id)s has not joined to the %(gr)s \
group', {'id': host, 'gr': group})

    def is_up(self, service_ref):
        group = '/' + service_ref['topic']
        if not group.startswith('/'):
            group = '/' + group
        host = service_ref['host']
        LOG.debug('ZK_Driver.is_up group = %(gr)s host = %(id)s',
                  {'id': host, 'gr': group})
        monitor = self._monitors.get(group)
        if monitor:
            nodes = monitor.get_all()
        else:
            LOG.debug('ZK_Driver.is_up cached membership is None')
            try:
                nodes = get_session().get_children(group)
            except Exception as ex:
                LOG.exception('Unexpected error raised: %s', ex)
                return False
        LOG.debug('ZK_Driver.is_up nodes = %s', str(nodes))
        if host in nodes:
            LOG.debug('Service found in cache: ' + str(host))
            return True
        else:
            LOG.debug('Service not found in cache: ' + str(host))
            return False


class FakeLoopingCall (LoopingCall):
    """ The fake Looping Call implementation, created for backward
    compatibility with a membership based on DB
    """
    def __init__(self, driver, host, group):
        self._driver = driver
        self._group = group
        self._host = host

    def stop(self):
        self._driver.leave(self._host, self._group)

    def start(self):
        pass

    def wait(self):
        pass
