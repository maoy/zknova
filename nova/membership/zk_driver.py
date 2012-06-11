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

import membership

from nova.common import evzookeeper
from collections import defaultdict
from nova import log as logging
from nova.membership import MemberShipDriver
from nova.common.evzookeeper import get_session

LOG = logging.getLogger(__name__)


class ZK_Driver(MemberShipDriver):
    
    _membership = {}
    _monitors = {}
    
#    def __init__(self):
#        zkservers = None
#        if(FLAGS.zk_servers):
#            zkservers = str(FLAGS.zk_servers)
#        else:
#            LOG.debug('no zkserver defined in flags, TODO: throw exception')
#
#        LOG.debug('zkservers defined in flags: ' + str(zkservers))
#    
#        self._membership = membership.Membership(ZKSession(zkservers))
    
    def monitor(self, members):
        """
        callback to update services cache.
        called when watched parent children been changed
        """
        self._services = members.copy()
        LOG.debug('Services Cache udpated:' + str(members.items()))
        
    
    def join(self, ctxt, host, group, binary, report_interval):
        LOG.debug('ZK_Driver.join to group = %s host= %s report_interval= %d ', str(group), str(host), report_interval)
        group = '/' + group
        self._membership[group + '/' + host] =  membership.Membership(get_session(report_interval), group, host)
        LOG.debug('out ZK_Driver.api.join. Membership: ' + str(self._membership))
    
    def subscribe_to_changes(self, groups):
        LOG.debug('ZK_Driver.subscribe_to_changes on groups %s ' , str(groups))
        
        sesion = get_session()
        for group in groups:            
            self._monitors[group] = membership.MembershipMonitor(sesion, group)
               
        LOG.debug('out ZK_Driver.subscribe_to_changes on groups Monitors: %s' , str(self._monitors))
        
    def is_up(self, service_ref):
        
        group = '/' + service_ref['topic']
        host = service_ref['host']
        LOG.debug('ZK_Driver.is_up group = %s host = %s', str(group), str(host))
        membership = self._monitors.get(group)
        
        if membership :
            nodes = membership.get_all()
        else :
            LOG.debug('ZK_Driver.is_up cached membership is None')
            try :
                nodes = get_session().get_children(group)
            except Exception as ex:
                LOG.exception('Unexpected error raised: %s', ex )
                return False
        
        LOG.debug('ZK_Driver.is_up nodes = %s', str(nodes))
        if host in nodes:
            LOG.debug('Service found in cache: ' + str(host))
            return True
        else:
            LOG.debug('Service not found in cache: ' + str(host))
            return False
            
    