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

"""Define APIs for the servicegroup access."""

from nova import flags
from nova.openstack.common import cfg
from nova.openstack.common import importutils
from nova.openstack.common import log as logging
from nova import utils

from random import choice


LOG = logging.getLogger(__name__)
_default_driver = 'nova.servicegroup.db_driver.DbDriver'
servicegroup_driver_opt = cfg.StrOpt('servicegroup_driver',
                                   default=_default_driver,
                                   help='The driver for servicegroup service.')

FLAGS = flags.FLAGS
FLAGS.register_opt(servicegroup_driver_opt)


class API(object):

    _driver = None

    def __new__(cls, *args, **kwargs):

        if not cls._driver:
            LOG.debug(_('ServiceGroup driver defined as an instance of %s '),
                      str(FLAGS.servicegroup_driver))
            driver_class = FLAGS.servicegroup_driver
            # Check if ZooKeeper is installed
            if(driver_class.endswith('ZK_Driver')):
                try:
                    import zookeeper
                except ImportError:
                    LOG.warn(_('Cannot import zookeeper, ServiceGroup driver '
                               'will fall back to the DB driver: %s'),
                             str(_default_driver))
                    driver_class = _default_driver
            cls._driver = importutils.import_object(driver_class)
            utils.check_isinstance(cls._driver, ServiceGroupDriver)
            # we don't have to check that cls._driver is not NONE,
            # check_isinstance does it
        return super(API, cls).__new__(cls)

    def join(self, member_id, group_id, service=None):
        """Add a new member to the ServiceGroup

        @param member_id: the joined member ID
        @param group_id: the group name, of the joined member
        @param service: the parameter can be used for notifications about
        disconnect mode and update some internals
        """
        msg = _('Join new ServiceGroup member %(member_id)s to the '
                '%(group_id)s group, service = %(service)s')
        LOG.debug(msg, locals())
        return self._driver.join(member_id, group_id, service)

    def subscribe_to_changes(self, groups):
        """Subscribing to cache changes"""
        LOG.debug(_('Subscribing to changes in the %s ServiceGroup groups'),
                  str(groups))
        return self._driver.subscribe_to_changes(groups)

    def service_is_up(self, member):
        """Check if the given member is up"""
        msg = _('Check if the given member [%s] is part of the '
                'ServiceGroup, is up')
        LOG.debug(msg, member)
        return self._driver.is_up(member)

    def leave(self, member_id, group_id):
        """Explicitly remove the given member from the ServiceGroup
        monitoring.
        """
        msg = _('Explicitly remove the given member %(member_id)s from the'
                '%(group_id)s group monitoring')
        LOG.debug(msg, locals())
        return self._driver.leave(member_id, group_id)

    def get_all(self, group_id):
        """Returns ALL members of the given group."""
        LOG.debug(_('Returns ALL members of the [%s] '
                    'ServiceGroup'), group_id)
        return self._driver.get_all(group_id)

    def get_one(self, group_id):
        """Returns one member of the given group. The strategy to select
        the member is decided by the driver (e.g. random or round-robin).
        """
        LOG.debug(_('Returns one member of the [%s] group'), group_id)
        return self._driver.get_one(group_id)


class ServiceGroupDriver(object):
    """Base class for ServiceGroup drivers."""

    def join(self, member_id, group_id, service=None):
        """Join the given service with it's group"""
        raise NotImplementedError()

    def subscribe_to_changes(self, groups):
        """Subscribe to changes under given groups"""
        raise NotImplementedError()

    def is_up(self, member):
        """ Check whether the given member is up. """
        raise NotImplementedError()

    def leave(self, member_id, group_id):
        """Remove the given member from the ServiceGroup monitoring"""
        raise NotImplementedError()

    def get_all(self, group_id):
        """Returns ALL members of the given group"""
        raise NotImplementedError()

    def get_one(self, group_id):
        """The default behavior of get_one is to randomly pick one from
        the result of get_all(). This is likely to be overridden in the
        actual driver implementation.
        """
        members = self.get_all(group_id)
        if members is None:
            return None
        length = len(members)
        if length == 0:
            return None
        return choice(members)
