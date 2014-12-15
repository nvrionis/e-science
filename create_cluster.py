#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script creates a cluster on ~okeanos.

@author: Ioannis Stenos, Nick Vrionis, George Tzelepis
"""
import logging
import sys
import django
import datetime
from argparse import ArgumentParser, ArgumentTypeError
from sys import argv, exit
from time import sleep
from kamaki.clients import ClientError
from os.path import join, expanduser, dirname, abspath
import os
sys.path.append(join(dirname(__file__), 'ember_django/backend'))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
from django_db_after_login import *
from get_flavors_quotas import *
from reroute_ssh import reroute_ssh_prep
from run_ansible_playbooks import install_yarn
from okeanos_utils import Cluster, check_credentials, endpoints_and_user_id, \
    init_cyclades, init_cyclades_netclient, init_plankton, get_project_id, \
    destroy_cluster, get_user_quota
from cluster_errors_constants import *



# Default values for YarnCluster creation.
_defaults = {
    'auth_url': 'https://accounts.okeanos.grnet.gr/identity/v2.0',
    'image': 'Debian Base',
    'logging': 'summary'
}


class _ArgCheck(object):
    """
    Used for type checking arguments supplied for use with type= and
    choices= argparse attributes
    """

    def __init__(self):
        self.logging_levels = {
            'critical': logging.CRITICAL,
            'error': logging.ERROR,
            'warning': logging.WARNING,
            'summary': SUMMARY,
            'report': REPORT,
            'info': logging.INFO,
            'debug': logging.DEBUG,
        }
        logging.addLevelName(REPORT, "REPORT")
        logging.addLevelName(SUMMARY, "SUMMARY")

    def unsigned_int(self, val):
        """
        :param val: int
        :return: val if val > 0 or raise exception
        """
        ival = int(val)
        if ival <= 0:
            raise ArgumentTypeError(" %s must be a positive number." % val)
        return ival

    def two_or_bigger(self, val):
        """
        :param val: int
        :return: val if > 2 or raise exception
        """
        ival = int(val)
        if ival < 2:
            raise ArgumentTypeError(" %s must be at least 2." % val)
        return ival

    def five_or_bigger(self, val):
        ival = int(val)
        if ival < 5:
            raise ArgumentTypeError(" %s must be at least 5." % val)
        return ival


class YarnCluster(object):
    """
    Class for create hadoop-yarn cluster functionality
    """

    def __init__(self, opts):
        """Initialization of YarnCluster data attributes"""
        self.opts = opts
        # Master VM ip, placeholder value
        self.HOSTNAME_MASTER_IP = '127.0.0.1'
        # master VM root password file, placeholder value
        self.pass_file = 'PLACEHOLDER'
        # List of cluster VMs
        self.server_dict = {}
        # project id of project name given as argument
        self.project_id = get_project_id(self.opts['token'],
                                         self.opts['project_name'])
        self.status = {}
        self.user = db_after_login(self.opts['token'], login=False)
        # Instance of an AstakosClient object
        self.auth = check_credentials(self.opts['token'],
                                      self.opts.get('auth_url',
                                                    _defaults['auth_url']))
        # Check if project has actual quota
        if self.check_project_quota() != 0:
            msg = 'Project %s exists but you have no quota to request' % \
                self.opts['project_name']
            raise ClientError(msg, error_project_quota)
        # ~okeanos endpoints and user id
        self.endpoints, self.user_id = endpoints_and_user_id(self.auth)

        # Instance of CycladesClient
        self.cyclades = init_cyclades(self.endpoints['cyclades'],
                                      self.opts['token'])
        # Instance of CycladesNetworkClient
        self.net_client = init_cyclades_netclient(self.endpoints['network'],
                                                  self.opts['token'])
        # Instance of Plankton/ImageClient
        self.plankton = init_plankton(self.endpoints['plankton'],
                                      self.opts['token'])
        self._DispatchCheckers = {}
        self._DispatchCheckers[len(self._DispatchCheckers) + 1] =\
            self.check_cluster_size_quotas
        self._DispatchCheckers[len(self._DispatchCheckers) + 1] =\
            self.check_network_quotas
        self._DispatchCheckers[len(self._DispatchCheckers) + 1] =\
            self.check_ip_quotas
        self._DispatchCheckers[len(self._DispatchCheckers) + 1] =\
            self.check_cpu_valid
        self._DispatchCheckers[len(self._DispatchCheckers) + 1] =\
            self.check_ram_valid
        self._DispatchCheckers[len(self._DispatchCheckers) + 1] =\
            self.check_disk_valid

    def check_cluster_size_quotas(self):
        """
        Checks if the user quota is enough to create the requested number
        of VMs.
        """
        dict_quotas = get_user_quota(self.auth)
        pending_vm = retrieve_pending_clusters(self.opts['token'],
                                               self.opts['project_name'])['VMs']
        limit_vm = dict_quotas[self.project_id]['cyclades.vm']['limit']
        usage_vm = dict_quotas[self.project_id]['cyclades.vm']['usage']
        available_vm = limit_vm - usage_vm - pending_vm
        if available_vm < self.opts['cluster_size']:
            msg = ' Cyclades VMs out of limit'
            raise ClientError(msg, error_quotas_cluster_size)
        else:
            return 0

    def check_network_quotas(self):
        """
        Checks if the user quota is enough to create a new private network
        Subtracts the number of networks used and pending from the max allowed
        number of networks
        """
        dict_quotas = get_user_quota(self.auth)
        pending_net = retrieve_pending_clusters(self.opts['token'],
                                               self.opts['project_name'])['Network']
        limit_net = dict_quotas[self.project_id]['cyclades.network.private']['limit']
        usage_net = dict_quotas[self.project_id]['cyclades.network.private']['usage']
        available_networks = limit_net - usage_net - pending_net
        if available_networks >= 1:
            logging.log(REPORT, ' Private Network quota is ok')
            return 0
        else:
            msg = ' Private Network quota exceeded'
            raise ClientError(msg, error_quotas_network)

    def check_ip_quotas(self):
        """Checks user's quota for unattached public ips."""
        dict_quotas = get_user_quota(self.auth)
        list_float_ips = self.net_client.list_floatingips()
        pending_ips = retrieve_pending_clusters(self.opts['token'],
                                               self.opts['project_name'])['Ip']
        limit_ips = dict_quotas[self.project_id]['cyclades.floating_ip']['limit']
        usage_ips = dict_quotas[self.project_id]['cyclades.floating_ip']['usage']
        available_ips = limit_ips - usage_ips - pending_ips
        for d in list_float_ips:
            if d['instance_id'] is None and d['port_id'] is None:
                available_ips += 1
        if available_ips > 0:
            return 0
        else:
            msg = ' Floating IP not available'
            raise ClientError(msg, error_get_ip)

    def check_cpu_valid(self):
        """
        Checks if the user quota is enough to bind the requested cpu resources.
        Subtracts the number of cpus used and pending from the max allowed
        number of cpus.
        """
        dict_quotas = get_user_quota(self.auth)
        pending_cpu = retrieve_pending_clusters(self.opts['token'],
                                               self.opts['project_name'])['Cpus']
        limit_cpu = dict_quotas[self.project_id]['cyclades.cpu']['limit']
        usage_cpu = dict_quotas[self.project_id]['cyclades.cpu']['usage']
        available_cpu = limit_cpu - usage_cpu - pending_cpu
        cpu_req = self.opts['cpu_master'] + \
            self.opts['cpu_slave'] * (self.opts['cluster_size'] - 1)
        if available_cpu < cpu_req:
            msg = ' Cyclades cpu out of limit'
            raise ClientError(msg, error_quotas_cpu)
        else:
            return 0

    def check_ram_valid(self):
        """
        Checks if the user quota is enough to bind the requested ram resources.
        Subtracts the number of ram used and pending from the max allowed
        number of ram.
        """
        dict_quotas = get_user_quota(self.auth)
        pending_ram = retrieve_pending_clusters(self.opts['token'],
                                               self.opts['project_name'])['Ram']
        limit_ram = dict_quotas[self.project_id]['cyclades.ram']['limit']
        usage_ram = dict_quotas[self.project_id]['cyclades.ram']['usage']
        available_ram = (limit_ram - usage_ram) / Bytes_to_MB - pending_ram
        ram_req = self.opts['ram_master'] + \
            self.opts['ram_slave'] * (self.opts['cluster_size'] - 1)
        if available_ram < ram_req:
            msg = ' Cyclades ram out of limit'
            raise ClientError(msg, error_quotas_ram)
        else:
            return 0

    def check_disk_valid(self):
        """
        Checks if the requested disk resources are available for the user.
        Subtracts the number of disk used and pending from the max allowed
        disk size.
        """
        dict_quotas = get_user_quota(self.auth)
        pending_cd = retrieve_pending_clusters(self.opts['token'],
                                               self.opts['project_name'])['Disk']
        limit_cd = dict_quotas[self.project_id]['cyclades.disk']['limit']
        usage_cd = dict_quotas[self.project_id]['cyclades.disk']['usage']
        cyclades_disk_req = self.opts['disk_master'] + \
            self.opts['disk_slave'] * (self.opts['cluster_size'] - 1)
        available_cyclades_disk_GB = (limit_cd - usage_cd) / Bytes_to_GB - pending_cd
        if available_cyclades_disk_GB < cyclades_disk_req:
            msg = ' Cyclades disk out of limit'
            raise ClientError(msg, error_quotas_cyclades_disk)
        else:
            return 0

    def check_all_resources(self):
        """
        Checks user's quota if every requested resource is available.
        Returns zero if everything available.
        """
        for checker in [func for (order, func) in sorted(self._DispatchCheckers.items())]:
            # for k, checker in self._DispatchCheckers.iteritems():
            retval = checker()
        return 0

    def get_flavor_id_master(self, cyclades_client):
        """
        Return the flavor id for the master based on cpu,ram,disk_size and
        disk template
        """
        try:
            flavor_list = cyclades_client.list_flavors(True)
        except ClientError:
            msg = ' Could not get list of flavors'
            raise ClientError(msg, error_flavor_list)
        flavor_id = 0
        for flavor in flavor_list:
            if flavor['ram'] == self.opts['ram_master'] and \
                            flavor['SNF:disk_template'] == self.opts['disk_template'] and \
                            flavor['vcpus'] == self.opts['cpu_master'] and \
                            flavor['disk'] == self.opts['disk_master']:
                flavor_id = flavor['id']

        return flavor_id

    def get_flavor_id_slave(self, cyclades_client):
        """
        Return the flavor id for the slave based on cpu,ram,disk_size and
        disk template
        """
        try:
            flavor_list = cyclades_client.list_flavors(True)
        except ClientError:
            msg = ' Could not get list of flavors'
            raise ClientError(msg, error_flavor_list)
        flavor_id = 0
        for flavor in flavor_list:
            if flavor['ram'] == self.opts['ram_slave'] and \
                            flavor['SNF:disk_template'] == self.opts['disk_template'] and \
                            flavor['vcpus'] == self.opts['cpu_slave'] and \
                            flavor['disk'] == self.opts['disk_slave']:
                flavor_id = flavor['id']

        return flavor_id

    def create_password_file(self, master_root_pass, master_name):
        """
        Creates a file named after the timestamped name of master node
        containing the root password of the master virtual machine of
        the cluster.
        """
        self.pass_file = join('./', master_name + '_root_password')
        self.pass_file = self.pass_file.replace(" ", "")
        with open(self.pass_file, 'w') as f:
            f.write(master_root_pass)

    def check_project_quota(self):
        """Checks that for a given project actual quota exist"""
        dict_quotas = get_user_quota(self.auth)
        if self.project_id in dict_quotas:
            return 0
        return error_project_quota

    def create_bare_cluster(self):
        """Creates a bare ~okeanos cluster."""
        # Finds user public ssh key
        USER_HOME = expanduser('~')
        chosen_image = {}
        pub_keys_path = join(USER_HOME, ".ssh/id_rsa.pub")
        logging.log(SUMMARY, ' Authentication verified')
        flavor_master = self.get_flavor_id_master(self.cyclades)
        flavor_slaves = self.get_flavor_id_slave(self.cyclades)
        if flavor_master == 0 or flavor_slaves == 0:
            msg = ' Combination of cpu, ram, disk and disk_template do' \
                ' not match an existing id'
            raise ClientError(msg, error_flavor_id)
        list_current_images = self.plankton.list_public(True, 'default')
        # Check availability of resources
        retval = self.check_all_resources()
        # Find image id of the operating system arg given
        for lst in list_current_images:
            if lst['name'] == self.opts['image']:
                chosen_image = lst
                break
        if not chosen_image:
            msg = self.opts['image']+' is not a valid image'
            raise ClientError(msg, error_image_id)
        logging.log(SUMMARY, ' Creating ~okeanos cluster')

        # Create timestamped name of the cluster
        date_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cluster_name = '%s%s%s' % (date_time, '-', self.opts['name'])
        self.opts['name'] = cluster_name

        # Update db with cluster status as pending
        db_cluster_create(self.opts['token'], self.opts)
        try:
            cluster = Cluster(self.cyclades, self.opts['name'],
                              flavor_master, flavor_slaves,
                              chosen_image['id'], self.opts['cluster_size'],
                              self.net_client, self.auth, self.project_id)

            self.HOSTNAME_MASTER_IP, self.server_dict = \
                cluster.create('', pub_keys_path, '')
            sleep(15)
        except Exception:
            # If error in bare cluster, update cluster status as destroyed
            db_cluster_update(self.opts['token'], "Destroyed", self.opts['name'])
            raise
        # wait for the machines to be pingable
        logging.log(SUMMARY, ' ~okeanos cluster created')
        # Get master VM root password
        master_root_pass = self.server_dict[0]['adminPass']
        master_name = self.server_dict[0]['name']
        # Write master VM root password to a file with same name as master VM
        self.create_password_file(master_root_pass, master_name)
        # Return master node ip and server dict
        return self.HOSTNAME_MASTER_IP, self.server_dict

    def create_yarn_cluster(self):
        """Create Yarn cluster"""
        self.HOSTNAME_MASTER_IP, self.server_dict = self.create_bare_cluster()
        logging.log(SUMMARY, ' Creating Yarn cluster')
        try:
            list_of_hosts = reroute_ssh_prep(self.server_dict,
                                             self.HOSTNAME_MASTER_IP)

            logging.log(SUMMARY, ' Installing and configuring Yarn')
            install_yarn(list_of_hosts, self.HOSTNAME_MASTER_IP,
                         self.server_dict[0]['name'])
            logging.log(SUMMARY, ' The root password of master VM [%s] '
                        'is on file %s', self.server_dict[0]['name'],
                        self.pass_file)
            # If Yarn cluster is build, update cluster status as active
            db_cluster_update(self.opts['token'], "Active", self.opts['name'],
                              self.HOSTNAME_MASTER_IP)
            return self.HOSTNAME_MASTER_IP, self.server_dict
        except Exception:
            logging.error(' An unrecoverable error occured. Created cluster'
                          ' and resources will be deleted')
            # If error in Yarn cluster, update cluster status as destroyed
            db_cluster_update(self.opts['token'], "Destroyed", self.opts['name'])
            self.destroy()
            raise

    def destroy(self):
        """Destroy Cluster"""
        destroy_cluster(self.opts['token'], self.HOSTNAME_MASTER_IP)


def main(opts):
    """
    The main function calls create_yarn_cluster with
    the arguments given from command line.
    """
    try:
        c_yarn_cluster = YarnCluster(opts)
        c_yarn_cluster.create_yarn_cluster()
    except ClientError, e:
        logging.error(' Fatal error:' + e.message)
        exit(error_fatal)
    except Exception, e:
        logging.error(' Fatal error:' + str(e.args[0]))
        exit(error_fatal)


if __name__ == "__main__":
    parser = ArgumentParser()
    checker = _ArgCheck()
    parser.add_argument("--name", help='The specified name of the cluster.'
                        ' Will be prefixed by a timestamp',
                        dest='name', required=True)

    parser.add_argument("--cluster_size", help='Total number of cluster nodes',
                        dest='cluster_size', type=checker.two_or_bigger,
                        required=True)

    parser.add_argument("--cpu_master", help='Number of cpu cores for the master node',
                        dest='cpu_master', type=checker.unsigned_int,
                        required=True)

    parser.add_argument("--ram_master", help='Size of RAM (MB) for the master node',
                        dest='ram_master', type=checker.unsigned_int,
                        required=True)

    parser.add_argument("--disk_master", help='Disk size (GB) for the master node',
                        dest='disk_master', type=checker.five_or_bigger,
                        required=True)

    parser.add_argument("--cpu_slave", help='Number of cpu cores for the slave node(s)',
                        dest='cpu_slave', type=checker.unsigned_int,
                        required=True)

    parser.add_argument("--ram_slave", help='Size of RAM (MB) for the slave node(s)',
                        dest='ram_slave', type=checker.unsigned_int,
                        required=True)

    parser.add_argument("--disk_slave", help='Disk size (GB) for the slave node(s)',
                        dest='disk_slave', type=checker.five_or_bigger,
                        required=True)

    parser.add_argument("--disk_template", help='Disk template',
                        dest='disk_template',
                        choices=['drbd', 'ext_vlmc'], required=True)

    parser.add_argument("--image", help='OS for the cluster.'
                        ' Default is Debian Base', dest='image',
                        default=_defaults['image'])

    parser.add_argument("--token", help='Synnefo authentication token',
                        dest='token', required=True)

    parser.add_argument("--auth_url", nargs='?', dest='auth_url',
                        default=_defaults['auth_url'],
                        help='Synnefo authentication url. Default is ' +
                        auth_url)

    parser.add_argument("--logging", dest='logging',
                        default=_defaults['logging'],
                        choices=checker.logging_levels.keys(),
                        help='Logging Level. Default: summary')

    parser.add_argument("--project_name", help='~okeanos project name'
                        ' to request resources from ',
                        dest='project_name', required=True)
    if len(argv) > 1:
        opts = vars(parser.parse_args(argv[1:]))
        if opts['logging'] == 'debug':
            log_directory = dirname(abspath(__file__))
            log_file_path = join(log_directory, "create_cluster_debug.log")

            logging.basicConfig(format='%(asctime)s:%(message)s',
                                filename=log_file_path,
                                level=logging.DEBUG, datefmt='%H:%M:%S')
            print ' Creating Hadoop cluster, logs will' + \
                  ' be appended in create_cluster_debug.log'
        else:
            logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                                level=checker.logging_levels[opts['logging']],
                                datefmt='%H:%M:%S')
        main(opts)
    else:
        logging.error('No arguments were given')
        exit(error_no_arguments)
