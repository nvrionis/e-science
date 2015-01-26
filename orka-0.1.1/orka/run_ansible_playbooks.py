#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This script installs and configures a Hadoop-Yarn cluster using Ansible.

@author: Ioannis Stenos, Nick Vrionis
"""
import os
from os.path import dirname, abspath
import logging

# Definitions of return value errors
from cluster_errors_constants import error_ansible_playbook, REPORT, SUMMARY


def install_yarn(hosts_list, master_ip, cluster_name, hadoop_image):
    """
    Calls ansible playbook for the installation of yarn and all
    required dependencies. Also  formats and starts yarn.
    """
    list_of_hosts = hosts_list
    master_hostname = list_of_hosts[0]['fqdn'].split('.', 1)[0]
    hostname_master = master_ip
    cluster_size = len(list_of_hosts)
    # Create ansible_hosts file
    try:
        file_name = create_ansible_hosts(cluster_name, list_of_hosts,
                                         hostname_master)
    except Exception:
        msg = 'Error while creating ansible hosts file'
        raise RuntimeError(msg, error_ansible_playbook)
    # Run Ansible playbook
    run_ansible(file_name, cluster_size, hadoop_image)
    logging.log(SUMMARY, ' Yarn Cluster is active. You can access it through '
                + hostname_master + ':8088/cluster')
    os.system('rm /tmp/master_' + master_hostname + '_pub_key')


def create_ansible_hosts(cluster_name, list_of_hosts, hostname_master):
    """
    Function that creates the ansible_hosts file and
    returns the name of the file.
    """
    ansible_hosts_prefix = cluster_name.replace(" ", "_")

    # Removes spaces and ':' from cluster name and appends it to ansible_hosts
    # The ansible_hosts file will now have a timestamped name

    filename = os.getcwd() + '/ansible_hosts_' + ansible_hosts_prefix
    # Create ansible_hosts file and write all information that is
    # required from Ansible playbook.
    with open(filename, 'w+') as target:
        target.write('[master]' + '\n')
        target.write(list_of_hosts[0]['fqdn'])
        target.write(' private_ip='+list_of_hosts[0]['private_ip'])
        target.write(' ansible_ssh_host=' + hostname_master + '\n' + '\n')
        target.write('[slaves]'+'\n')

        for host in list_of_hosts[1:]:
            target.write(host['fqdn'])
            target.write(' private_ip='+host['private_ip'])
            target.write(' ansible_ssh_port='+str(host['port']))
            target.write(' ansible_ssh_host='+list_of_hosts[0]['fqdn'] + '\n')
    return filename


def run_ansible(filename, cluster_size, hadoop_image):
    """
    Calls the ansible playbook that installs and configures
    hadoop and everything needed for hadoop to be functional.
    Filename as argument is the name of ansible_hosts file.
    If a hadoop image was used in the VMs creation, ansible
    playbook will not install Hadoop-Yarn and will only perform
    the appropriate configuration.
    """
    logging.log(REPORT, ' Ansible starts Yarn installation on master and '
                        'slave nodes')
    # First time call of Ansible playbook install.yml executes tasks
    # required for hadoop installation on every virtual machine. Runs with
    # -f flag which is the fork argument of Ansible.
    level = logging.getLogger().getEffectiveLevel()
    # ansible_log file to write if logging level is
    # different than report or summary
    ansible_verbosity = ""
    ansible_log = " > ansible.log"
    if level == REPORT or level == SUMMARY or level == logging.INFO:
        ansible_log = ""
    elif level == logging.DEBUG:
        ansible_verbosity = " -vv"
        log_file_path = os.path.join(os.getcwd(), "create_cluster_debug.log")
        ansible_log = " >> " + log_file_path
    if level == logging.INFO:
        ansible_verbosity = " -v"
    orka_dir = dirname(abspath(__file__))
    ansible_path = orka_dir + '/ansible/site.yml'

    if hadoop_image:
        exit_status = os.system('export ANSIBLE_HOST_KEY_CHECKING=False;'
                                'ansible-playbook -i ' + filename + ' ' +
                                ansible_path + ansible_verbosity +
                                ' -f ' + str(cluster_size) +
                                ' -e "choose_role=yarn format=True start_yarn=True" -t postconfig'
                                + ansible_log)
    else:
        exit_status = os.system('export ANSIBLE_HOST_KEY_CHECKING=False;'
                                'ansible-playbook -i ' + filename + ' ' +
                                ansible_path + ansible_verbosity +
                                ' -f ' + str(cluster_size) +
                                ' -e "choose_role=yarn format=True start_yarn=True"'
                                + ansible_log)
    if exit_status != 0:
        msg = ' Ansible failed with exit status %d' % exit_status
        raise RuntimeError(msg, exit_status)
