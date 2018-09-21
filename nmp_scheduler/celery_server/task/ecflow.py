#!/usr/bin/env python3
# coding=utf-8
import datetime
import json
import gzip
import requests
from fabric.api import run, cd, execute, env
from celery import group

from nmp_scheduler.celery_server.celery import app


"""
ecflow Status
"""


@app.task()
def get_ecflow_status_task(repo):

    owner_name = repo['owner']
    repo_name = repo['repo']
    ecflow_host = repo['ecflow_host']
    ecflow_port = repo['ecflow_port']

    config_dict = app.task_config.config
    collector_host_user = config_dict['ecflow_status_task']['collector']['user']
    collector_host_password = config_dict['ecflow_status_task']['collector']['password']
    collector_host_host = config_dict['ecflow_status_task']['collector']['host']
    project_dir = config_dict['ecflow_status_task']['project']['dir']
    project_program = config_dict['ecflow_status_task']['project']['program']
    project_script = config_dict['ecflow_status_task']['project']['script']
    project_config = config_dict['ecflow_status_task']['project']['config']

    user = repo.get('user', collector_host_user)
    password = repo.get('password', collector_host_password)
    host = repo.get('host', collector_host_host)

    env_hosts = ['{user}@{host}'.format(user=user, host=host)]
    env_password = '{password}'.format(password=password)

    env.hosts = env_hosts
    env.password = env_password

    def get_ecflow_status():
        with cd(project_dir):
            run("{program} {script} --owner={owner} --repo={repo} --config={config} --host={ecflow_host} --port={ecflow_port} --verbose".format(
                program=project_program,
                script=project_script,
                owner=owner_name,
                repo=repo_name,
                config=project_config,
                ecflow_host=ecflow_host,
                ecflow_port=ecflow_port
            ))

    execute(get_ecflow_status)


@app.task()
def get_group_ecflow_status_task():
    config_dict = app.task_config.config

    repos = config_dict['group_ecflow_status_task']

    # celery task group
    g = group(get_ecflow_status_task.s(a_repo) for a_repo in repos)
    result = g.delay()
    return


if __name__ == "__main__":
    args = {
        'owner': 'nwp_xp',
        'repo': 'pi_nwpc_pd_bk',
        'ecflow_host': '10.40.140.16',
        'ecflow_port': '31071'
    }
    print(json.dumps(get_ecflow_status_task(args), indent=2))