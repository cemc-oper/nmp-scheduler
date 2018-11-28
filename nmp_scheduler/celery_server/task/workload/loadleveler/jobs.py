# coding: utf-8
from celery import group
from fabric import Connection

from nmp_scheduler.celery_server.celery import app


@app.task()
def get_jobs_task(param):
    user = param['user']
    password = param['password']
    host = param['host']
    port = param['port']

    config_dict = app.task_config.config

    project_dir = config_dict['aix']['hpc']['loadleveler_status']['project']['dir']
    project_program = config_dict['aix']['hpc']['loadleveler_status']['project']['program']
    project_script = config_dict['aix']['hpc']['loadleveler_status']['project']['script']

    host_for_connection = '{user}@{host}'.format(user=user, host=host)

    connection = Connection(host_for_connection, connect_kwargs={
        'password': password
    })

    def get_disk_usage(c):
        with c.cd(project_dir):
            c.run("{program} {script}".format(
                program=project_program,
                script=project_script,
            ))

    get_disk_usage(connection)


@app.task()
def get_group_jobs_task():
    config_dict = app.task_config.config

    group_tasks = config_dict['aix']['hpc']['loadleveler_status']['task_group']

    # celery task group
    g = group(get_jobs_task.s(param) for param in group_tasks)
    result = g.delay()
    return
