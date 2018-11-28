# coding=utf-8
from fabric import Connection
from celery import group

from nmp_scheduler.celery_server.celery import app


@app.task()
def get_hpc_disk_usage(param):
    user = param['user']
    password = param['password']
    host = param['host']
    port = param['port']

    config_dict = app.task_config.config

    project_dir = config_dict['aix']['hpc']['disk_usage']['project']['dir']
    project_program = config_dict['aix']['hpc']['disk_usage']['project']['program']
    project_script = config_dict['aix']['hpc']['disk_usage']['project']['script']

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
def get_group_hpc_disk_usage_task():
    config_dict = app.task_config.config

    group_tasks = config_dict['aix']['hpc']['disk_usage']['task_group']

    # celery task group
    g = group(get_hpc_disk_usage.s(param) for param in group_tasks)
    result = g.delay()
    return


@app.task()
def get_group_hpc_disk_space_task():
    config_dict = app.task_config.config

    group_tasks = config_dict['aix']['hpc']['disk_space']['task_group']

    # celery task group
    g = group(get_hpc_disk_space.s(param) for param in group_tasks)
    result = g.delay()
    return


if __name__ == "__main__":
    import os
    os.environ['MODE'] = 'develop'
    # print(task.hpc.get_group_hpc_loadleveler_status_task())
    # print(task.hpc.get_group_hpc_disk_usage_task.delay())
    print(get_group_hpc_disk_space_task.delay())


@app.task()
def get_hpc_disk_space(param):
    user = param['user']
    password = param['password']
    host = param['host']
    port = param['port']

    config_dict = app.task_config.config

    project_dir = config_dict['aix']['hpc']['disk_space']['project']['dir']
    project_program = config_dict['aix']['hpc']['disk_space']['project']['program']
    project_script = config_dict['aix']['hpc']['disk_space']['project']['script']

    host_for_connection = '{user}@{host}'.format(user=user, host=host)

    connection = Connection(host_for_connection, connect_kwargs={
        'password': password
    })

    def get_disk_space(c):
        with c.cd(project_dir):
            c.run("{program} {script}".format(
                program=project_program,
                script=project_script,
            ))

    get_disk_space(connection)