# coding=utf-8
from fabric.api import run, cd, execute, env
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

    env_hosts = ['{user}@{host}'.format(user=user, host=host)]
    env_password = '{password}'.format(password=password)

    env.hosts = env_hosts
    env.password = env_password

    def get_disk_usage():
        with cd(project_dir):
            run("{program} {script}".format(
                program=project_program,
                script=project_script,
            ))

    execute(get_disk_usage)


@app.task()
def get_group_hpc_disk_usage_task():
    config_dict = app.task_config.config

    group_tasks = config_dict['aix']['hpc']['disk_usage']['task_group']

    # celery task group
    g = group(get_hpc_disk_usage.s(param) for param in group_tasks)
    result = g.delay()
    return


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

    env_hosts = ['{user}@{host}'.format(user=user, host=host)]
    env_password = '{password}'.format(password=password)

    env.hosts = env_hosts
    env.password = env_password

    def get_disk_space():
        with cd(project_dir):
            run("{program} {script}".format(
                program=project_program,
                script=project_script,
            ))

    execute(get_disk_space)


@app.task()
def get_group_hpc_disk_space_task():
    config_dict = app.task_config.config

    group_tasks = config_dict['aix']['hpc']['disk_space']['task_group']

    # celery task group
    g = group(get_hpc_disk_space.s(param) for param in group_tasks)
    result = g.delay()
    return


@app.task()
def get_hpc_loadleveler_usage(param):
    user = param['user']
    password = param['password']
    host = param['host']
    port = param['port']

    config_dict = app.task_config.config

    project_dir = config_dict['aix']['hpc']['loadleveler_status']['project']['dir']
    project_program = config_dict['aix']['hpc']['loadleveler_status']['project']['program']
    project_script = config_dict['aix']['hpc']['loadleveler_status']['project']['script']

    env_hosts = ['{user}@{host}'.format(user=user, host=host)]
    env_password = '{password}'.format(password=password)

    env.hosts = env_hosts
    env.password = env_password

    def get_disk_usage():
        with cd(project_dir):
            run("{program} {script}".format(
                program=project_program,
                script=project_script,
            ))

    execute(get_disk_usage)


@app.task()
def get_group_hpc_loadleveler_status_task():
    config_dict = app.task_config.config

    group_tasks = config_dict['aix']['hpc']['loadleveler_status']['task_group']

    # celery task group
    g = group(get_hpc_loadleveler_usage.s(param) for param in group_tasks)
    result = g.delay()
    return


if __name__ == "__main__":
    import os
    os.environ['MODE'] = 'develop'
    # print(task.hpc.get_group_hpc_loadleveler_status_task())
    # print(task.hpc.get_group_hpc_disk_usage_task.delay())
    print(nwpc_monitor_task_scheduler.celery_server.task.aix.hpc.get_group_hpc_disk_space_task.delay())
