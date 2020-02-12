# coding: utf-8
from celery import group

from nmp_scheduler.celery_server.celery import app
from nmp_scheduler.workflow.sms.status import get_and_post_sms_status


@app.task()
def get_sms_status_task(repo: dict):
    owner_name = repo['owner']
    repo_name = repo['repo']
    sms_host = repo['sms_host']
    sms_prog = repo['sms_prog']
    sms_user = repo['sms_user']
    sms_name = repo['sms_name']

    config_dict = app.task_config.config
    rpc_target = config_dict['sms']['status_task']['collector']['server']['rpc_target']
    post_url = config_dict['sms']['status_task']['collector']['post']['url']

    post_url = post_url.format(owner=owner_name, repo=repo_name)

    app.log.get_default_logger().info('getting sms status for {owner}/{repo}...'.format(
        owner=owner_name, repo=repo_name
    ))
    response = get_and_post_sms_status(
        owner_name,
        repo_name,
        sms_name,
        sms_host,
        sms_user,
        sms_prog,
        rpc_target,
        post_url,
    )
    app.log.get_default_logger().info(
        'getting sms status for {owner}/{repo}...done: {response}'.format(
            owner=owner_name, repo=repo_name, response=response.status
        ))


@app.task()
def get_group_sms_status_task():
    config_dict = app.task_config.config

    repos = config_dict['sms']['status_task']['group_status_task']

    # celery task group
    g = group(get_sms_status_task.s(a_repo) for a_repo in repos)
    result = g.delay()
    return
