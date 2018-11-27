# coding: utf-8
from celery import group
import grpc

from nmp_scheduler.celery_server.celery import app


@app.task()
def get_sms_status_task(repo):

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

    from nmp_scheduler.celery_server.task.sms.proto import sms_collector_pb2_grpc, sms_collector_pb2
    status_request = sms_collector_pb2.StatusRequest(
        owner=owner_name,
        repo=repo_name,
        sms_host=sms_host,
        sms_prog=str(sms_prog),
        sms_name=sms_name,
        sms_user=sms_user,
        sms_password='1',
        disable_post=False,
        post_url=post_url,
        content_encoding='gzip',
        verbose=True
    )

    app.log.get_default_logger().info('getting sms status for {owner}/{repo}...'.format(
        owner=owner_name, repo=repo_name
    ))
    with grpc.insecure_channel(rpc_target) as channel:
        stub = sms_collector_pb2_grpc.SmsCollectorStub(channel)
        response = stub.CollectStatus(status_request)
        app.log.get_default_logger().info(
            'getting sms status for {owner}/{repo}...done: {response}'.format(
                owner=owner_name, repo=repo_name, response=response.status
            ))


@app.task()
def get_group_sms_status_task():
    config_dict = app.task_config.config

    repos = config_dict['sms']['group_status_task']

    # celery task group
    g = group(get_sms_status_task.s(a_repo) for a_repo in repos)
    result = g.delay()
    return
