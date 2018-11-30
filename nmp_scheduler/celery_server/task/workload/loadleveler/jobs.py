# coding: utf-8
from celery import group
import grpc

from nmp_scheduler.celery_server.celery import app


@app.task()
def get_jobs_task(param):
    owner = param['owner']
    repo = param['repo']
    user = param['user']
    password = param['password']
    host = param['host']
    port = param['port']

    config_dict = app.task_config.config
    rpc_target = config_dict['workload']['loadleveler']['collector']['server']['rpc_target']
    post_url = config_dict['workload']['loadleveler']['jobs']['post']['url']

    post_url = post_url.format(owner=owner, repo=repo)

    from nmp_scheduler.celery_server.task.workload.loadleveler.proto import (
        loadleveler_collector_pb2, loadleveler_collector_pb2_grpc
    )

    jobs_request = loadleveler_collector_pb2.JobsRequest(
        owner=owner,
        repo=repo,
        host=host,
        port=port,
        user=user,
        password=password,
        output_style='post',
        post_url=post_url,
        content_encoding='gzip',
        verbose=True
    )

    app.log.get_default_logger().info('getting loadleveler jobs for {owner}/{repo}...'.format(
        owner=owner, repo=repo
    ))
    with grpc.insecure_channel(rpc_target) as channel:
        stub = loadleveler_collector_pb2_grpc.LoadLevelerCollectorStub(channel)
        response = stub.CollectJobs(jobs_request)
        app.log.get_default_logger().info('getting loadleveler jobs for {owner}/{repo}...done: {response}'.format(
            owner=owner, repo=repo, response=response.status
        ))


@app.task()
def get_group_jobs_task():
    config_dict = app.task_config.config

    group_tasks = config_dict['workload']['loadleveler']['jobs']['task_group']

    # celery task group
    g = group(get_jobs_task.s(param) for param in group_tasks)
    result = g.delay()
    return
