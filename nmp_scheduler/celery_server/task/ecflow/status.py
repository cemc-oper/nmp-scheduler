# coding: utf-8
from celery import group
import grpc

from nmp_scheduler.celery_server.celery import app


@app.task()
def get_ecflow_status_task(repo):

    owner_name = repo['owner']
    repo_name = repo['repo']
    ecflow_host = repo['ecflow_host']
    ecflow_port = repo['ecflow_port']

    config_dict = app.task_config.config

    rpc_target = config_dict['ecflow']['status_task']['collector']['server']['rpc_target']
    post_url = config_dict['ecflow']['status_task']['collector']['post']['url']
    if 'collector' in repo:
        collector_config = repo['collector']
        if 'server' in collector_config:
            rpc_target = collector_config['server']['rpc_target']
        if 'post' in collector_config:
            post_url = collector_config['post']['url']

    post_url = post_url.format(owner=owner_name, repo=repo_name)

    from nmp_scheduler.workflow.ecflow.proto import ecflow_client_pb2_grpc
    from nmp_scheduler.workflow.ecflow.proto import ecflow_client_pb2
    status_request = ecflow_client_pb2.StatusRequest(
        owner=owner_name,
        repo=repo_name,
        host=ecflow_host,
        port=str(ecflow_port),
        post_url=post_url,
        content_encoding='gzip',
        disable_post=False,
        verbose=True
    )

    app.log.get_default_logger().info('getting ecflow status for {owner}/{repo}...on {rpc_target}'.format(
        owner=owner_name, repo=repo_name, rpc_target=rpc_target
    ))
    with grpc.insecure_channel(rpc_target) as channel:
        stub = ecflow_client_pb2_grpc.EcflowClientServiceStub(channel)
        response = stub.CollectStatus(status_request)
        app.log.get_default_logger().info(
            'getting ecflow status for {owner}/{repo}...done: {response}'.format(
                owner=owner_name, repo=repo_name, response=response.status
            ))


@app.task()
def get_group_ecflow_status_task():
    config_dict = app.task_config.config

    repos = config_dict['ecflow']['group_status_task']

    # celery task group
    g = group(get_ecflow_status_task.s(a_repo) for a_repo in repos)
    result = g.delay()
    return


if __name__ == "__main__":
    args = {
        'owner': 'nwp_xp',
        'repo': 'pi_nwpc_pd_bk',
        'ecflow_host': '10.40.143.18',
        'ecflow_port': '31071'
    }
    # print(json.dumps(get_ecflow_status_task(args), indent=2))
    import nmp_scheduler
    result = nmp_scheduler.celery_server.task.ecflow.status.get_ecflow_status_task.delay(args)
    result.get(timeout=20)
    print(result)
