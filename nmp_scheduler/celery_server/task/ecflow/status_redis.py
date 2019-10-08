# coding: utf-8
import json
import datetime
import gzip

from celery import group
import redis
from dateutil.parser import parse
import requests

from nmp_scheduler.celery_server.celery import app
from nwpc_workflow_model.ecflow import Bunch, NodeStatus


def get_ecflow_status_records(owner: str, repo: str):
    redis_config = app.task_config.config["ecflow"]["status_task"]["storage"]
    client = redis.Redis(host=redis_config["host"], port=redis_config["port"], db=redis_config["db"])
    key = f"{owner}/{repo}/status"
    value = client.get(key)
    if value is None:
        return None
    return json.loads(value)


def get_name_from_path(repo_name: str, node_path: str):
    if node_path == "/":
        return repo_name
    pos = node_path.rfind("/")
    return node_path[pos+1:]


def generate_bunch(repo_name: str, status_records: dict):
    bunch = Bunch()
    for status_record in status_records:
        node_path = status_record["path"]
        node_name = get_name_from_path(repo_name, node_path)
        node = {
            "path": node_path,
            "status": NodeStatus(status_record["status"]),
            "name": node_name
        }
        bunch.add_node_status(node)
    return bunch


@app.task()
def get_ecflow_status_from_redis(repo: dict):
    owner_name = repo['owner']
    repo_name = repo['repo']
    ecflow_host = repo['ecflow_host']
    ecflow_port = repo['ecflow_port']
    server_name = repo_name

    records = get_ecflow_status_records(owner_name, repo_name)
    if records is None:
        return

    collected_time = parse(records["collected_time"])
    status_records = records["status_records"]

    bunch = generate_bunch(repo_name, status_records)
    bunch_dict = bunch.to_dict()

    current_time = (datetime.datetime.utcnow() + datetime.timedelta(hours=8)).isoformat()
    data = {
        'app': 'ecflow_status_collector',
        'type': 'ecflow_status',
        'timestamp': current_time,
        'data': {
            'owner': owner_name,
            'repo': repo_name,
            'server_name': server_name,
            'ecflow_host': ecflow_host,
            'ecflow_port': ecflow_port,
            'time': collected_time.strftime("%Y-%m-%dT%H:%M:%S.%f"),
            'status': bunch_dict
        }
    }

    post_data = {
        'message': json.dumps(data)
    }

    config_dict = app.task_config.config

    post_url = config_dict['ecflow']['status_task']['collector']['post']['url']
    if 'collector' in repo:
        collector_config = repo['collector']
        if 'post' in collector_config:
            post_url = collector_config['post']['url']

    post_url = post_url.format(owner=owner_name, repo=repo_name)

    gzipped_data = gzip.compress(bytes(json.dumps(post_data), 'utf-8'))

    requests.post(post_url, data=gzipped_data, headers={
        'content-encoding': 'gzip'
    })
    return


@app.task()
def get_group_ecflow_status_from_redis():
    config_dict = app.task_config.config

    repos = config_dict['ecflow']['group_status_task']

    # celery task group
    g = group(get_ecflow_status_from_redis.s(a_repo) for a_repo in repos)
    result = g.delay()
    return


if __name__ == "__main__":
    args = {
        'owner': 'nwp_xp',
        'repo': 'nwpc_pd',
        'ecflow_host': '10.40.143.18',
        'ecflow_port': '31071'
    }
    # get_ecflow_status_from_redis(args)

    import nmp_scheduler
    result = nmp_scheduler.celery_server.task.ecflow.status_redis.get_ecflow_status_from_redis.delay(args)
    result.get(timeout=20)
    print(result)
