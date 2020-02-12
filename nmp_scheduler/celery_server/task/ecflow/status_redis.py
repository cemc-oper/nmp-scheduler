# coding: utf-8
import json
import datetime
import gzip

from celery import group
from celery.utils.log import get_task_logger
from dateutil.parser import parse
import requests

from nmp_scheduler.celery_server.celery import app
from nmp_scheduler.workflow.ecflow.status import get_ecflow_status_records
from nmp_scheduler.workflow.ecflow.bunch import generate_bunch

logger = get_task_logger(__name__)


@app.task()
def get_ecflow_status_from_redis(repo: dict):
    """

    :param repo:
        {
            "owner": "owner name",
            "repo": "repo name:,
            "ecflow_host": "ecflow host",
            "ecflow_port": "ecflow port",
        }
    :return:

    ecflow status task config:
        ```yaml
        status_task:
          collector:
            post:
              url: "broker url for ecflow status"
          storage:
            type: redis
            host: "redis host"
            port: redis port
            db: redis db
        ```
    """
    logger.info(f"begin to run task: {repo}")
    owner_name = repo['owner']
    repo_name = repo['repo']
    ecflow_host = repo['ecflow_host']
    ecflow_port = repo['ecflow_port']
    server_name = repo_name

    ecflow_status_task_config = app.task_config.config["ecflow"]["status_task"]

    redis_config = ecflow_status_task_config["storage"]

    records = get_ecflow_status_records(redis_config, owner_name, repo_name)
    if records is None:
        logger.warning(f"[{owner_name}/{repo_name}] ecflow records is None")
        return

    collected_time = parse(records["collected_time"]) + datetime.timedelta(hours=8)
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

    post_url = ecflow_status_task_config['collector']['post']['url']
    if 'collector' in repo:
        collector_config = repo['collector']
        if 'post' in collector_config:
            post_url = collector_config['post']['url']

    post_url = post_url.format(owner=owner_name, repo=repo_name)
    logger.info(f"[{owner_name}/{repo_name}] post url: {post_url}")

    gzipped_data = gzip.compress(bytes(json.dumps(post_data), 'utf-8'))

    response = requests.post(post_url, data=gzipped_data, headers={
        'content-encoding': 'gzip'
    })
    logger.info(f"[{owner_name}/{repo_name}] posted to web: {response}")

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
