import datetime
import gzip
import json

import requests

from nmp_scheduler.celery_server.celery import app
from nmp_scheduler.workflow.sms.node import check_sms_node


@app.task()
def check_sms_node_task(args: dict):
    """

    :param args:
    {
        'owner': 'owner',
        'repo': 'repo',
        'sms': {
            'sms_host': sms host,
            'sms_prog': sms RPC prog,
            'sms_name': sms server name,
            'sms_user': sms user,
            'sms_password': sms password
        },
        'task': {
            'name': 'grapes_meso_post',
            'type': 'sms-node',
            'trigger': [
                {
                    'type': 'time',
                    'time': '11:35:00'
                }
            ],
            "nodes": [
                {
                    'node_path': '/grapes_meso_post',
                    'check_list': [
                        {
                            'type': 'variable',
                            'name': 'SMSDATE',
                            'value': {
                                'type': 'date',
                                'operator': 'equal',
                                'fields': 'current'
                            }
                        },
                        {
                            'type': 'status',
                            'value': {
                                'operator': 'in',
                                'fields': [
                                    "submitted",
                                    "active",
                                    "complete"
                                ]
                            }
                        }
                    ]
                }
            ]
        }
    }

    :return:
    {
        'app': 'nmp_scheduler',
        'type': 'sms_node_task',
        'timestamp': iso format,
        'data': {
            'owner': owner,
            'repo': repo,
            'request': {
                'task': task object,
            },
            'response': {
                'nodes':[
                    {
                        'node_path': node_path,
                        'check_list_result': array, see check_sms_node
                    },
                    ...
                ]
            }
        }
    }

    nodes: an array of node result

    """
    config_dict = app.task_config.config

    owner = args['owner']
    repo = args['repo']

    current_task = args['task']
    nodes = current_task['nodes']

    collector_config = config_dict['sms']['node_task']['collector']

    node_result = []
    for a_node in nodes:
        result = check_sms_node(
            collector_config,
            owner=owner,
            repo=repo,
            sms_info=args['sms'],
            sms_node=a_node)
        node_result.append(result)

    result = {
        'app': 'nmp_scheduler',
        'type': 'sms_node_task',
        'timestamp': datetime.datetime.utcnow().isoformat(),
        'data': {
            'owner': args['owner'],
            'repo': args['repo'],
            'time': datetime.datetime.utcnow().isoformat(),
            'request': {
                'task': args['task'],
            },
            'response': {
                'nodes': node_result
            }
        }
    }
    post_data = {
        'message': json.dumps(result)
    }

    gzipped_data = gzip.compress(bytes(json.dumps(post_data), 'utf-8'))
    url = collector_config['post']['url'].format(
        owner=args['owner'],
        repo=args['repo']
    )

    requests.post(url, data=gzipped_data, headers={
        'content-encoding': 'gzip'
    })

    return result
