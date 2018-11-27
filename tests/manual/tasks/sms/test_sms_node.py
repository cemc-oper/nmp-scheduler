# coding: utf-8
from nmp_scheduler.celery_server.task import get_sms_node_task
from nmp_scheduler.celery_server.celery import app


def test_sms_node_task():
    config_dict = app.task_config.config
    hpc_config = config_dict['sms_status_task']['hpc']

    args = {
        'owner': 'nwp_xp',
        'repo': 'aix_nwpc_pd',
        'auth': {
            'host': hpc_config['host'],
            'port': 22,
            'user': hpc_config['user'],
            'password': hpc_config['password']
        },
        'sms': {
            'sms_server': 'nwpc_pd',
            'sms_user': 'nwp_xp',
            'sms_password': '1'
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
                    'node_path': '/meso_post',
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
    result = get_sms_node_task.delay(args)
    result.get(timeout=20)
    print(result)


if __name__ == "__main__":
    test_sms_node_task()
