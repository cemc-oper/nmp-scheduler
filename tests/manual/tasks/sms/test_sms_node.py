# coding: utf-8
from nmp_scheduler.celery_server.task.sms.node import check_sms_node_task


def test_sms_node_task():
    args = {
        'owner': 'nwp_xp',
        'repo': 'aix_nwpc_pd',
        'sms': {
            'sms_host': '10.20.49.131',
            'sms_prog': '310071',
            'sms_name': 'nwpc_pd',
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
                                'fields': '20181128'
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
    result = check_sms_node_task.delay(args)
    result.get(timeout=20)
    print(result)


if __name__ == "__main__":
    test_sms_node_task()
