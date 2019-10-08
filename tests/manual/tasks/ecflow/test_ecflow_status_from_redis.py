# coding: utf-8


def test_ecflow_status():
    from nmp_scheduler.celery_server.celery import app
    from nmp_scheduler.celery_server.task.ecflow.status_redis import get_ecflow_status_from_redis

    args = {
        'owner': 'nwp_xp',
        'repo': 'nwpc_pd',
        'ecflow_host': '10.40.143.18',
        'ecflow_port': '31071',
    }

    # print(json.dumps(get_ecflow_status_task(args), indent=2))
    result = get_ecflow_status_from_redis.delay(args)
    result.get(timeout=20)
    print(result)


if __name__ == "__main__":
    test_ecflow_status()
