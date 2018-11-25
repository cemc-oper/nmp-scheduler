# coding: utf-8
from nmp_scheduler.celery_server.task.ecflow import get_ecflow_status_task


def test_ecflow_status():
    args = {
        'owner': 'nwp_xp',
        'repo': 'pi_nwpc_pd_bk',
        'ecflow_host': '10.40.143.18',
        'ecflow_port': '31071'
    }
    # print(json.dumps(get_ecflow_status_task(args), indent=2))
    result = get_ecflow_status_task.delay(args)
    result.get(timeout=20)
    print(result)


if __name__ == "__main__":
    test_ecflow_status()
