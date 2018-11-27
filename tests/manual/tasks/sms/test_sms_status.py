# coding: utf-8
from nmp_scheduler.celery_server.task.sms.status import get_sms_status_task


def test_sms_status_task():
    args = {
        'owner': 'nwp_xp',
        'repo': 'nwpc_op',
        'sms_host': '10.20.49.131',
        'sms_prog': '310066',
        'sms_user': 'nwp',
        'sms_name': 'nwpc_op',
    }
    # print(json.dumps(get_ecflow_status_task(args), indent=2))
    result = get_sms_status_task.delay(args)
    result.get(timeout=10)
    print(result)


if __name__ == "__main__":
    test_sms_status_task()
