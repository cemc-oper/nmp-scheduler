# coding: utf-8
import click

from nmp_scheduler.celery_server.task.workload.loadleveler.jobs import get_jobs_task


@click.command()
@click.option('-u', '--user')
@click.option('-p', '--password')
def test_get_jobs_task(user, password):
    args = {
        'owner': 'nwp_xp',
        'repo': 'ibm_uranus',
        'user': user,
        'password': password,
        'host': '10.20.49.131',
        'port': 22
    }
    result = get_jobs_task.delay(args)
    result.get(timeout=20)
    print(result)


if __name__ == "__main__":
    test_get_jobs_task()
