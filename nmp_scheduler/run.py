# coding=utf-8
import os
import sys
import platform
import pathlib
import click


def set_config_file_path_env(config_file):
    config_file_path = pathlib.Path(config_file)
    if not config_file_path.is_absolute():
        config_file_path = pathlib.Path(pathlib.Path.cwd(), config_file_path)
    os.environ['NWPC_MONITOR_TASK_SCHEDULER_CONFIG'] = str(config_file_path)


@click.group()
@click.option('--config-file', help="config file path")
def cli(config_file):
    if config_file:
        set_config_file_path_env(config_file)


@cli.command()
@click.option('--name', help="worker's name", required=True)
@click.option('--queues', help="worker's queues, split by ',', default is None")
def worker(queues, name):
    from nmp_scheduler.celery_server.celery import app
    # print(sys.argv)
    if queues:
        queue_list = queues.split(',')
        app.select_queues(queue_list)
    app.Worker(
        hostname='{name}@{host}'.format(name=name, host=platform.node()),
        loglevel='INFO'
    ).start()


@cli.command()
def beat():
    from nmp_scheduler.celery_server.celery import app, celery_config
    app.Beat(
        schedule=celery_config.config['celery_beat']['schedule'],
        max_interval=5,
        loglevel='DEBUG'
    ).run()


if __name__ == "__main__":
    cli()
