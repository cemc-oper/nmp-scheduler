# coding=utf-8
import os
import sys
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
def worker():
    from nmp_scheduler.celery_server.celery import app
    print(sys.argv)
    app.Worker().start()


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
