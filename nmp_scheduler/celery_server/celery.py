# coding=utf-8
from celery import Celery

from nmp_scheduler.celery_server.config import CeleryConfig, TaskConfig

celery_config = CeleryConfig.load_celery_config()

app = Celery(
    celery_config.config['celery_server']['name'],
    loglevel="INFO"
)

app.config_from_object(celery_config)

app.celery_config = celery_config
app.task_config = app.celery_config.load_task_config()

# from nmp_scheduler.celery_server import task


if __name__ == '__main__':
    app.start()
