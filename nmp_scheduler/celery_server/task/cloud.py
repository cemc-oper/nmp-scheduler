# coding=utf-8
from fabric.api import run, cd, execute, env
from celery import group
import requests

from nmp_scheduler.celery_server.celery import app

"""
DingTalk
"""


@app.task()
def update_dingtalk_token_task():
    config_dict = app.task_config.config
    url = config_dict['update_dingtalk_token_task']['url']
    requests.get(url)
    return


@app.task()
def update_weixin_token_task():
    config_dict = app.task_config.config
    url = config_dict['update_weixin_token_task']['url']
    requests.get(url)
    return


if __name__ == "__main__":
    r = update_dingtalk_token_task.delay()
