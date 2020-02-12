# coding=utf-8
import os
from pathlib import Path

import yaml
from celery.schedules import crontab


SCHEDULER_CONFIG_ENV_VAR = "NWPC_MONITOR_TASK_SCHEDULER_CONFIG"


class CeleryConfig(object):
    def __init__(self, config_file_path: str or Path):
        self.config_file_path = config_file_path
        with open(config_file_path, "r") as config_file:
            config_dict = yaml.safe_load(config_file)
            self.config = config_dict
            celery_server_config = config_dict['celery_server']
            broker_config = celery_server_config['broker']
            backend_config = celery_server_config['backend']

            if "rabbitmq" in broker_config:
                rabbitmq_host = broker_config['rabbitmq']['host']
                rabbitmq_port = broker_config['rabbitmq']['port']
                task_scheduler_celery_broker = f"pyamqp://guest:guest@{rabbitmq_host}:{rabbitmq_port}//"
                self.broker_url = f"{task_scheduler_celery_broker}"

            if "mysql" in backend_config:
                mysql_host = backend_config['mysql']['host']
                mysql_port = backend_config['mysql']['port']
                mysql_user = backend_config['mysql']['user']
                mysql_password = backend_config['mysql']['password']
                task_scheduler_celery_backend = (
                    f"db+mysql+mysqlconnector:"
                    f"//{mysql_user}:{mysql_password}@{mysql_host}:{mysql_port}"
                    f"/celery_backend"
                )
                self.result_backend = f"{task_scheduler_celery_backend}"

            if "redis" in backend_config:
                redis_host = backend_config['redis']['host']
                redis_port = backend_config['redis']['port']
                task_scheduler_celery_backend = f"redis://@{redis_host}:{redis_port}/0"
                self.result_backend = f"{task_scheduler_celery_backend}"

            self.include = [
                'nmp_scheduler.celery_server.task'
            ]

            # celery beat
            celery_beat_config = config_dict['celery_beat']
            beat_config = celery_beat_config['beat_schedule']
            beat_schedule = {}
            if beat_config is not None:
                for a_beat_item in beat_config:
                    item_schedule = a_beat_item['schedule']
                    if item_schedule['type'] == 'crontab':
                        schedule_param = item_schedule['param']
                        crontab_param_dict = {}
                        for a_param in schedule_param:
                            crontab_param_dict[a_param] = schedule_param[a_param]
                        beat_schedule[a_beat_item['name']] = {
                            'task': a_beat_item['task'],
                            'schedule': crontab(**crontab_param_dict),
                            'args': ()
                        }
                    else:
                        print(f"we do not support this type: {item_schedule['type']}")

            # print(beat_schedule)
            self.beat_schedule = beat_schedule

            task_routes = list()
            for a_route in celery_server_config['task_routes']:
                task_routes.append((a_route['pattern'], {'queue': a_route['queue']}))
            self.task_routes = (task_routes,)

    @staticmethod
    def load_celery_config():
        if SCHEDULER_CONFIG_ENV_VAR not in os.environ:
            raise Exception(f'{SCHEDULER_CONFIG_ENV_VAR} must be set.')

        config_file_path = os.environ[SCHEDULER_CONFIG_ENV_VAR]
        print(f"config file path: {config_file_path}")

        config = CeleryConfig(config_file_path)
        return config

    def load_task_config(self):
        config_file_dir_path = Path(self.config_file_path).parent
        task_config_file_path = Path(config_file_dir_path, self.config['celery_task']['task_config_file'])
        return TaskConfig(str(task_config_file_path))


class TaskConfig(object):
    def __init__(self, config_file_path: str or Path):
        self.config_file_path = config_file_path
        with open(config_file_path, 'r') as config_file:
            config_dict = yaml.safe_load(config_file)
            self.config = config_dict

    @classmethod
    def get_config_file_dir(cls):
        config_file_directory = os.path.dirname(__file__) + "/../conf"
        return config_file_directory
