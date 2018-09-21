# coding=utf-8
import os
from pathlib import Path
import yaml
from celery.schedules import crontab


class CeleryConfig(object):
    def __init__(self, config_file_path):
        self.config_file_path = config_file_path
        with open(config_file_path, 'r') as config_file:
            config_dict = yaml.load(config_file)
            self.config = config_dict
            celery_server_config = config_dict['celery_server']
            broker_config = celery_server_config['broker']
            backend_config = celery_server_config['backend']

            if 'rabbitmq' in broker_config:
                rabbitmq_host = broker_config['rabbitmq']['host']
                rabbitmq_port = broker_config['rabbitmq']['port']
                task_scheduler_celery_broker = 'amqp://guest:guest@{host}:{port}//'.format(
                    host=rabbitmq_host, port=rabbitmq_port
                )
                self.broker_url = '{task_scheduler_celery_broker}'.format(
                    task_scheduler_celery_broker=task_scheduler_celery_broker)

            if 'mysql' in backend_config:
                mysql_host = backend_config['mysql']['host']
                mysql_port = backend_config['mysql']['port']
                mysql_user = backend_config['mysql']['user']
                mysql_password = backend_config['mysql']['password']
                task_scheduler_celery_backend = \
                    'db+mysql+mysqlconnector://{user}:{password}@{host}:{port}/celery_backend'.format(
                        user=mysql_user, password=mysql_password,
                        host=mysql_host, port=mysql_port
                    )
                self.result_backend = '{task_scheduler_celery_backend}'.format(
                    task_scheduler_celery_backend=task_scheduler_celery_backend)

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
                        print('we do not support this type: {schedule_type}'.format(schedule_type=item_schedule['type']))

            # print(beat_schedule)
            self.beat_schedule = beat_schedule

    @staticmethod
    def load_celery_config():
        if 'NWPC_MONITOR_TASK_SCHEDULER_CONFIG' not in os.environ:
            raise Exception('NWPC_MONITOR_TASK_SCHEDULER_CONFIG must be set.')

        config_file_path = os.environ['NWPC_MONITOR_TASK_SCHEDULER_CONFIG']
        print("config file path:", config_file_path)

        config = CeleryConfig(config_file_path)
        return config

    def load_task_config(self):
        config_file_dir_path = Path(self.config_file_path).parent
        task_config_file_path = Path(config_file_dir_path, self.config['celery_task']['task_config_file'])
        return TaskConfig(str(task_config_file_path))


class TaskConfig(object):
    def __init__(self, config_file_path):
        self.config_file_path = config_file_path
        with open(config_file_path, 'r') as config_file:
            config_dict = yaml.load(config_file)
            self.config = config_dict

    @staticmethod
    def get_config_file_dir():
        config_file_directory = os.path.dirname(__file__) + "/../conf"
        return config_file_directory
