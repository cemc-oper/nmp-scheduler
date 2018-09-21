import datetime
import os
import pathlib
import yaml
from celery.schedules import crontab
from nmp_scheduler.celery_server.celery import app

from nmp_scheduler.celery_server.task.sms import get_sms_node_task


@app.on_after_finalize.connect
def setup_sms_node_periodic_task(sender, **kwargs):
    task_config = app.task_config.config
    if 'sms_node_task' not in task_config:
        print("there is no sms node tasks.")
        return

    print("setup sms node periodic tasks")
    task_config_dir = str(pathlib.Path(app.task_config.config_file_path).parent)
    repo_config_dir = app.task_config.config['sms_node_task']['repo_config_dir']
    repo_config_dir = str(pathlib.Path(task_config_dir, repo_config_dir))

    for root, dirs, files in os.walk(repo_config_dir):
        print(files)
        for file_path in files:
            print(file_path)
            if not file_path.endswith('yaml'):
                continue
            config_file_path = file_path
            print(os.path.join(repo_config_dir,config_file_path))
            with open(os.path.join(repo_config_dir,config_file_path), 'r') as config_file:
                config = yaml.load(config_file)

            if config is None:
                print("Error in loading config")
                return

            task_list = config['task_list']
            for a_task in task_list:
                task_triggers = a_task['trigger']
                task_args = {
                    'owner': config['owner'],
                    'repo': config['repo'],
                    'auth': config['auth'],
                    'sms': config['sms'],
                    'task': a_task
                }
                for a_trigger in task_triggers:
                    trigger_type = a_trigger['type']
                    if trigger_type == 'time':
                        trigger_time = datetime.datetime.strptime(a_trigger['time'], "%H:%M:%S")
                        crontab_param_dict = {
                            'minute': trigger_time.minute,
                            'hour': trigger_time.hour
                        }
                        # crontab_param_dict = {
                        #     'minute': datetime.datetime.utcnow().minute + 1,
                        #     'hour': datetime.datetime.utcnow().hour
                        # }
                        print('add periodic_task', a_task['name'], crontab_param_dict)
                        sender.add_periodic_task(
                            crontab(**crontab_param_dict),
                            get_sms_node_task.s(task_args)
                        )
                    else:
                        print("trigger type is not supported:", trigger_type)

