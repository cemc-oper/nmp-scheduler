#!/usr/bin/env python3
# coding=utf-8
import datetime
import json
import gzip
import requests
from fabric.api import run, cd, execute, env
from celery import group

from nmp_scheduler.celery_server.celery import app
from nwpc_workflow_model.sms.sms_node import SmsNode


"""
SMS Status
"""


@app.task()
def get_sms_status_task(repo):

    owner_name = repo['owner']
    repo_name = repo['repo']
    sms_user = repo['sms_user']
    sms_name = repo['sms_name']

    config_dict = app.task_config.config
    hpc_user = config_dict['sms_status_task']['hpc']['user']
    hpc_password = config_dict['sms_status_task']['hpc']['password']
    hpc_host = config_dict['sms_status_task']['hpc']['host']
    project_dir = config_dict['sms_status_task']['project']['dir']
    project_program = config_dict['sms_status_task']['project']['program']
    project_script = config_dict['sms_status_task']['project']['script']

    user = repo.get('user', hpc_user)
    password = repo.get('password', hpc_password)
    host = repo.get('host', hpc_host)

    env_hosts = ['{user}@{host}'.format(user=user, host=host)]
    env_password = '{password}'.format(password=password)

    env.hosts = env_hosts
    env.password = env_password

    def get_sms_status(sms_user, sms_name, user):
        with cd(project_dir):
            run("{program} {script} -o {owner} -r {repo} -u {user} -n {sms_name}".format(
                program=project_program,
                script=project_script,
                owner=owner_name,
                repo=repo_name,
                sms_user=sms_user,
                sms_name=sms_name,
                user=user
            ))

    execute(get_sms_status, sms_user=sms_user, sms_name=sms_name, user=user)


@app.task()
def get_group_sms_status_task():
    config_dict = app.task_config.config

    repos = config_dict['group_sms_status_task']

    # celery task group
    g = group(get_sms_status_task.s(a_repo) for a_repo in repos)
    result = g.delay()
    return


def check_sms_node(project_conf, sms_info, sms_node):
    """
    check a sms node. return check result.
    
    :param project_conf: config dict for program
        {
            'dir': program work dir,
            'program': python executable,
            'script': python script
        }
    :param sms_info: sms server information
        {
            'sms_server': sms server,
            'sms_user': sms user,
            'sms_password': sms password
        }
    :param sms_node: sms node task for one node path
        {
            'node path': node path,
            'check_list': check list, 
        }
        
        an item in check list: 
        common fields:
        {
           'type': check type, [ variable, status ] 
        }
        check type related fields:
        * variable, check variable value, there may be more than one variable for a single node path.
            {
                'name': variable name,
                'value': {
                    'type': variable type,
                    'operator': 'equal',
                    'fields': fields of operators, array or an object
                }
            }
            
        * status, check sms node status
            {
                'value': {
                    'operator': operator, such as in
                    'fields': array, []
                }
            }
    :return: check result
        {
            
        }
    """

    project_dir = project_conf['dir']
    project_program = project_conf['program']
    project_script = project_conf['script']

    sms_server = sms_info['sms_server']
    sms_user = sms_info['sms_user']
    sms_password = sms_info['sms_password']

    node_path = sms_node['node_path']

    check_list_result = []

    with cd(project_dir):
        run_result = run(
            "{program} {script} --sms-server={sms_server} "
            "--sms-user={sms_user} --sms-password {sms_password} --node-path={node_path}"
                .format(
                program=project_program,
                script=project_script,
                sms_server=sms_server,
                sms_user=sms_user,
                sms_password=sms_password,
                node_path=node_path
            )).splitlines()
        cur_line_no = 0
        result_length = len(run_result)
        while cur_line_no < result_length and (not run_result[cur_line_no].startswith("{")):
            cur_line_no += 1

        response_json_string = '\n'.join(run_result[cur_line_no:])

        sms_node_dict = json.loads(response_json_string)['data']['response']['node']
        node_object = SmsNode.create_from_dict(sms_node_dict)

        for a_check_item in sms_node['check_list']:
            check_type = a_check_item['type']
            is_condition_fit = None
            check_result = {
                'type': check_type,
                'is_condition_fit': is_condition_fit
            }

            if check_type == 'variable':
                var_name = a_check_item['name']
                check_result['name'] = var_name

                value_type = a_check_item['value']['type']
                value_operator = a_check_item['value']['operator']

                if value_type == "date":
                    if value_operator == 'equal':
                        expected_var_value = a_check_item['value']['fields']
                        if expected_var_value == 'current' or expected_var_value == 'today':
                            expected_var_value = datetime.datetime.now(tz=datetime.timezone.utc).strftime("%Y%m%d")
                        elif expected_var_value == 'yesterday':
                            expected_var_value = (
                                datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=1)
                            ).strftime("%Y%m%d")

                        var = node_object.get_variable(var_name)
                        if var.value == expected_var_value:
                            is_condition_fit = True
                        else:
                            is_condition_fit = False

                        check_result['value'] = {
                                'expected_value': expected_var_value,
                                'value': var.value,
                        }
                        check_result['is_condition_fit'] = is_condition_fit

            elif check_type == 'status':
                status = node_object.status
                if a_check_item['value']['operator'] == 'in':
                    fields = a_check_item['value']['fields']
                    if status in fields:
                        is_condition_fit = True
                    else:
                        is_condition_fit = False
                check_result['value'] = {
                    'expected_value': a_check_item['value'],
                    'value': status
                }
                check_result['is_condition_fit'] = is_condition_fit

            if is_condition_fit is None:
                print("current var check is not supported", a_check_item)

            check_list_result.append(check_result)

    return {
        'node_path': node_path,
        'check_list_result': check_list_result
    }


@app.task()
def get_sms_node_task(args):
    """
    
    :param args: 
    {
        'owner': 'owner',
        'repo': 'repo',
        'auth': {
            'host': 'host',
            'port': 'port',
            'user': 'user',
            'password': 'password'
        },
        'sms': {
            'sms_server': 'sms_server',
            'sms_user': 'sms_user',
            'sms_password': 'sms_password'
        },
        'task': {
            'name': 'grapes_meso_post',
            'type': 'sms-node',
            'trigger': [
                {
                    'type': 'time',
                    'time': '11:35:00'
                }
            ],
            "nodes": [
                {
                    'node_path': '/grapes_meso_post',
                    'check_list': [
                        {
                            'type': 'variable',
                            'name': 'SMSDATE',
                            'value': {
                                'type': 'date',
                                'operator': 'equal',
                                'fields': 'current'
                            }
                        },
                        {
                            'type': 'status',
                            'value': {
                                'operator': 'in',
                                'fields': [
                                    "submitted",
                                    "active",
                                    "complete"
                                ]
                            }
                        }
                    ]
                }
            ]
        }
    }
    :return: 
    {
        'app': 'nmp_scheduler',
        'type': 'sms_node_task',
        'timestamp': iso format,
        'data': {
            'owner': owner,
            'repo': repo,
            'request': {
                'task': task object,
            },
            'response': {
                'nodes':[
                    {
                        'node_path': node_path,
                        'check_list_result': array, see check_sms_node
                    },
                    ...
                ]
            }
        }
    }
    
    nodes: an array of node result
        
    """
    config_dict = app.task_config.config

    host = args['auth']['host']
    port = args['auth']['port']
    user = args['auth']['user']
    password = args['auth']['password']

    env_hosts = ['{user}@{host}'.format(user=user, host=host)]
    env_password = '{password}'.format(password=password)

    env.hosts = env_hosts
    env.password = env_password

    current_task = args['task']
    nodes = current_task['nodes']

    node_result = []
    for a_node in nodes:
        result = execute(
            check_sms_node,
            project_conf=config_dict['sms_node_task']['project'],
            sms_info=args['sms'],
            sms_node=a_node)
        node_result.extend(result.values())

    result = {
        'app': 'nmp_scheduler',
        'type': 'sms_node_task',
        'timestamp': datetime.datetime.utcnow().isoformat(),
        'data': {
            'owner': args['owner'],
            'repo': args['repo'],
            'request': {
                'task': args['task'],
            },
            'response': {
                'nodes': node_result
            }
        }
    }
    post_data = {
        'message': json.dumps(result)
    }

    gzipped_data = gzip.compress(bytes(json.dumps(post_data), 'utf-8'))
    url = config_dict['sms_node_task']['post']['url'].format(
        owner=args['owner'],
        repo=args['repo']
    )
    requests.post(url, data=gzipped_data, headers={
        'content-encoding': 'gzip'
    })

    return result


if __name__ == "__main__":
    args = {
        'owner': 'owner',
        'repo': 'repo',
        'auth': {
            'host': 'host',
            'port': 'port',
            'user': 'user',
            'password': 'password'
        },
        'sms': {
            'sms_server': 'sms_server',
            'sms_user': 'sms_user',
            'sms_password': 'sms_password'
        },
        'task': {
            'name': 'grapes_meso_post',
            'type': 'sms-node',
            'trigger': [
                {
                    'type': 'time',
                    'time': '11:35:00'
                }
            ],
            "nodes": [
                {
                    'node_path': '/grapes_meso_post',
                    'check_list': [
                        {
                            'type': 'variable',
                            'name': 'SMSDATE',
                            'value': {
                                'type': 'date',
                                'operator': 'equal',
                                'fields': 'current'
                            }
                        },
                        {
                            'type': 'status',
                            'value': {
                                'operator': 'in',
                                'fields': [
                                    "submitted",
                                    "active",
                                    "complete"
                                ]
                            }
                        }
                    ]
                }
            ]
        }
    }
    print(json.dumps(get_sms_node_task(args), indent=2))
