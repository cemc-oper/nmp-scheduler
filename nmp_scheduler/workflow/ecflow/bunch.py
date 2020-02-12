from nwpc_workflow_model.ecflow import Bunch
from nwpc_workflow_model.node_status import NodeStatus


def generate_bunch(repo_name: str, status_records: dict) -> Bunch:
    bunch = Bunch()
    for status_record in status_records:
        node_path = status_record["path"]
        node_name = _get_name_from_path(repo_name, node_path)
        node = {
            "path": node_path,
            "status": NodeStatus(status_record["status"]),
            "name": node_name
        }
        bunch.add_node_status(node)
    return bunch


def _get_name_from_path(repo_name: str, node_path: str) -> str:
    if node_path == "/":
        return repo_name
    pos = node_path.rfind("/")
    return node_path[pos+1:]
