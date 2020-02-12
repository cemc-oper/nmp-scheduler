import json

import redis


def get_ecflow_status_records(redis_config: dict, owner: str, repo: str) -> dict or None:
    client = redis.Redis(
        host=redis_config["host"],
        port=redis_config["port"],
        db=redis_config["db"]
    )
    key = f"{owner}/{repo}/status"
    value = client.get(key)
    if value is None:
        return None
    return json.loads(value)
