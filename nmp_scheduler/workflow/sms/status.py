import grpc

from nmp_scheduler.workflow.sms.proto import (
    sms_collector_pb2, sms_collector_pb2_grpc)


def get_and_post_sms_status(
        owner_name: str,
        repo_name: str,
        sms_name: str,
        sms_host: str,
        sms_user: str,
        sms_prog: str,
        rpc_target: str,
        post_url: str,
) -> sms_collector_pb2.Response:
    status_request = sms_collector_pb2.StatusRequest(
        owner=owner_name,
        repo=repo_name,
        sms_host=sms_host,
        sms_prog=str(sms_prog),
        sms_name=sms_name,
        sms_user=sms_user,
        sms_password='1',
        disable_post=False,
        post_url=post_url,
        content_encoding='gzip',
        verbose=True
    )

    with grpc.insecure_channel(rpc_target) as channel:
        stub = sms_collector_pb2_grpc.SmsCollectorStub(channel)
        response = stub.CollectStatus(status_request)
        return response
