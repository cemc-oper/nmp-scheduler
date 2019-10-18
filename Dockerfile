FROM python:3.6

LABEL maintainer="perillaroc@gmail.com"

RUN pip3 install git+https://github.com/nwpc-oper/nwpc-workflow-model

COPY nmp_scheduler/ /srv/nmp_scheduler/
COPY setup.py /srv/setup.py

RUN cd /srv/ \
    && pip install .

WORKDIR /srv

ENV NWPC_MONITOR_TASK_SCHEDULER_CONFIG /etc/nmp-scheduler/celery.config.yaml

ENTRYPOINT ["python3", "/srv/nmp_scheduler/run.py"]

CMD ["--help"]
