# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import threading
import time
import tenacity
from collections import defaultdict
from typing import Tuple, Optional

from airflow.settings import pod_mutation_hook
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.state import State
from datetime import datetime as dt
from datetime import timedelta
from kubernetes.client.models.v1_pod import V1Pod
from kubernetes import watch, client
from kubernetes.client.rest import ApiException
from kubernetes.stream import stream as kubernetes_stream
from airflow import AirflowException
from requests.exceptions import BaseHTTPError
from .kube_client import get_kube_client
from airflow.kubernetes.pod_generator import PodDefaults


class PodStatus:
    PENDING = 'pending'
    RUNNING = 'running'
    FAILED = 'failed'
    SUCCEEDED = 'succeeded'


class PodLauncher(LoggingMixin):
    def __init__(self, kube_client=None, in_cluster=True, cluster_context=None,
                 extract_xcom=False):
        super().__init__()
        self._client = kube_client or get_kube_client(in_cluster=in_cluster,
                                                      cluster_context=cluster_context)

        self._watch = watch.Watch()
        self.extract_xcom = extract_xcom

    def run_pod_async(self, pod: V1Pod, **kwargs):
        pod_mutation_hook(pod)

        sanitized_pod = self._client.api_client.sanitize_for_serialization(pod)
        json_pod = json.dumps(sanitized_pod, indent=2)

        self.log.debug('Pod Creation Request: \n%s', json_pod)
        try:
            resp = self._client.create_namespaced_pod(body=sanitized_pod,
                                                      namespace=pod.metadata.namespace, **kwargs)
            self.log.debug('Pod Creation Response: %s', resp)
        except Exception as e:
            self.log.exception('Exception when attempting '
                               'to create Namespaced Pod: %s', json_pod)
            raise e
        return resp

    def delete_pod(self, pod: V1Pod):
        try:
            self._client.delete_namespaced_pod(
                pod.metadata.name, pod.metadata.namespace, body=client.V1DeleteOptions())
        except ApiException as e:
            # If the pod is already deleted
            if e.status != 404:
                raise

    def run_pod(
            self,
            pod: V1Pod,
            startup_timeout: int = 120,
            get_logs: bool = True,
            get_resource_usage_logs: bool = False,
            resource_usage_logs_interval: int = 60) -> Tuple[State, Optional[str]]:
        """
        Launches the pod synchronously and waits for completion.

        :param pod:
        :param startup_timeout: Timeout for startup of the pod (if pod is pending for too long, fails task)
        :param get_logs:  whether to query k8s for logs
        :param get_resource_usage_logs: whether to get resource usage of the pod or not
        :param resource_usage_logs_interval: How often (in seconds) to fetch resource utilization
        :return:
        """
        resp = self.run_pod_async(pod)
        curr_time = dt.now()
        if resp.status.start_time is None:
            while self.pod_not_started(pod):
                delta = dt.now() - curr_time
                if delta.seconds >= startup_timeout:
                    raise AirflowException("Pod took too long to start")
                time.sleep(1)
            self.log.debug('Pod not yet started')

        return self._monitor_pod(pod, get_logs, get_resource_usage_logs, resource_usage_logs_interval)

    def _monitor_pod(self,
                     pod: V1Pod,
                     get_logs: bool,
                     get_resource_usage_logs: bool,
                     resource_usage_logs_interval: int) -> Tuple[State, Optional[str]]:
        # if resource usages logs are requested start a thread to do it.
        resource_monitoring_thread = threading.Thread(target=self._log_pod_resource_usage,
                                                      args=(pod, resource_usage_logs_interval))
        if get_resource_usage_logs:
            resource_monitoring_thread.start()
        if get_logs:
            logs = self.read_pod_logs(pod)
            for line in logs:
                self.log.info(line)
        result = None
        if self.extract_xcom:
            while self.base_container_is_running(pod):
                self.log.info('Container %s has state %s', pod.metadata.name, State.RUNNING)
                time.sleep(2)
            result = self._extract_xcom(pod)
            self.log.info(result)
            result = json.loads(result)
        while self.pod_is_running(pod):
            self.log.info('Pod %s has state %s', pod.metadata.name, State.RUNNING)
            time.sleep(2)
        if get_resource_usage_logs:
            resource_monitoring_thread.join()
        return self._task_status(self.read_pod(pod)), result

    def _log_pod_resource_usage(self, pod: V1Pod, resource_usage_logs_interval: int = 0):
        if resource_usage_logs_interval <= 0:
            self.log.error('Resource usage log: Parameter resource_usage_logs_interval must be positive. '
                           'Cancelling pod usage monitoring thread.')
            return
        self.log.info('Resource usage log: pod usage monitoring thread started for pod: {0}'.format(pod.metadata.name))
        resource_usage_api_url = "/apis/metrics.k8s.io/v1beta1/namespaces/{0}/pods/{1}".format(pod.metadata.namespace,
                                                                                               pod.metadata.name)
        cur_time = dt.now()
        last_reported_time = cur_time - timedelta(seconds=resource_usage_logs_interval)
        while self.pod_is_running(pod):
            if cur_time < last_reported_time + timedelta(seconds=resource_usage_logs_interval):
                time.sleep(1)
                cur_time = dt.now()
                continue
            try:
                last_reported_time = dt.now()
                resp = self._client.api_client.call_api(resource_usage_api_url,
                                                        'GET',
                                                        _preload_content=True,
                                                        response_type=object,
                                                        _return_http_data_only=True)
            except Exception as e:
                self.log.error('Resource usage log: Failed to fetch usage for pod: {0}, Exception: {1}'
                               .format(pod.metadata.name, e))
                continue
            containers_usages = self._get_cpu_memory_usage_from_api_response(resp)
            for container_name, cpu_usage, memory_usage in containers_usages:
                self.log.info('Resource usage log: pod: {0}, container: {1} -- cpu usage: {2}, memory usage: {3}'
                              .format(pod.metadata.name,
                                      container_name,
                                      cpu_usage,
                                      memory_usage))

    def _get_cpu_memory_usage_from_api_response(self, metric_api_response: dict):
        containers_usages =[]
        try:
            containers = metric_api_response['containers']
            for container in containers:
                container_name = container['name']
                cpu_usage = container['usage']['cpu']
                mem_usage = container['usage']['memory']
                containers_usages.append((container_name, cpu_usage, mem_usage))
        except Exception as e:
            self.log.error('Resource usage log: Failed to get containers usages from metric api response: {0}, '
                           'Exception: {1}'.format(metric_api_response, e))
        return containers_usages

    def _task_status(self, event):
        self.log.info(
            'Event: %s had an event of type %s',
            event.metadata.name, event.status.phase)
        status = self.process_status(event.metadata.name, event.status.phase)
        return status

    def pod_not_started(self, pod: V1Pod):
        state = self._task_status(self.read_pod(pod))
        return state == State.QUEUED

    def pod_is_running(self, pod: V1Pod):
        state = self._task_status(self.read_pod(pod))
        return state != State.SUCCESS and state != State.FAILED

    def base_container_is_running(self, pod: V1Pod):
        event = self.read_pod(pod)
        status = next(iter(filter(lambda s: s.name == 'base',
                                  event.status.container_statuses)), None)
        if not status:
            return False
        return status.state.running is not None

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def read_pod_logs(self, pod: V1Pod):
        try:
            return self._client.read_namespaced_pod_log(
                name=pod.metadata.name,
                namespace=pod.metadata.namespace,
                container='base',
                follow=True,
                tail_lines=10,
                _preload_content=False
            )
        except BaseHTTPError as e:
            raise AirflowException(
                'There was an error reading the kubernetes API: {}'.format(e)
            )

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(3),
        wait=tenacity.wait_exponential(),
        reraise=True
    )
    def read_pod(self, pod: V1Pod):
        try:
            return self._client.read_namespaced_pod(pod.metadata.name, pod.metadata.namespace)
        except BaseHTTPError as e:
            raise AirflowException(
                'There was an error reading the kubernetes API: {}'.format(e)
            )

    def _extract_xcom(self, pod: V1Pod):
        resp = kubernetes_stream(self._client.connect_get_namespaced_pod_exec,
                                 pod.metadata.name, pod.metadata.namespace,
                                 container=PodDefaults.SIDECAR_CONTAINER_NAME,
                                 command=['/bin/sh'], stdin=True, stdout=True,
                                 stderr=True, tty=False,
                                 _preload_content=False)
        try:
            result = self._exec_pod_command(
                resp, 'cat {}/return.json'.format(PodDefaults.XCOM_MOUNT_PATH))
            self._exec_pod_command(resp, 'kill -s SIGINT 1')
        finally:
            resp.close()
        if result is None:
            raise AirflowException('Failed to extract xcom from pod: {}'.format(pod.metadata.name))
        return result

    def _exec_pod_command(self, resp, command):
        if resp.is_open():
            self.log.info('Running command... %s\n', command)
            resp.write_stdin(command + '\n')
            while resp.is_open():
                resp.update(timeout=1)
                if resp.peek_stdout():
                    return resp.read_stdout()
                if resp.peek_stderr():
                    self.log.info(resp.read_stderr())
                    break

    def process_status(self, job_id, status):
        status = status.lower()
        if status == PodStatus.PENDING:
            return State.QUEUED
        elif status == PodStatus.FAILED:
            self.log.info('Event with job id %s Failed', job_id)
            return State.FAILED
        elif status == PodStatus.SUCCEEDED:
            self.log.info('Event with job id %s Succeeded', job_id)
            return State.SUCCESS
        elif status == PodStatus.RUNNING:
            return State.RUNNING
        else:
            self.log.info('Event: Invalid state %s on job %s', status, job_id)
            return State.FAILED
