import logging
import os
from typing import Any, Dict, Optional, Type

from marquez_client import MarquezClient
from marquez_client.models import JobType
from openlineage.client import set_producer
from openlineage.client.facet import BaseFacet
from openlineage.client.run import Job
from openlineage.client.run import Run
from openlineage.client.run import RunEvent
from openlineage.client.run import RunState


_DEFAULT_OWNER = "anonymous"
_DEFAULT_NAMESPACE = "default"

_NAMESPACE = os.getenv("OPENLINEAGE_NAMESPACE", None)
if not _NAMESPACE:
    _NAMESPACE = os.getenv("MARQUEZ_NAMESPACE", _DEFAULT_NAMESPACE)
OPENLINEAGE_PREFECT_VERSION = "0.0.0"

_PRODUCER = f"https://github.com/OpenLineage/OpenLineage/tree/{OPENLINEAGE_PREFECT_VERSION}/integration/prefect"

set_producer(_PRODUCER)


log = logging.getLogger(__name__)


# class OpenLineageAdapter:
#     """
#     Adapter for translating prefect events to OpenLineage events.
#     """
#
#     _client = None
#
#     @property
#     def client(self) -> MarquezClient:
#         if not self._client:
#             # Back comp with Marquez integration
#             marquez_url = os.getenv("MARQUEZ_URL")
#             marquez_api_key = os.getenv("MARQUEZ_API_KEY")
#             if marquez_url:
#                 self._client = MarquezClient(marquez_url)
#             #     log.info(f"Sending lineage events to {marquez_url}")
#             #     self._client = OpenLineageClient(
#             #         marquez_url, OpenLineageClientOptions(api_key=marquez_api_key, verify=False)
#             #     )
#             # else:
#             #     self._client = OpenLineageClient.from_environment()
#         return self._client
#
#     def start_task(
#         self,
#         run_id: str,
#         job_name: str,
#         job_description: str,
#         event_time: str,
#         parent_run_id: Optional[str],
#         code_location: Optional[str],
#         inputs: Optional[Any],
#         outputs: Optional[Any],
#         run_facets: Optional[Dict[str, Type[BaseFacet]]] = None,  # Custom run facets
#     ) -> str:
#         """
#         Emits openlineage event of type START
#         :param run_id: globally unique identifier of task in dag run
#         :param job_name: globally unique identifier of task in dag
#         :param job_description: user provided description of job
#         :param event_time:
#         :param parent_run_id: identifier of job spawning this task
#         :param code_location: file path or URL of DAG file
#         :param run_facets:
#         :return:
#         """
#         event = RunEvent(
#             eventType=RunState.START,
#             eventTime=event_time,
#             run=self._build_run(run_id, parent_run_id, job_name, run_facets),
#             job=self._build_job(job_name, job_description, code_location),
#             inputs=inputs or [],
#             outputs=outputs or [],
#             producer=_PRODUCER,
#         )
#         self.client.emit(event)
#         return event.run.runId
#
#     def complete_task(
#         self,
#         run_id: str,
#         job_name: str,
#         end_time: str,
#         inputs: Optional[Any],
#         outputs: Optional[Any],
#     ):
#         """
#         Emits openlineage event of type COMPLETE
#         :param run_id: globally unique identifier of task in dag run
#         :param job_name: globally unique identifier of task between dags
#         :param end_time: time of task completion
#         :param step: metadata container with information extracted from operator
#         """
#         event = RunEvent(
#             eventType=RunState.COMPLETE,
#             eventTime=end_time,
#             run=self._build_run(run_id),
#             job=self._build_job(job_name),
#             inputs=inputs,
#             outputs=outputs,
#             producer=_PRODUCER,
#         )
#         self.client.emit(event)
#
#     def fail_task(
#         self,
#         run_id: str,
#         job_name: str,
#         end_time: str,
#         inputs: Optional[Any],
#         outputs: Optional[Any],
#     ):
#         """
#         Emits openlineage event of type FAIL
#         :param run_id: globally unique identifier of task in dag run
#         :param job_name: globally unique identifier of task between dags
#         :param end_time: time of task completion
#         :param step: metadata container with information extracted from operator
#         """
#         event = RunEvent(
#             eventType=RunState.FAIL,
#             eventTime=end_time,
#             run=self._build_run(run_id),
#             job=self._build_job(job_name),
#             inputs=inputs,
#             outputs=outputs,
#             producer=_PRODUCER,
#         )
#         self.client.emit(event)
#
#     @staticmethod
#     def _build_run(
#         run_id: str,
#         parent_run_id: Optional[str] = None,
#         job_name: Optional[str] = None,
#         custom_facets: Dict[str, Type[BaseFacet]] = None,
#     ) -> Run:
#         facets = {}
#         # if parent_run_id:
#         #     facets.update({"parentRun": ParentRunFacet.create(parent_run_id, _NAMESPACE, job_name)})
#
#         if custom_facets:
#             facets.update(custom_facets)
#
#         return Run(run_id, facets)
#
#     @staticmethod
#     def _build_job(
#         job_name: str,
#         job_description: Optional[str] = None,
#         code_location: Optional[str] = None,
#     ):
#         facets = {}
#
#         # if job_description:
#         #     facets.update({"documentation": DocumentationJobFacet(job_description)})
#         # if code_location:
#         #     facets.update({"sourceCodeLocation": SourceCodeLocationJobFacet("", code_location)})
#
#         return Job(_NAMESPACE, job_name, facets)


class MarquezAdapter:
    """
    Adapter for translating prefect events to OpenLineage events.
    """

    _client = None

    @property
    def client(self) -> MarquezClient:
        if not self._client:
            # Back comp with Marquez integration
            marquez_url = os.getenv("MARQUEZ_URL")
            if marquez_url:
                self._client = MarquezClient(marquez_url)
        return self._client

    def start_task(
        self,
        run_id: str,
        job_name: str,
        job_description: str,
        event_time: str,
        parent_run_id: Optional[str],
        code_location: Optional[str],
        inputs: Optional[Any],
        outputs: Optional[Any],
        job_type: JobType = JobType.BATCH,
        run_facets: Optional[Dict[str, Type[BaseFacet]]] = None,  # Custom run facets
    ) -> str:
        """
        Emits openlineage event of type START
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task in dag
        :param job_description: user provided description of job
        :param event_time:
        :param parent_run_id: identifier of job spawning this task
        :param code_location: file path or URL of DAG file
        :param run_facets:
        :return:
        """
        event = RunEvent(
            eventType=RunState.START,
            eventTime=event_time,
            run=self._build_run(run_id, parent_run_id, job_name, run_facets),
            job=self._build_job(job_name, job_description, code_location),
            inputs=inputs or [],
            outputs=outputs or [],
            producer=_PRODUCER,
        )
        self.client.create_job(
            namespace_name=_NAMESPACE,
            job_name=job_name,
            job_type=job_type,
        )
        self.client.create_job_run(
            namespace_name=_NAMESPACE,
            job_name=job_name,
            run_id=run_id,
        )
        return event.run.runId

    def complete_task(
        self,
        run_id: str,
        job_name: str,
        end_time: str,
        inputs: Optional[Any],
        outputs: Optional[Any],
    ):
        """
        Emits openlineage event of type COMPLETE
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param end_time: time of task completion
        :param step: metadata container with information extracted from operator
        """
        event = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=end_time,
            run=self._build_run(run_id),
            job=self._build_job(job_name),
            inputs=inputs,
            outputs=outputs,
            producer=_PRODUCER,
        )
        assert event
        self.client.mark_job_run_as_completed(run_id=run_id, at=end_time)

    def fail_task(
        self,
        run_id: str,
        job_name: str,
        end_time: str,
        inputs: Optional[Any],
        outputs: Optional[Any],
    ):
        """
        Emits openlineage event of type FAIL
        :param run_id: globally unique identifier of task in dag run
        :param job_name: globally unique identifier of task between dags
        :param end_time: time of task completion
        :param step: metadata container with information extracted from operator
        """
        event = RunEvent(
            eventType=RunState.FAIL,
            eventTime=end_time,
            run=self._build_run(run_id),
            job=self._build_job(job_name),
            inputs=inputs,
            outputs=outputs,
            producer=_PRODUCER,
        )
        assert event
        self.client.mark_job_run_as_failed(run_id=run_id, at=end_time)

    @staticmethod
    def _build_run(
        run_id: str,
        parent_run_id: Optional[str] = None,
        job_name: Optional[str] = None,
        custom_facets: Dict[str, Type[BaseFacet]] = None,
    ) -> Run:
        facets = {}
        # if parent_run_id:
        #     facets.update({"parentRun": ParentRunFacet.create(parent_run_id, _NAMESPACE, job_name)})

        if custom_facets:
            facets.update(custom_facets)

        return Run(run_id, facets)

    @staticmethod
    def _build_job(
        job_name: str,
        job_description: Optional[str] = None,
        code_location: Optional[str] = None,
    ):
        facets = {}

        # if job_description:
        #     facets.update({"documentation": DocumentationJobFacet(job_description)})
        # if code_location:
        #     facets.update({"sourceCodeLocation": SourceCodeLocationJobFacet("", code_location)})

        return Job(_NAMESPACE, job_name, facets)
