import os
from tempfile import TemporaryFile
from typing import Iterator, List, Optional

import yaml
from google.api_core.exceptions import NotFound
from google.cloud import error_reporting, storage
from google.cloud.storage import Blob

import gcp_logging
from configuration import Configuration, ConfigurationEncoder
from context import Context
from environment import Environment
from gcs_to_bigquery import GCStoBigQuery
from incoming_request import BodyRequest
from pubsub import PubSub, PubSubTopic


def process(
    ctx: Context, request: BodyRequest, execution_id: str
) -> (str, Exception, Optional[int], List[str]):
    if request.object_id is not None:
        filename = request.object_id.split("/")[-1]
        gcp_logging.info(f"work on file: {filename}")
        object_ids: List[str] = [request.object_id]
    else:
        gcp_logging.info(f"work on folder: {request.folder}")
        object_ids: List[str] = find_object_ids(request.bucket_id, request.folder)
        if len(object_ids) == 0:
            return "DONE", None, None, object_ids

    reporting_client = error_reporting.Client(
        service=ctx.env.function_name, version=ctx.env.version
    )

    source_uris: List[str] = [
        f"gs://{request.bucket_id}/{object_id}" for object_id in object_ids
    ]

    try:
        configuration: Configuration = extract_configuration_from_gcs(request, ctx.env)
        gcp_logging.info(f"Configuration found")
        gcs_to_bq: GCStoBigQuery = GCStoBigQuery(
            source_uris, configuration, request.additional_columns
        )
        gcp_logging.info("running integration")
        run_result: (str, Exception, Optional[int]) = gcs_to_bq.run(ctx)
        result: (str, Exception, Optional[int], List[str]) = (
            run_result[0],
            run_result[1],
            run_result[2],
            object_ids,
        )
    except Exception as e:
        reporting_client.report_exception()
        gcp_logging.error(str(e))
        result: (str, Exception, Optional[int], List[str]) = (
            "ERROR",
            e,
            None,
            object_ids,
        )

    status: str = result[0]

    gcp_logging.info(
        f"Integration for {request.folder or os.path.split(request.object_id)[0]}: {result}"
    )

    for object_id in object_ids:
        # if integrate by folder, not setting number of lines per object but per folder
        lines_treated: Optional[int] = (
            result[2] if request.object_id is not None else None
        )
        send_pub_sub_message(
            status,
            request.bucket_id,
            ctx.env,
            result[1],
            execution_id,
            nb_lines_treated=lines_treated,
            object_id=object_id,
        )

    if request.object_id is None:
        # Sending a message for the folder with the lines
        # Statistics will be found from it
        send_pub_sub_message(
            status,
            request.bucket_id,
            ctx.env,
            result[1],
            execution_id,
            nb_lines_treated=result[2],
            folder=request.folder,
        )

    return result


def send_pub_sub_message(
    status: str,
    bucket_id: str,
    environment: Environment,
    errors: Exception,
    execution_id: str,
    nb_lines_treated: Optional[int] = None,
    object_id: Optional[str] = None,
    folder: Optional[str] = None,
):
    topic_name = (
        environment.pub_sub_error_topic
        if status.upper() == "ERROR"
        else environment.pub_sub_topic
    )
    topic = PubSubTopic(environment.project_id, topic_name)
    PubSub(topic).publish(
        payload={
            "project": environment.project_id,
            "result": status.upper(),
            "errors": str(errors),
            "bucketId": bucket_id,
            "objectId": object_id,
            "folder": folder,
            "executionId": execution_id,
            "linesTreated": nb_lines_treated,
        },
        encoder=ConfigurationEncoder,
    )


def extract_configuration_from_gcs(
    request: BodyRequest, environment: Environment
) -> Configuration:
    """
    Extract job configuration from YAML file

    :param environment: environment execution
    :param request: Cloud Storage request triggered
    :return: the configuration as object
    """
    gcp_logging.info(f"extract configuration based on {request}")
    folder: str = request.folder or os.path.split(request.object_id)[0]
    configuration_uri = (
        f"gs://{environment.configuration_bucket}/{folder}/configuration.yaml"
    )
    gcp_logging.info(f"configuration uri: {configuration_uri}")

    storage_client: storage.Client = storage.Client()

    with TemporaryFile() as f:
        try:
            storage_client.download_blob_to_file(configuration_uri, f)
        except NotFound:
            raise ConfigurationNotFoundException(configuration_uri)

        f.seek(0)
        configuration: str = f.read()

    return Configuration(yaml.load(configuration, Loader=yaml.FullLoader))


class ConfigurationNotFoundException(RuntimeError):
    def __init__(self, configuration_uri: str):
        super(Exception, self).__init__()
        self.configuration_uri: str = configuration_uri

    def __str__(self):
        return (
            f"Unable to find configuration file " f"(file URI: {self.configuration_uri}"
        )


def find_object_ids(bucket_id: str, folder: str) -> List[str]:
    storage_client: storage.Client = storage.Client()
    prefix: str = f"{folder}/"
    blobs: Iterator[Blob] = storage_client.list_blobs(bucket_id, prefix=prefix)

    # Take only object id which have no other subfolder
    # i.e those without any remaining '/' beyond prefix
    start_search: int = len(prefix)

    return [blob.name for blob in blobs if blob.name.find("/", start_search) == -1]
