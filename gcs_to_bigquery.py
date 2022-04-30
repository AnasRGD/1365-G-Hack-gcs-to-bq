import os
import time
from concurrent.futures import TimeoutError
from http.client import RemoteDisconnected
from typing import List, Optional

from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.cloud.bigquery import Table, TimePartitioning
from google.cloud.bigquery.external_config import ExternalConfig, ExternalSourceFormat
from google.cloud.bigquery.job import QueryJob, QueryJobConfig
from google.cloud.bigquery.schema import SchemaField
from google.cloud.bigquery.table import Row, RowIterator
from google.cloud.exceptions import GoogleCloudError
from urllib3.exceptions import ReadTimeoutError

import gcp_logging
from configuration import AdditionalColumn, Configuration, FileSchemaField
from context import Context
from incoming_request import AdditionalColumnBodyRequest


class GCStoBigQuery(object):
    def __init__(
        self,
        source_uris: List[str],
        configuration: Configuration,
        additional_columns_request: List[AdditionalColumnBodyRequest],
    ) -> None:
        self.bigquery_client: bigquery.Client = bigquery.Client()
        self.source_uris: List[str] = source_uris
        self.configuration: Configuration = configuration
        self.additional_columns_request: List[
            AdditionalColumnBodyRequest
        ] = additional_columns_request

    def run(self, ctx: Context) -> (str, Exception, int):
        tmp_table_name: str = f"{self.configuration.table}_tmp"
        job_config: QueryJobConfig = QueryJobConfig()
        job_config.table_definitions = {tmp_table_name: self.__csv_configuration()}

        # In truncate mode, try to ensure there is data in incoming file
        # Otherwise the table would be empty, probably due to wrong incoming file
        query: str = f"SELECT COUNT(*) as nb_lines FROM {tmp_table_name}"
        query_result: (RowIterator, Exception) = self.__run_query(
            ctx, query, job_config
        )
        if query_result[1] is not None:
            return "ERROR", query_result[1], None

        r: Row = next(iter(query_result[0]))
        nb_lines: int = r.get("nb_lines") or 0

        if nb_lines == 0:
            gcp_logging.info(f"No rows to process for file {self.source_uris}")
            return "DONE", None, nb_lines

        # We set the destination table to write data in
        # We also set corresponding parameters
        destination_table_id: str = (
            f'{os.environ["GCP_PROJECT"]}.'
            f"{self.configuration.dataset}."
            f"{self.configuration.table}"
        )

        table_schema: List[SchemaField] = build_table_schema(
            self.configuration.file_schema, self.configuration.additional_columns
        )
        job_config.destination = Table(destination_table_id, table_schema)
        job_config.write_disposition = f"WRITE_{self.configuration.mode.value}"
        job_config.create_disposition = "CREATE_IF_NEEDED"
        job_config.time_partitioning = self.__time_partitioning()

        query: str = build_query(
            self.configuration.file_schema,
            self.configuration.additional_columns,
            self.additional_columns_request,
            tmp_table_name,
        )

        query_result: (RowIterator, Exception) = self.__run_query(
            ctx, query, job_config
        )
        if query_result[1] is not None:
            return "ERROR", query_result[1], nb_lines

        gcp_logging.info(f"{nb_lines} lines treated")
        return "DONE", None, nb_lines

    def __run_query(
        self, ctx: Context, query: str, job_config: QueryJobConfig
    ) -> (RowIterator, Exception):
        query_job: QueryJob = self.bigquery_client.query(
            query, job_config=job_config, location=self.configuration.location
        )

        while True:
            # Poll every seconds until timeout has been reached or job is not running anymore (i.e complete)
            time.sleep(1)

            # Check if there is enough time remaining to complete the query
            # We consider that cancellation can take up to 20 seconds to be effective
            # that's why we cancel if there is less than 20 seconds remaining
            to_cancel = ctx.remaining_time() < 20
            if to_cancel:
                cancellation_sent = query_job.cancel()
                if cancellation_sent:
                    gcp_logging.info(f"bigquery cancellation sent")
                    break

            # Refresh the job properties from BigQuery API then update his running status
            query_job.done()
            running = query_job.running()
            if not running:
                break

        try:
            # In every case, we try to receive the result
            # Even if we cancelled, the job possibly ended normally before the cancellation was done
            result: RowIterator = query_job.result()
        except GoogleCloudError as e:
            gcp_logging.error(f"The query job failed: {str(e)}")
            return None, e
        except (TimeoutError, ReadTimeoutError) as e:
            gcp_logging.error(
                f"Query job timed out, timeout was {ctx.env.timeout}, got error {str(e)}"
            )
            return None, e
        except BadRequest as e:
            gcp_logging.error(f"Error happened in the BigQuery request: {str(e)}")
            return None, e
        except RemoteDisconnected as e:
            gcp_logging.error(f"Client lost connection: {str(e)}")
            return None, e
        except Exception as e:
            gcp_logging.error(f"Got unexpected error: {str(e)}")
            raise e

        return result, None

    def __csv_configuration(self) -> ExternalConfig:
        """
        Configure the external data source and query job
        """
        external_config: ExternalConfig = ExternalConfig(ExternalSourceFormat.CSV)
        external_config.source_uris = self.source_uris
        external_config.schema = build_external_file_schema(
            self.configuration.file_schema
        )
        external_config.options.field_delimiter = self.configuration.field_delimiter
        external_config.options.skip_leading_rows = self.configuration.header_rows
        external_config.max_bad_records = self.configuration.footer_rows
        external_config.ignore_unknown_values = True
        external_config.options.quote_character = self.configuration.quote_character
        return external_config

    def __time_partitioning(self) -> Optional[TimePartitioning]:
        if self.configuration.time_partitioning is None:
            return None

        # convert into milliseconds: 86400000 ms = 1 day
        expiration_ms: int = (
            self.configuration.time_partitioning.expiration_day * 86400000
            if self.configuration.time_partitioning.expiration_day is not None
            else None
        )
        return TimePartitioning(
            self.configuration.time_partitioning.type,
            self.configuration.time_partitioning.field,
            expiration_ms=expiration_ms,
        )


def build_external_file_schema(fields: List[FileSchemaField]) -> List[SchemaField]:
    return list(map(lambda f: SchemaField(f.name, f.field_type), fields))


def build_table_schema(
    fields: List[FileSchemaField], additional_columns: List[AdditionalColumn]
) -> List[SchemaField]:
    def build_field(field: FileSchemaField) -> SchemaField:
        field_type: str = field.field_type
        if field.transformation is not None and field.transformation.type is not None:
            field_type = field.transformation.type

        bq_field: SchemaField
        if field.required:
            bq_field = SchemaField(
                field.name, field_type, description=field.description, mode="REQUIRED"
            )
        else:
            bq_field = SchemaField(
                field.name, field_type, description=field.description
            )
        return bq_field

    results: List[SchemaField] = list(map(build_field, fields))

    if additional_columns is None:
        return results

    for col in additional_columns:
        mode: str = "REQUIRED" if col.required else "NULLABLE"
        results.append(
            SchemaField(col.name, col.type, mode=mode, description=col.description)
        )

    return results


def build_query(
    fields: List[FileSchemaField],
    additional_columns: List[AdditionalColumn],
    additional_columns_request: List[AdditionalColumnBodyRequest],
    tmp_table_name: str,
) -> str:
    query_fields: List[str] = [
        f.name
        if f.transformation is None
        else f"{f.transformation.expression} AS {f.name}"
        for f in fields
    ]

    for col in additional_columns:
        query_field: str
        if col.in_request:
            col_request: AdditionalColumnBodyRequest = next(
                r for r in additional_columns_request if r.name == col.name
            )
            expression: str = col_request.value
            if (
                col.transformation is not None
                and col.transformation.expression is not None
            ):
                expression = col.transformation.expression.replace(
                    col.name, str(col_request.value)
                )
            query_field = f"{expression} AS {col.name}"
        else:
            query_field = f"{col.transformation.expression} AS {col.name}"

        query_fields.append(query_field)

    field_part: str = ", \n  ".join(query_fields)
    return f"SELECT \n  {field_part} \nFROM {tmp_table_name}"
