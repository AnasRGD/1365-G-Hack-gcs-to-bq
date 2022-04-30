import os
import time
from typing import Optional

from environment import Environment


class Context(object):
    def __init__(self):
        self.env: Optional[Environment] = None
        self.start_time: float = time.perf_counter()

    def remaining_time(self) -> float:
        """
        Get the time remaining (in seconds) before the function is cut.
        Based on the environment variable FUNCTION_TIMEOUT_SEC
        :return: time remaining in seconds
        """
        now: float = time.perf_counter()
        return self.start_time + self.env.timeout - now

    def load_environment(self) -> None:
        project_id = os.environ["GCP_PROJECT"]
        configuration_bucket = os.environ.get("CONFIGURATION_BUCKET")
        function_name = os.environ.get("FUNCTION_NAME")
        timeout = os.environ.get("FUNCTION_TIMEOUT_SEC") or "540"

        pub_sub_topic = os.environ.get("PUB_SUB_TOPIC")
        if pub_sub_topic is None:
            pub_sub_topic = "gcs_to_bq_integration_done"
        pub_sub_error_topic = os.environ.get("PUB_SUB_ERROR_TOPIC")
        if pub_sub_error_topic is None:
            pub_sub_error_topic = "gcs_to_bq_integration_done"

        self.env = Environment(
            project_id=project_id,
            configuration_bucket=configuration_bucket,
            function_name=function_name,
            pub_sub_topic=pub_sub_topic,
            pub_sub_error_topic=pub_sub_error_topic,
            timeout=float(timeout),
        )
