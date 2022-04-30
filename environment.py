class Environment(object):
    def __init__(
        self,
        project_id: str,
        configuration_bucket: str,
        function_name: str,
        pub_sub_topic: str,
        pub_sub_error_topic: str,
        timeout: float,
    ):
        self.project_id: str = project_id
        self.configuration_bucket: str = configuration_bucket
        self.function_name: str = function_name
        self.pub_sub_topic: str = pub_sub_topic
        self.pub_sub_error_topic: str = pub_sub_error_topic
        self.timeout: float = timeout
