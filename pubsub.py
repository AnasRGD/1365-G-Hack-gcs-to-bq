from json import dumps

from google.cloud import pubsub_v1

import gcp_logging

publisher = pubsub_v1.PublisherClient()


class PubSubTopic(object):
    def __init__(self, project_id, name):
        self.project_id = project_id
        self.name = name


class PubSub(object):
    def __init__(self, topic: PubSubTopic):
        self.publisher = pubsub_v1.PublisherClient()
        self.topic = topic

    def publish(self, payload: object, encoder=None) -> None:
        """
        Publish a Pub/Sub event to a topic.

        :param payload: Pub/Sub event payload
        :param encoder:
        """
        topic_path = self.build_topic_path(self.topic)

        if isinstance(payload, str):
            payload_str = payload
        else:
            payload_str = dumps(payload, cls=encoder)

        try:
            future = publisher.publish(topic_path, payload_str.encode("utf-8"))
            message_id = future.result()
        except Exception as e:
            raise PublishException(topic_path=topic_path, payload=payload_str) from e

        gcp_logging.info(
            f"Pub/Sub message published to {topic_path} ({message_id})\n"
            f"payload: {payload_str}"
        )

    def build_topic_path(self, topic: PubSubTopic) -> str:
        """
        Builds topic path from Pub/Sub topic reference.

        :param topic: Pub/Sub topic reference
        """
        return self.publisher.topic_path(topic.project_id, topic.name)


class PublishException(Exception):
    def __init__(self, topic_path: str, payload: str):
        super(Exception, self).__init__()
        self.topic_path = topic_path
        self.payload = payload

    def __str__(self):
        return (
            f"Unable to publish Pub/Sub message "
            f"(topic: {self.topic_path}, payload: {self.payload})"
        )
