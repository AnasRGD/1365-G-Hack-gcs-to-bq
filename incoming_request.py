from typing import List, Optional, Union


class BodyRequest(object):
    def __init__(self, body: dict):
        self.bucket_id: str = body["bucketId"]
        self.object_id: Optional[str] = body.get("objectId")
        self.folder: Optional[str] = body.get("folder")

        self.additional_columns: List[AdditionalColumnBodyRequest] = (
            []
            if "additionalColumns" not in body
            else list(
                map(lambda r: AdditionalColumnBodyRequest(r), body["additionalColumns"])
            )
        )

    def __str__(self):
        return (
            f"BodyRequest("
            f"objectId={self.object_id},"
            f"folder={self.folder},"
            f"bucketId={self.bucket_id},"
            f'additional_columns={",".join(map(lambda r: str(r), self.additional_columns))}'
            f")"
        )


class AdditionalColumnBodyRequest(object):
    def __init__(self, column: dict):
        self.name: str = column["name"]
        self.value: Union[str, int] = column["value"]

    def __str__(self):
        return f"AdditionalColumnBodyRequest(name={self.name},value={self.value})"
