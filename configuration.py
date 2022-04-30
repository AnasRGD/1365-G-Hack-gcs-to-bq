import json
from enum import Enum
from typing import List, Optional


class Configuration(object):
    def __init__(self, configuration: dict) -> None:
        self.dataset = configuration["dataset"]
        self.table = configuration["table"]
        self.field_delimiter: str = (
            configuration["field_delimiter"]
            if "field_delimiter" in configuration
            else ";"
        )
        self.header_rows: int = (
            configuration["header_rows"] if "header_rows" in configuration else 0
        )
        self.footer_rows: int = (
            configuration["footer_rows"] if "footer_rows" in configuration else 0
        )
        self.quote_character: str = (
            configuration["quote_character"]
            if "quote_character" in configuration
            else "'"
        )

        self.file_schema: List[FileSchemaField] = list(
            map(lambda field: FileSchemaField(field), configuration["file_schema"])
        )

        self.time_partitioning: Optional[ConfigurationTimePartitioning] = (
            ConfigurationTimePartitioning(configuration["time_partitioning"])
            if "time_partitioning" in configuration
            else None
        )

        self.mode: Mode = Mode[configuration["mode"]]
        self.location: str = configuration.get("location") or "EU"
        self.additional_columns: List[AdditionalColumn] = (
            list(
                map(
                    lambda column: AdditionalColumn(column),
                    configuration["additional_columns"],
                )
            )
            if "additional_columns" in configuration
            else []
        )

    def __repr__(self):
        return {
            "dataset": self.dataset,
            "table": self.table,
            "field_delimiter": self.field_delimiter,
            "header_rows": self.header_rows,
            "footer_rows": self.footer_rows,
            "quote_character": self.quote_character,
            "file_schema": [s.__repr__() for s in self.file_schema],
            "mode": str(self.mode.value),
            "location": self.location,
            "time_partitioning": self.time_partitioning.__repr__(),
            "additional_columns": [s.__repr__() for s in self.additional_columns],
        }

    def __str__(self):
        return (
            f"Configuration(dataset={self.dataset}, "
            f"table={self.table}, "
            f"field_delimiter={self.field_delimiter}, "
            f"header_rows={self.header_rows}, "
            f"footer_rows={self.footer_rows}, "
            f"quote_character={self.quote_character}, "
            f'file_schema=[{",".join(map(lambda s: str(s), self.file_schema))}], '
            f"mode={self.mode}, "
            f"location={self.location}, "
            f"time_partitioning={self.time_partitioning},"
            f'additional_columns=[{",".join(map(lambda s: str(s), self.additional_columns))}])'
        )


class Mode(Enum):
    APPEND = "APPEND"
    TRUNCATE = "TRUNCATE"

    def __repr__(self):
        return {"mode": self.value}

    def __str__(self):
        return f"Mode(value={self.value})"


class ConfigurationEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Configuration):
            return o.__repr__()
        return json.JSONEncoder.default(self, o)


class FileSchemaField(object):
    def __init__(self, configuration: dict):
        self.name: str = configuration["name"]
        self.field_type: str = configuration["type"]
        self.required: bool = (
            configuration.get("required") if "required" in configuration else False
        )
        self.description: str = configuration.get("description")
        transformation: dict = configuration.get("transformation")
        self.transformation: FileSchemaTransformation = (
            FileSchemaTransformation(transformation)
            if transformation is not None
            else None
        )

    def __str__(self):
        return (
            f"FileSchemaField("
            f"name={self.name},"
            f"type={self.field_type},"
            f"required={self.required},"
            f"description={self.description},"
            f"transformation={self.transformation}"
            f")"
        )

    def __repr__(self):
        return {
            "name": self.name,
            "type": self.field_type,
            "required": self.required,
            "description": self.description,
            "transformation": self.transformation.__repr__()
            if self.transformation is not None
            else None,
        }


class FileSchemaTransformation(object):
    def __init__(self, configuration: dict):
        self.type: str = configuration.get("type")
        self.expression: str = configuration.get("expression")

    def __str__(self):
        return (
            f"FileSchemaTransformation(type={self.type},expression={self.expression})"
        )

    def __repr__(self):
        return {"type": self.type, "expression": self.expression}


class AdditionalColumn(object):
    def __init__(self, configuration: dict):
        self.name: str = configuration["name"]
        self.type: str = configuration["type"]
        self.required: bool = (
            configuration.get("required") if "required" in configuration else False
        )
        self.in_request: bool = (
            configuration.get("in_request") if "in_request" in configuration else False
        )
        self.description: str = configuration.get("description")
        self.transformation: Optional[AdditionalColumnTransformation] = (
            AdditionalColumnTransformation(configuration["transformation"])
            if "transformation" in configuration
            else None
        )

    def __str__(self):
        return (
            f"AdditionalColumn("
            f"name={self.name},"
            f"type={self.type},"
            f"transformation={self.transformation},"
            f"description={self.description},"
            f"required={self.required},"
            f"in_request={self.in_request}"
            f")"
        )

    def __repr__(self):
        return {
            "name": self.name,
            "type": self.type,
            "description": self.description,
            "required": self.required,
            "in_request": self.in_request,
            "transformation": self.transformation.__repr__()
            if self.transformation is not None
            else None,
        }


class AdditionalColumnTransformation(object):
    def __init__(self, configuration: dict):
        self.expression: str = configuration.get("expression")

    def __str__(self):
        return f"AdditionalColumnTransformation(" f"expression={self.expression})"

    def __repr__(self):
        return {"expression": self.expression}


class ConfigurationTimePartitioning(object):
    def __init__(self, configuration: dict):
        self.type: str = configuration["type"]
        # optional fields
        self.field: str = configuration.get("field")
        self.expiration_day: int = configuration.get("expiration_day")

    def __str__(self) -> str:
        return (
            f"TimePartitioning(type={self.type},"
            f"field={self.field},"
            f"expiration_day={self.expiration_day})"
        )

    def __repr__(self):
        return {
            "type": self.type,
            "field": self.field,
            "expiration_day": self.expiration_day,
        }
