import importlib.resources as impresources
import os
import typing
from typing import Annotated, Any, Literal, Union

import pydantic as pyd
import yaml
from pydantic import Field, field_validator, model_validator

# Patterns from schema
STABLE_ID_PATTERN = r"^[A-Za-z0-9_-]+$"
SHORTHAND_REFERENCE_PATTERN = r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$"
FULLY_QUALIFIED_REFERENCE_PATTERN = r"^(?:(?:https?:\/\/)?[A-Za-z0-9._\-\/]+\.yaml#)?/?[A-Za-z_][A-Za-z0-9_]*\/[A-Za-z0-9_-]+(?:\/[A-Za-z_][A-Za-z0-9_]*\/[A-Za-z0-9_-]+)*$"

# Type aliases for validated strings
StableId = Annotated[str, Field(pattern=STABLE_ID_PATTERN)]
ShorthandRef = Annotated[str, Field(pattern=SHORTHAND_REFERENCE_PATTERN)]
FullyQualifiedRef = Annotated[str, Field(pattern=FULLY_QUALIFIED_REFERENCE_PATTERN)]


class AuthoritativeDefinition(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    id: StableId | None = None
    url: str
    type: Literal["businessDefinition", "transformationImplementation", "videoTutorial", "tutorial", "implementation"]
    description: str | None = None


class CustomProperty(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    id: StableId | None = None
    property: str
    value: Any
    description: str | None = None


class Support(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    id: StableId | None = None
    channel: str
    url: str | None = None
    description: str | None = None
    tool: Literal["email", "slack", "teams", "discord", "ticket", "googlechat", "other"] | None = None
    scope: Literal["interactive", "announcements", "issues", "notifications"] | None = None
    invitationUrl: str | None = None
    customProperties: list[CustomProperty] | None = None


class Pricing(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    id: StableId | None = None
    priceAmount: float | int | None = None
    priceCurrency: str | None = None
    priceUnit: str | None = None


class TeamMember(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    id: StableId | None = None
    username: str
    name: str | None = None
    description: str | None = None
    role: str | None = None
    dateIn: str | None = Field(None, pattern=r"^\d{4}-\d{2}-\d{2}$")  # YYYY-MM-DD format
    dateOut: str | None = Field(None, pattern=r"^\d{4}-\d{2}-\d{2}$")
    replacedByUsername: str | None = None
    tags: list[str] | None = None
    customProperties: list[CustomProperty] | None = None
    authoritativeDefinitions: list[AuthoritativeDefinition] | None = None


class Team(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    id: StableId | None = None
    name: str | None = None
    description: str | None = None
    members: list[TeamMember] | None = None
    tags: list[str] | None = None
    customProperties: list[CustomProperty] | None = None
    authoritativeDefinitions: list[AuthoritativeDefinition] | None = None


class ServiceLevelAgreementProperty(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    id: StableId | None = None
    property: str
    value: str | float | int | bool | None
    valueExt: str | float | int | bool | None = None
    unit: str | None = None
    element: str | None = None
    driver: Literal["regulatory", "analytics", "operational"] | None = None
    description: str | None = None
    scheduler: str | None = None
    schedule: str | None = None


DATA_QUALITY_OPERATORS = (
    "mustBe",
    "mustNotBe",
    "mustBeGreaterThan",
    "mustBeGreaterOrEqualTo",
    "mustBeLessThan",
    "mustBeLessOrEqualTo",
    "mustBeBetween",
    "mustNotBeBetween",
)


class DataQuality(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    id: StableId | None = None
    authoritativeDefinitions: list[AuthoritativeDefinition] | None = None
    businessImpact: Literal["operational", "regulatory"] | None = None
    customProperties: list[CustomProperty] | None = None
    description: str | None = None
    dimension: (
        Literal["accuracy", "completeness", "conformity", "consistency", "coverage", "timeliness", "uniqueness"] | None
    ) = None
    method: str | None = None
    name: str | None = None
    schedule: str | None = None
    scheduler: str | None = None
    severity: Literal["info", "warning", "error"] | None = None
    tags: list[str] | None = None
    type: Literal["text", "library", "sql", "custom"] | None = None
    unit: str | None = None
    metric: Literal["nullValues", "missingValues", "invalidValues", "duplicateValues", "rowCount"] | None = None
    rule: str | None = None  # Deprecated: Use metric instead
    arguments: dict[str, Any] | None = None
    mustBe: Any | None = None
    mustNotBe: Any | None = None
    mustBeGreaterThan: float | int | None = None
    mustBeGreaterOrEqualTo: float | int | None = None
    mustBeLessThan: float | int | None = None
    mustBeLessOrEqualTo: float | int | None = None
    mustBeBetween: list[float | int] | None = Field(None, min_length=2, max_length=2)
    mustNotBeBetween: list[float | int] | None = Field(None, min_length=2, max_length=2)
    query: str | None = None
    engine: str | None = None
    implementation: str | dict[str, Any] | None = None

    @field_validator("mustBeBetween", "mustNotBeBetween")
    @classmethod
    def validate_unique_between(cls, v: list[float | int] | None) -> list[float | int] | None:
        if v is not None and len(v) != len(set(v)):
            raise ValueError("must contain unique items")
        return v

    @model_validator(mode="after")
    def validate_type_specific_fields(self) -> "DataQuality":
        if self.type == "library" or self.metric:
            if not self.metric:
                raise ValueError("metric is required when type='library'")
        elif self.type == "sql":
            if not self.query:
                raise ValueError("query is required when type='sql'")
        elif self.type == "custom":
            if not self.engine or not self.implementation:
                raise ValueError("engine and implementation are required when type='custom'")
        return self

    @model_validator(mode="after")
    def validate_operator_exclusivity(self) -> "DataQuality":
        set_operators = [op for op in DATA_QUALITY_OPERATORS if getattr(self, op) is not None]
        if len(set_operators) > 1:
            raise ValueError(f"Only one comparison operator may be specified, got: {set_operators}")
        return self

    @model_validator(mode="after")
    def validate_field_per_type(self) -> "DataQuality":
        """Enforce per-type field restrictions per schema's allOf with unevaluatedProperties: false."""
        is_library = self.type == "library" or self.metric is not None
        is_sql = self.type == "sql"
        is_custom = self.type == "custom"

        library_fields = {"metric", "rule", "arguments"} | set(DATA_QUALITY_OPERATORS)
        sql_fields = {"query"} | set(DATA_QUALITY_OPERATORS)
        custom_fields = {"engine", "implementation"}

        allowed: set[str] = set()
        if is_library:
            allowed |= library_fields
        if is_sql:
            allowed |= sql_fields
        if is_custom:
            allowed |= custom_fields

        all_type_specific = library_fields | sql_fields | custom_fields
        disallowed = sorted(f for f in all_type_specific - allowed if getattr(self, f) is not None)
        if disallowed:
            type_label = self.type or "unspecified"
            raise ValueError(f"Fields not allowed for type='{type_label}': {', '.join(disallowed)}")
        return self


class Description(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    usage: str | None = None
    purpose: str | None = None
    limitations: str | None = None
    authoritativeDefinitions: list[AuthoritativeDefinition] | None = None
    customProperties: list[CustomProperty] | None = None


class Relationship(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    type: Literal["foreignKey"] = "foreignKey"
    from_: Union[ShorthandRef, FullyQualifiedRef, list[Union[ShorthandRef, FullyQualifiedRef]]] | None = Field(
        default=None, alias="from"
    )
    to: Union[ShorthandRef, FullyQualifiedRef, list[Union[ShorthandRef, FullyQualifiedRef]]] | None = None
    customProperties: list[CustomProperty] | None = None

    @model_validator(mode="after")
    def validate_relationship(self) -> "Relationship":
        if not self.to:
            raise ValueError("'to' field is required in relationship")

        if self.from_ is not None and self.to is not None:
            from_is_array = isinstance(self.from_, list)
            to_is_array = isinstance(self.to, list)
            if from_is_array != to_is_array:
                raise ValueError("'from' and 'to' must both be single values or both be arrays")
            if from_is_array and len(self.from_) != len(self.to):
                raise ValueError("'from' and 'to' arrays must have the same length for composite keys")

        return self


_LOGICAL_TYPE_OPTION_KEYS: dict[str, set[str]] = {
    "string": {"minLength", "maxLength", "pattern", "format"},
    "date": {"format", "exclusiveMaximum", "maximum", "exclusiveMinimum", "minimum", "timezone", "defaultTimezone"},
    "timestamp": {
        "format",
        "exclusiveMaximum",
        "maximum",
        "exclusiveMinimum",
        "minimum",
        "timezone",
        "defaultTimezone",
    },
    "time": {"format", "exclusiveMaximum", "maximum", "exclusiveMinimum", "minimum", "timezone", "defaultTimezone"},
    "integer": {"multipleOf", "maximum", "exclusiveMaximum", "minimum", "exclusiveMinimum", "format"},
    "number": {"multipleOf", "maximum", "exclusiveMaximum", "minimum", "exclusiveMinimum", "format"},
    "object": {"maxProperties", "minProperties", "required"},
    "array": {"maxItems", "minItems", "uniqueItems"},
}


class _SchemaPropertyBase(pyd.BaseModel):
    """Shared fields and validators for SchemaProperty and SchemaItemProperty."""

    model_config = pyd.ConfigDict(extra="forbid")

    id: StableId | None = None
    physicalType: str | None = None
    physicalName: str | None = None
    description: str | None = None
    businessName: str | None = None
    authoritativeDefinitions: list[AuthoritativeDefinition] | None = None
    tags: list[str] | None = None
    customProperties: list[CustomProperty] | None = None
    primaryKey: bool = False
    primaryKeyPosition: int = -1
    logicalType: (
        Literal["string", "date", "timestamp", "time", "number", "integer", "object", "array", "boolean"] | None
    ) = None
    logicalTypeOptions: dict[str, Any] | None = None
    required: bool = False
    unique: bool = False
    partitioned: bool = False
    partitionKeyPosition: int = -1
    classification: str | None = None
    encryptedName: str | None = None
    transformSourceObjects: list[str] | None = None
    transformLogic: str | None = None
    transformDescription: str | None = None
    examples: list[Any] | None = None
    criticalDataElement: bool = False
    relationships: list[Relationship] | None = None
    quality: list[DataQuality] | None = None
    properties: list["SchemaProperty"] | None = None
    items: typing.Optional["SchemaItemProperty"] = None

    @model_validator(mode="after")
    def validate_logical_type_options(self):
        self._validate_logical_type_options_keys()
        self._validate_items_with_array_type()
        self._validate_properties_with_object_type()
        return self

    def _validate_logical_type_options_keys(self) -> None:
        if not self.logicalTypeOptions:
            return
        self._check_allowed_option_keys()
        self._check_non_negative_options()
        self._check_multiple_of_positive()
        self._check_object_required_constraints()

    def _check_allowed_option_keys(self) -> None:
        allowed_keys = _LOGICAL_TYPE_OPTION_KEYS.get(self.logicalType, set())
        if not allowed_keys:
            return
        invalid_keys = set(self.logicalTypeOptions.keys()) - allowed_keys
        if invalid_keys:
            raise ValueError(f"Invalid logicalTypeOptions for {self.logicalType}: {invalid_keys}")

    def _check_non_negative_options(self) -> None:
        non_negative_keys = {"minLength", "maxLength", "maxProperties", "minProperties", "maxItems", "minItems"}
        for key in non_negative_keys & set(self.logicalTypeOptions.keys()):
            value = self.logicalTypeOptions[key]
            if isinstance(value, int) and value < 0:
                raise ValueError(f"logicalTypeOptions.{key} must be >= 0, got {value}")

    def _check_multiple_of_positive(self) -> None:
        if "multipleOf" not in self.logicalTypeOptions:
            return
        value = self.logicalTypeOptions["multipleOf"]
        if isinstance(value, (int, float)) and value <= 0:
            raise ValueError(f"logicalTypeOptions.multipleOf must be > 0, got {value}")

    def _check_object_required_constraints(self) -> None:
        if self.logicalType != "object" or "required" not in self.logicalTypeOptions:
            return
        required = self.logicalTypeOptions["required"]
        if not isinstance(required, list):
            return
        if len(required) < 1:
            raise ValueError("logicalTypeOptions.required must have at least one item")
        if len(required) != len(set(required)):
            raise ValueError("logicalTypeOptions.required items must be unique")

    def _validate_items_with_array_type(self) -> None:
        if self.items is not None and self.logicalType != "array":
            raise ValueError("'items' can only be specified when logicalType is 'array'")

    def _validate_properties_with_object_type(self) -> None:
        if self.properties is not None and self.logicalType != "object":
            raise ValueError("'properties' can only be specified when logicalType is 'object'")


class SchemaProperty(_SchemaPropertyBase):
    """Top-level schema property where 'name' is required."""

    name: str


class SchemaItemProperty(_SchemaPropertyBase):
    """Schema property used as array items where 'name' is optional."""

    name: str | None = None


class SchemaObject(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    id: StableId | None = None
    name: str
    physicalType: str | None = None
    description: str | None = None
    businessName: str | None = None
    authoritativeDefinitions: list[AuthoritativeDefinition] | None = None
    tags: list[str] | None = None
    customProperties: list[CustomProperty] | None = None
    logicalType: Literal["object"] = "object"
    physicalName: str | None = None
    dataGranularityDescription: str | None = None
    properties: list[SchemaProperty] | None = None
    relationships: list[Relationship] | None = None
    quality: list[DataQuality] | None = None


class Role(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    id: StableId | None = None
    role: str
    description: str | None = None
    access: str | None = None
    firstLevelApprovers: str | None = None
    secondLevelApprovers: str | None = None
    customProperties: list[CustomProperty] | None = None


SERVER_TYPE_REQUIRED_FIELDS: dict[str, list[str]] = {
    "api": ["location"],
    "athena": ["stagingDir", "schema_"],
    "azure": ["location", "format"],
    "bigquery": ["project", "dataset"],
    "clickhouse": ["host", "port", "database"],
    "databricks": ["catalog", "schema_"],
    "denodo": ["host", "port"],
    "dremio": ["host", "port"],
    "duckdb": ["database"],
    "glue": ["account", "database"],
    "cloudsql": ["host", "port", "database", "schema_"],
    "db2": ["host", "port", "database"],
    "hive": ["host", "database"],
    "impala": ["host", "database"],
    "informix": ["host", "database"],
    "zen": ["host", "database"],
    "kafka": ["host"],
    "kinesis": [],
    "local": ["path", "format"],
    "mysql": ["host", "port", "database"],
    "oracle": ["host", "port", "serviceName"],
    "postgresql": ["host", "port", "database", "schema_"],
    "postgres": ["host", "port", "database", "schema_"],
    "presto": ["host"],
    "pubsub": ["project"],
    "redshift": ["database", "schema_"],
    "s3": ["location"],
    "sftp": ["location"],
    "snowflake": ["account", "database", "schema_"],
    "sqlserver": ["host", "database", "schema_"],
    "synapse": ["host", "port", "database"],
    "trino": ["host", "port", "catalog", "schema_"],
    "vertica": ["host", "port", "database", "schema_"],
    "custom": [],
}

# Per-type allowed type-specific fields (per JSON schema unevaluatedProperties: false)
SERVER_TYPE_ALLOWED_FIELDS: dict[str, set[str]] = {
    "api": {"location"},
    "athena": {"stagingDir", "schema_", "catalog", "regionName"},
    "azure": {"location", "format", "delimiter"},
    "bigquery": {"project", "dataset"},
    "clickhouse": {"host", "port", "database"},
    "databricks": {"host", "catalog", "schema_"},
    "denodo": {"host", "port", "database"},
    "dremio": {"host", "port", "schema_"},
    "duckdb": {"database", "schema_"},
    "glue": {"account", "database", "location", "format"},
    "cloudsql": {"host", "port", "database", "schema_"},
    "db2": {"host", "port", "database", "schema_"},
    "hive": {"host", "port", "database"},
    "impala": {"host", "port", "database"},
    "informix": {"host", "port", "database"},
    "zen": {"host", "port", "database"},
    "kafka": {"host", "format"},
    "kinesis": {"region", "format"},
    "local": {"path", "format"},
    "mysql": {"host", "port", "database"},
    "oracle": {"host", "port", "serviceName"},
    "postgresql": {"host", "port", "database", "schema_"},
    "postgres": {"host", "port", "database", "schema_"},
    "presto": {"host", "catalog", "schema_"},
    "pubsub": {"project"},
    "redshift": {"host", "database", "schema_", "region", "account"},
    "s3": {"location", "endpointUrl", "format", "delimiter"},
    "sftp": {"location", "format", "delimiter"},
    "snowflake": {"host", "port", "account", "database", "schema_", "warehouse"},
    "sqlserver": {"host", "port", "database", "schema_"},
    "synapse": {"host", "port", "database"},
    "trino": {"host", "port", "catalog", "schema_"},
    "vertica": {"host", "port", "database", "schema_"},
    "custom": {
        "account",
        "catalog",
        "database",
        "dataset",
        "delimiter",
        "endpointUrl",
        "format",
        "host",
        "location",
        "path",
        "port",
        "project",
        "region",
        "regionName",
        "schema_",
        "serviceName",
        "stagingDir",
        "warehouse",
        "stream",
    },
}

# Set of all type-specific (non-base) Server fields
SERVER_TYPE_SPECIFIC_FIELDS: set[str] = {
    "account",
    "catalog",
    "database",
    "dataset",
    "delimiter",
    "endpointUrl",
    "format",
    "host",
    "location",
    "path",
    "port",
    "project",
    "region",
    "regionName",
    "schema_",
    "serviceName",
    "stagingDir",
    "stream",
    "warehouse",
}


class Server(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    id: StableId | None = None
    server: str
    type: Literal[
        "api",
        "athena",
        "azure",
        "bigquery",
        "clickhouse",
        "databricks",
        "denodo",
        "dremio",
        "duckdb",
        "glue",
        "cloudsql",
        "db2",
        "hive",
        "impala",
        "informix",
        "kafka",
        "kinesis",
        "local",
        "mysql",
        "oracle",
        "postgresql",
        "postgres",
        "presto",
        "pubsub",
        "redshift",
        "s3",
        "sftp",
        "snowflake",
        "sqlserver",
        "synapse",
        "trino",
        "vertica",
        "zen",
        "custom",
    ]
    description: str | None = None
    environment: str | None = None
    roles: list[Role] | None = None
    customProperties: list[CustomProperty] | None = None

    # Type-specific fields (only valid for certain server types per schema)
    account: str | None = None
    catalog: str | None = None
    database: str | None = None
    dataset: str | None = None
    delimiter: str | None = None
    endpointUrl: str | None = None
    format: str | None = None
    host: str | None = None
    location: str | None = None
    path: str | None = None
    port: int | None = None
    project: str | None = None
    region: str | None = None
    regionName: str | None = None
    schema_: str | None = Field(default=None, alias="schema")
    serviceName: str | None = None
    stagingDir: str | None = None
    stream: str | None = None
    warehouse: str | None = None

    @model_validator(mode="after")
    def validate_server_type_requirements(self) -> "Server":
        """Validate that required fields for each server type are present."""
        required_fields = SERVER_TYPE_REQUIRED_FIELDS.get(self.type, [])
        missing_fields = [f for f in required_fields if getattr(self, f, None) is None]

        if missing_fields:
            display = [("schema" if f == "schema_" else f) for f in missing_fields]
            raise ValueError(f"Server type '{self.type}' requires fields: {', '.join(display)}")

        return self

    @model_validator(mode="after")
    def validate_server_type_allowed_fields(self) -> "Server":
        """Validate that only type-specific fields allowed for this type are set."""
        allowed = SERVER_TYPE_ALLOWED_FIELDS.get(self.type, set())
        set_fields = {f for f in SERVER_TYPE_SPECIFIC_FIELDS if getattr(self, f, None) is not None}
        invalid = set_fields - allowed

        if invalid:
            display = sorted(("schema" if f == "schema_" else f) for f in invalid)
            raise ValueError(f"Server type '{self.type}' does not allow fields: {', '.join(display)}")

        return self


class OpenDataContractStandard(pyd.BaseModel):
    model_config = pyd.ConfigDict(extra="forbid")

    version: str
    kind: Literal["DataContract"] = "DataContract"
    apiVersion: Literal["v3.1.0", "v3.0.2", "v3.0.1", "v3.0.0", "v2.2.2", "v2.2.1", "v2.2.0"] = "v3.1.0"
    id: str
    status: Literal["proposed", "draft", "active", "deprecated", "retired"]
    name: str | None = None
    tenant: str | None = None
    tags: list[str] | None = None
    servers: list[Server] | None = None
    dataProduct: str | None = None
    description: Description | None = None
    domain: str | None = None
    schema_: list[SchemaObject] | None = Field(default=None, alias="schema")
    support: list[Support] | None = None
    price: Pricing | None = None
    team: Team | list[TeamMember] | None = None
    roles: list[Role] | None = None
    slaDefaultElement: str | None = Field(default=None, deprecated=True)
    slaProperties: list[ServiceLevelAgreementProperty] | None = None
    authoritativeDefinitions: list[AuthoritativeDefinition] | None = None
    customProperties: list[CustomProperty] | None = None
    contractCreatedTs: str | None = Field(
        None, pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$"
    )  # ISO 8601 date-time

    def to_yaml(self) -> str:
        return yaml.dump(
            self.model_dump(exclude_defaults=True, exclude_none=True, by_alias=True),
            sort_keys=False,
            allow_unicode=True,
        )

    @classmethod
    def from_file(cls, file_path: str) -> "OpenDataContractStandard":
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file '{file_path}' does not exist.")
        with open(file_path, "r", encoding="utf-8") as file:
            file_content = file.read()
        return cls.from_string(file_content)

    @classmethod
    def from_string(cls, data_contract_str: str) -> "OpenDataContractStandard":
        data = yaml.safe_load(data_contract_str)
        return cls(**data)

    @classmethod
    def json_schema(cls):
        package_name = __package__
        json_schema = "schema.json"
        with impresources.open_text(package_name, json_schema) as file:
            return file.read()
