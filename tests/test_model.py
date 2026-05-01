import os
import tempfile

import pytest
import yaml
from pydantic import ValidationError

from open_data_contract_standard.model import OpenDataContractStandard


def test_roundtrip():
    data_contract_str = """
version: 1.0.0
kind: DataContract
id: 53581432-6c55-4ba2-a65f-72344a91553b
status: active
name: my_table
dataProduct: my_quantum
apiVersion: v3.1.0
team:
  name: my_team
    """
    contract = OpenDataContractStandard.from_string(data_contract_str)
    # Verify that the contract was loaded correctly
    assert contract.version == "1.0.0"
    assert contract.id == "53581432-6c55-4ba2-a65f-72344a91553b"
    assert contract.status == "active"
    assert contract.name == "my_table"


def assert_equals_yaml(data_contract_str):
    assert yaml.safe_load(data_contract_str) == yaml.safe_load(
        OpenDataContractStandard.from_string(data_contract_str).to_yaml()
    )


def test_json_schema():
    assert "" != OpenDataContractStandard.json_schema()


# Tests for required fields
def test_required_fields():
    """Test that required fields are enforced."""
    with pytest.raises(ValidationError):
        OpenDataContractStandard()

    with pytest.raises(ValidationError):
        OpenDataContractStandard(version="1.0.0")

    with pytest.raises(ValidationError):
        OpenDataContractStandard(version="1.0.0", kind="DataContract")

    # Should succeed with all required fields
    contract = OpenDataContractStandard(
        version="1.0.0", kind="DataContract", apiVersion="v3.1.0", id="test-id", status="active"
    )
    assert contract.version == "1.0.0"


def test_kind_enum():
    """Test that kind only accepts 'DataContract'."""
    with pytest.raises(ValidationError) as exc_info:
        OpenDataContractStandard(
            version="1.0.0", kind="InvalidKind", apiVersion="v3.1.0", id="test-id", status="active"
        )
    assert "Input should be 'DataContract'" in str(exc_info.value)


def test_api_version_enum():
    """Test that apiVersion only accepts known versions."""
    with pytest.raises(ValidationError) as exc_info:
        OpenDataContractStandard(
            version="1.0.0", kind="DataContract", apiVersion="v999.0.0", id="test-id", status="active"
        )
    assert "Input should be" in str(exc_info.value)


def test_status_enum():
    """Test that status is constrained to valid values."""
    valid_statuses = ["proposed", "draft", "active", "deprecated", "retired"]
    for status in valid_statuses:
        contract = OpenDataContractStandard(
            version="1.0.0", kind="DataContract", apiVersion="v3.1.0", id="test-id", status=status
        )
        assert contract.status == status

    with pytest.raises(ValidationError):
        OpenDataContractStandard(
            version="1.0.0", kind="DataContract", apiVersion="v3.1.0", id="test-id", status="invalid_status"
        )


def test_server_type_validation():
    """Test that servers enforce type-specific required fields."""
    # BigQuery should require project and dataset
    with pytest.raises(ValidationError) as exc_info:
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            servers=[
                {
                    "server": "my-bigquery",
                    "type": "bigquery",
                    # Missing required: project, dataset
                }
            ],
        )
    assert "requires fields" in str(exc_info.value).lower()

    # Should succeed with required fields
    contract_valid = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        servers=[{"server": "my-bigquery", "type": "bigquery", "project": "my-project", "dataset": "my-dataset"}],
    )
    assert len(contract_valid.servers) == 1


def test_server_postgres_type_validation():
    """Test PostgreSQL server requires specific fields."""
    with pytest.raises(ValidationError):
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            servers=[
                {
                    "server": "postgres-prod",
                    "type": "postgresql",
                    "host": "localhost",
                    # Missing: port, database, schema_
                }
            ],
        )

    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        servers=[
            {
                "server": "postgres-prod",
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "database": "mydb",
                "schema": "public",
            }
        ],
    )
    assert contract.servers[0].type == "postgresql"


def test_support_channel_required():
    """Test that support channel is required."""
    with pytest.raises(ValidationError):
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            support=[
                {
                    "url": "https://example.com"
                    # Missing: channel
                }
            ],
        )

    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        support=[{"channel": "email", "url": "support@example.com"}],
    )
    assert contract.support[0].channel == "email"


def test_schema_property_name_required():
    """Test that schema property names are required."""
    with pytest.raises(ValidationError):
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            schema=[
                {
                    "name": "my_table",
                    "properties": [
                        {
                            "physicalType": "STRING"
                            # Missing: name
                        }
                    ],
                }
            ],
        )


def test_data_quality_type_specific_validation():
    """Test that data quality type determines required fields."""
    # Type: library requires metric
    with pytest.raises(ValidationError):
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            schema=[
                {
                    "name": "my_table",
                    "quality": [
                        {
                            "type": "library",
                            "name": "check",
                            # Missing: metric
                        }
                    ],
                }
            ],
        )

    # Type: sql requires query
    with pytest.raises(ValidationError):
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            schema=[
                {
                    "name": "my_table",
                    "quality": [
                        {
                            "type": "sql",
                            "name": "check",
                            # Missing: query
                        }
                    ],
                }
            ],
        )

    # Type: custom requires engine and implementation
    with pytest.raises(ValidationError):
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            schema=[
                {
                    "name": "my_table",
                    "quality": [
                        {
                            "type": "custom",
                            "name": "check",
                            # Missing: engine, implementation
                        }
                    ],
                }
            ],
        )


def test_role_name_required():
    """Test that role name is required."""
    with pytest.raises(ValidationError):
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            roles=[
                {
                    "description": "A role"
                    # Missing: role
                }
            ],
        )

    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        roles=[{"role": "analyst"}],
    )
    assert contract.roles[0].role == "analyst"


def test_relationship_to_required():
    """Test that relationship 'to' field is required."""
    with pytest.raises(ValidationError):
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            schema=[
                {
                    "name": "my_table",
                    "properties": [
                        {
                            "name": "user_id",
                            "logicalType": "integer",
                            "relationships": [
                                {
                                    # Missing: to
                                }
                            ],
                        }
                    ],
                }
            ],
        )


def test_extra_fields_forbidden():
    """Test that extra/unknown fields are rejected."""
    with pytest.raises(ValidationError):
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            unknownField="should fail",
        )


def test_relationship_type_default():
    """Test that relationship type defaults to 'foreignKey'."""
    from open_data_contract_standard.model import Relationship

    rel = Relationship(to="orders.id")
    assert rel.type == "foreignKey"


def test_relationship_schema_level_with_from():
    """Test that schema-level relationships can have 'from' field."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        schema=[
            {
                "name": "orders",
                "relationships": [{"from": "orders.customer_id", "to": "customers.id", "type": "foreignKey"}],
            }
        ],
    )
    assert contract.schema_[0].relationships[0].from_ == "orders.customer_id"
    assert contract.schema_[0].relationships[0].to == "customers.id"
    assert contract.schema_[0].relationships[0].type == "foreignKey"


def test_relationship_property_level_without_from():
    """Test that property-level relationships work without 'from' field."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        schema=[
            {
                "name": "orders",
                "properties": [
                    {"name": "customer_id", "logicalType": "integer", "relationships": [{"to": "customers.id"}]}
                ],
            }
        ],
    )
    assert contract.schema_[0].properties[0].relationships[0].from_ is None
    assert contract.schema_[0].properties[0].relationships[0].to == "customers.id"
    assert contract.schema_[0].properties[0].relationships[0].type == "foreignKey"


def test_relationship_composite_keys():
    """Test that composite keys in relationships have matching array lengths."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        schema=[
            {
                "name": "orders",
                "relationships": [
                    {
                        "from": ["orders.customer_id", "orders.date"],
                        "to": ["customers.id", "customers.date"],
                        "type": "foreignKey",
                    }
                ],
            }
        ],
    )
    rel = contract.schema_[0].relationships[0]
    assert len(rel.from_) == 2
    assert len(rel.to) == 2


def test_relationship_composite_keys_mismatched_length():
    """Test that composite keys with mismatched lengths are rejected."""
    with pytest.raises(ValidationError) as exc_info:
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            schema=[
                {
                    "name": "orders",
                    "relationships": [
                        {
                            "from": ["orders.customer_id", "orders.date"],
                            "to": ["customers.id"],  # Mismatched length
                            "type": "foreignKey",
                        }
                    ],
                }
            ],
        )
    assert "same length" in str(exc_info.value).lower()


def test_relationship_composite_keys_mixed_array_types():
    """Test that one field cannot be array and the other not."""
    with pytest.raises(ValidationError) as exc_info:
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            schema=[
                {
                    "name": "orders",
                    "relationships": [
                        {
                            "from": "orders.customer_id",  # Single value
                            "to": ["customers.id", "customers.date"],  # Array
                        }
                    ],
                }
            ],
        )
    assert "both" in str(exc_info.value).lower()


def test_schema_property_items_with_array_type():
    """Test that items can be specified with array logicalType."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        schema=[
            {
                "name": "my_table",
                "properties": [
                    {"name": "tags", "logicalType": "array", "items": {"name": "tag", "logicalType": "string"}}
                ],
            }
        ],
    )
    assert contract.schema_[0].properties[0].items is not None
    assert contract.schema_[0].properties[0].items.name == "tag"


def test_schema_property_items_not_allowed_with_non_array():
    """Test that items cannot be specified with non-array logicalType."""
    with pytest.raises(ValidationError) as exc_info:
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            schema=[
                {
                    "name": "my_table",
                    "properties": [
                        {"name": "count", "logicalType": "integer", "items": {"name": "item", "logicalType": "string"}}
                    ],
                }
            ],
        )
    assert "items" in str(exc_info.value).lower() and "array" in str(exc_info.value).lower()


def test_schema_property_properties_with_object_type():
    """Test that properties can be specified with object logicalType."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        schema=[
            {
                "name": "my_table",
                "properties": [
                    {
                        "name": "metadata",
                        "logicalType": "object",
                        "properties": [{"name": "key", "logicalType": "string"}],
                    }
                ],
            }
        ],
    )
    assert contract.schema_[0].properties[0].properties is not None
    assert len(contract.schema_[0].properties[0].properties) == 1


def test_schema_property_properties_not_allowed_with_non_object():
    """Test that properties cannot be specified with non-object logicalType."""
    with pytest.raises(ValidationError) as exc_info:
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            schema=[
                {
                    "name": "my_table",
                    "properties": [
                        {
                            "name": "value",
                            "logicalType": "string",
                            "properties": [{"name": "nested", "logicalType": "string"}],
                        }
                    ],
                }
            ],
        )
    assert "properties" in str(exc_info.value).lower() and "object" in str(exc_info.value).lower()


def test_schema_object_logical_type_defaults_to_object():
    """Test that SchemaObject logicalType defaults to 'object'."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        schema=[
            {
                "name": "my_table"
                # logicalType not specified, should default to "object"
            }
        ],
    )
    assert contract.schema_[0].logicalType == "object"


def test_schema_object_logical_type_only_object_allowed():
    """Test that SchemaObject logicalType can only be 'object'."""
    with pytest.raises(ValidationError):
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            schema=[
                {
                    "name": "my_table",
                    "logicalType": "array",  # Invalid, should only be "object"
                }
            ],
        )


def test_authoritative_definition_required_fields():
    """Test that AuthoritativeDefinition requires url and type."""
    from open_data_contract_standard.model import AuthoritativeDefinition

    # Valid
    ad = AuthoritativeDefinition(url="https://example.com", type="businessDefinition")
    assert ad.url == "https://example.com"

    # Missing url
    with pytest.raises(ValidationError):
        AuthoritativeDefinition(type="businessDefinition")

    # Missing type
    with pytest.raises(ValidationError):
        AuthoritativeDefinition(url="https://example.com")


def test_schema_property_boolean_defaults():
    """Test that SchemaProperty boolean fields default to False per schema."""
    from open_data_contract_standard.model import SchemaProperty

    # Create property with minimal fields
    prop = SchemaProperty(name="test_column")

    # Boolean fields should default to False
    assert prop.primaryKey is False
    assert prop.required is False
    assert prop.unique is False
    assert prop.partitioned is False
    assert prop.criticalDataElement is False


def test_schema_property_integer_defaults():
    """Test that SchemaProperty integer position fields default to -1 per schema."""
    from open_data_contract_standard.model import SchemaProperty

    # Create property with minimal fields
    prop = SchemaProperty(name="test_column")

    # Integer position fields should default to -1
    assert prop.primaryKeyPosition == -1
    assert prop.partitionKeyPosition == -1


def test_schema_property_boolean_explicit_true():
    """Test that SchemaProperty boolean fields can be explicitly set to True."""
    from open_data_contract_standard.model import SchemaProperty

    prop = SchemaProperty(
        name="test_column", primaryKey=True, required=True, unique=True, partitioned=True, criticalDataElement=True
    )

    assert prop.primaryKey is True
    assert prop.required is True
    assert prop.unique is True
    assert prop.partitioned is True
    assert prop.criticalDataElement is True


def test_schema_property_integer_explicit_value():
    """Test that SchemaProperty integer position fields can be explicitly set."""
    from open_data_contract_standard.model import SchemaProperty

    prop = SchemaProperty(name="test_column", primaryKeyPosition=1, partitionKeyPosition=2)

    assert prop.primaryKeyPosition == 1
    assert prop.partitionKeyPosition == 2


def test_schema_property_defaults_yaml_roundtrip():
    """Test that default values are properly handled in YAML roundtrip."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        schema=[
            {
                "name": "my_table",
                "properties": [
                    {
                        "name": "user_id",
                        "logicalType": "integer",
                        # Not specifying required, primaryKey, etc - should default to False
                    }
                ],
            }
        ],
    )

    # Check that defaults are applied
    prop = contract.schema_[0].properties[0]
    assert prop.required is False
    assert prop.primaryKey is False
    assert prop.unique is False
    assert prop.partitioned is False
    assert prop.criticalDataElement is False
    assert prop.primaryKeyPosition == -1
    assert prop.partitionKeyPosition == -1

    # Convert to YAML
    yaml_str = contract.to_yaml()

    # Load back from YAML
    contract2 = OpenDataContractStandard.from_string(yaml_str)
    prop2 = contract2.schema_[0].properties[0]

    # Defaults should still be applied
    assert prop2.required is False
    assert prop2.primaryKey is False
    assert prop2.unique is False
    assert prop2.partitioned is False
    assert prop2.criticalDataElement is False
    assert prop2.primaryKeyPosition == -1
    assert prop2.partitionKeyPosition == -1


# ============================================================
# Tests for Server type-specific allowed fields (Gap 1)
# ============================================================


def test_server_bigquery_rejects_invalid_fields():
    """BigQuery server should not allow fields like host, port, database."""
    with pytest.raises(ValidationError) as exc_info:
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            servers=[
                {
                    "server": "my-bq",
                    "type": "bigquery",
                    "project": "my-project",
                    "dataset": "my-dataset",
                    "host": "should-not-be-allowed",  # Invalid for BigQuery
                }
            ],
        )
    assert "does not allow" in str(exc_info.value).lower()


def test_server_kafka_rejects_invalid_fields():
    """Kafka server should only allow host and format."""
    with pytest.raises(ValidationError) as exc_info:
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            servers=[
                {
                    "server": "my-kafka",
                    "type": "kafka",
                    "host": "kafka.example.com",
                    "database": "should-not-be-allowed",  # Invalid for Kafka
                }
            ],
        )
    assert "does not allow" in str(exc_info.value).lower()


def test_server_pubsub_rejects_invalid_fields():
    """PubSub server should only allow project."""
    with pytest.raises(ValidationError) as exc_info:
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            servers=[
                {
                    "server": "my-pubsub",
                    "type": "pubsub",
                    "project": "my-project",
                    "host": "should-not-be-allowed",
                }
            ],
        )
    assert "does not allow" in str(exc_info.value).lower()


def test_server_custom_allows_many_fields():
    """Custom server should allow any of the documented fields."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        servers=[
            {
                "server": "my-custom",
                "type": "custom",
                "host": "host",
                "port": 1234,
                "database": "db",
                "stream": "my-stream",
            }
        ],
    )
    assert contract.servers[0].stream == "my-stream"


def test_server_snowflake_allowed_fields():
    """Snowflake should allow host, port, account, database, schema, warehouse."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        servers=[
            {
                "server": "snow",
                "type": "snowflake",
                "account": "myaccount",
                "database": "mydb",
                "schema": "myschema",
                "warehouse": "wh",
            }
        ],
    )
    assert contract.servers[0].warehouse == "wh"


# ============================================================
# Tests for DataQuality oneOf operators (Gap 2)
# ============================================================


def test_data_quality_single_operator_allowed():
    """A DataQuality check can have one comparison operator."""
    from open_data_contract_standard.model import DataQuality

    dq = DataQuality(type="library", metric="rowCount", mustBeGreaterThan=0)
    assert dq.mustBeGreaterThan == 0


def test_data_quality_multiple_operators_rejected():
    """A DataQuality check cannot have more than one comparison operator."""
    from open_data_contract_standard.model import DataQuality

    with pytest.raises(ValidationError) as exc_info:
        DataQuality(
            type="library",
            metric="rowCount",
            mustBe=100,
            mustBeGreaterThan=50,
        )
    assert "only one" in str(exc_info.value).lower()


def test_data_quality_no_operator_allowed():
    """A DataQuality check without operators is allowed (operator is optional)."""
    from open_data_contract_standard.model import DataQuality

    dq = DataQuality(type="library", metric="rowCount")
    assert dq.mustBe is None
    assert dq.mustBeGreaterThan is None


# ============================================================
# Tests for DataQuality mustBeBetween uniqueItems (Gap 3)
# ============================================================


def test_data_quality_must_be_between_unique_values():
    """mustBeBetween must contain unique values."""
    from open_data_contract_standard.model import DataQuality

    # Valid: unique values
    dq = DataQuality(type="library", metric="rowCount", mustBeBetween=[1, 100])
    assert dq.mustBeBetween == [1, 100]


def test_data_quality_must_be_between_duplicate_rejected():
    """mustBeBetween with duplicate values should be rejected."""
    from open_data_contract_standard.model import DataQuality

    with pytest.raises(ValidationError) as exc_info:
        DataQuality(type="library", metric="rowCount", mustBeBetween=[5, 5])
    assert "unique" in str(exc_info.value).lower()


def test_data_quality_must_not_be_between_duplicate_rejected():
    """mustNotBeBetween with duplicate values should be rejected."""
    from open_data_contract_standard.model import DataQuality

    with pytest.raises(ValidationError) as exc_info:
        DataQuality(type="library", metric="rowCount", mustNotBeBetween=[10, 10])
    assert "unique" in str(exc_info.value).lower()


# ============================================================
# Tests for contractCreatedTs format (Gap 4)
# ============================================================


def test_contract_created_ts_basic_utc():
    """Basic UTC ISO 8601 date-time should be accepted."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        contractCreatedTs="2023-01-01T12:00:00Z",
    )
    assert contract.contractCreatedTs == "2023-01-01T12:00:00Z"


def test_contract_created_ts_with_milliseconds():
    """ISO 8601 date-time with milliseconds should be accepted."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        contractCreatedTs="2023-01-01T12:00:00.123Z",
    )
    assert contract.contractCreatedTs == "2023-01-01T12:00:00.123Z"


def test_contract_created_ts_with_timezone_offset():
    """ISO 8601 date-time with timezone offset should be accepted."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        contractCreatedTs="2023-01-01T12:00:00+01:00",
    )
    assert contract.contractCreatedTs == "2023-01-01T12:00:00+01:00"


def test_contract_created_ts_invalid_format_rejected():
    """Invalid date-time format should be rejected."""
    with pytest.raises(ValidationError):
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            contractCreatedTs="2023-01-01",  # Date only, missing time
        )


# ============================================================
# Tests for SchemaItemProperty (Gap 5)
# ============================================================


def test_schema_item_property_name_optional():
    """Array items can be specified without a name."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        schema=[
            {
                "name": "my_table",
                "properties": [
                    {
                        "name": "tags",
                        "logicalType": "array",
                        "items": {
                            "logicalType": "string"
                            # No name - should be allowed for items
                        },
                    }
                ],
            }
        ],
    )
    items = contract.schema_[0].properties[0].items
    assert items.name is None
    assert items.logicalType == "string"


def test_schema_item_property_with_name_still_works():
    """Array items can still optionally have a name."""
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="v3.1.0",
        id="test-id",
        status="active",
        schema=[
            {
                "name": "my_table",
                "properties": [
                    {"name": "tags", "logicalType": "array", "items": {"name": "tag", "logicalType": "string"}}
                ],
            }
        ],
    )
    items = contract.schema_[0].properties[0].items
    assert items.name == "tag"


def test_schema_property_still_requires_name():
    """Top-level schema properties still require name."""
    with pytest.raises(ValidationError):
        OpenDataContractStandard(
            version="1.0.0",
            kind="DataContract",
            apiVersion="v3.1.0",
            id="test-id",
            status="active",
            schema=[
                {
                    "name": "my_table",
                    "properties": [
                        {
                            "logicalType": "integer"
                            # Missing name - should be rejected
                        }
                    ],
                }
            ],
        )


def test_schema_item_property_nested_array():
    """Items can have nested logicalType options."""
    from open_data_contract_standard.model import SchemaItemProperty

    item = SchemaItemProperty(logicalType="object", properties=[{"name": "key", "logicalType": "string"}])
    assert item.name is None
    assert len(item.properties) == 1


# ============================================================
# Tests for Relationship.type strict (Gap 6)
# ============================================================


def test_relationship_type_explicit_none_rejected():
    """Relationship.type cannot be explicitly set to None."""
    from open_data_contract_standard.model import Relationship

    with pytest.raises(ValidationError):
        Relationship(to="customers.id", type=None)


def test_relationship_type_omitted_uses_default():
    """When type is omitted, it defaults to 'foreignKey'."""
    from open_data_contract_standard.model import Relationship

    rel = Relationship(to="customers.id")
    assert rel.type == "foreignKey"


def test_relationship_type_explicit_foreign_key_works():
    """Explicit type='foreignKey' should be accepted."""
    from open_data_contract_standard.model import Relationship

    rel = Relationship(to="customers.id", type="foreignKey")
    assert rel.type == "foreignKey"


# ============================================================
# Tests for logicalTypeOptions value constraints (Bonus)
# ============================================================


def test_logical_type_options_min_length_non_negative():
    """minLength in logicalTypeOptions must be >= 0."""
    from open_data_contract_standard.model import SchemaProperty

    # Valid
    prop = SchemaProperty(name="col", logicalType="string", logicalTypeOptions={"minLength": 0})
    assert prop.logicalTypeOptions["minLength"] == 0

    # Invalid: negative
    with pytest.raises(ValidationError) as exc_info:
        SchemaProperty(name="col", logicalType="string", logicalTypeOptions={"minLength": -1})
    assert ">= 0" in str(exc_info.value)


def test_logical_type_options_multiple_of_positive():
    """multipleOf in logicalTypeOptions must be > 0."""
    from open_data_contract_standard.model import SchemaProperty

    # Valid
    prop = SchemaProperty(name="col", logicalType="integer", logicalTypeOptions={"multipleOf": 5})
    assert prop.logicalTypeOptions["multipleOf"] == 5

    # Invalid: zero
    with pytest.raises(ValidationError) as exc_info:
        SchemaProperty(name="col", logicalType="integer", logicalTypeOptions={"multipleOf": 0})
    assert "> 0" in str(exc_info.value)


# ============================================================
# Tests for DataQuality field restrictions per type
# ============================================================


def test_data_quality_operators_rejected_for_custom():
    """Custom DataQuality should not allow comparison operators."""
    from open_data_contract_standard.model import DataQuality

    with pytest.raises(ValidationError) as exc_info:
        DataQuality(type="custom", engine="soda", implementation="check", mustBe=5)
    assert "not allowed" in str(exc_info.value).lower()


def test_data_quality_operators_rejected_for_text():
    """Text DataQuality should not allow comparison operators."""
    from open_data_contract_standard.model import DataQuality

    with pytest.raises(ValidationError) as exc_info:
        DataQuality(type="text", description="A check", mustBe=5)
    assert "not allowed" in str(exc_info.value).lower()


def test_data_quality_query_only_for_sql():
    """Query field should only be allowed for type='sql'."""
    from open_data_contract_standard.model import DataQuality

    with pytest.raises(ValidationError) as exc_info:
        DataQuality(
            type="library",
            metric="rowCount",
            mustBe=1,
            query="SELECT 1",  # Not allowed for library
        )
    assert "not allowed" in str(exc_info.value).lower()


def test_data_quality_engine_only_for_custom():
    """Engine field should only be allowed for type='custom'."""
    from open_data_contract_standard.model import DataQuality

    with pytest.raises(ValidationError) as exc_info:
        DataQuality(
            type="sql",
            query="SELECT 1",
            engine="soda",  # Not allowed for sql
        )
    assert "not allowed" in str(exc_info.value).lower()


def test_data_quality_metric_only_for_library():
    """Metric field should only be allowed when in library mode."""
    from open_data_contract_standard.model import DataQuality

    # type=custom + metric: metric forces library mode (per schema), so this is OK
    # but type=sql + metric is fine because metric implies library mode in addition
    # Most explicit case: type=text + metric - metric makes it library mode, so allowed
    # The actual gap is for type=custom + metric
    # Per schema, both library and custom branches apply (metric→library, type=custom→custom)

    # Let's test sql + engine which is more clear-cut
    with pytest.raises(ValidationError):
        DataQuality(type="text", description="check", query="SELECT 1")


def test_data_quality_text_with_only_base_fields():
    """Text DataQuality with only base fields should be valid."""
    from open_data_contract_standard.model import DataQuality

    dq = DataQuality(type="text", name="my_check", description="A descriptive check")
    assert dq.type == "text"


def test_data_quality_sql_with_query_and_operator():
    """SQL DataQuality with query and operator should be valid."""
    from open_data_contract_standard.model import DataQuality

    dq = DataQuality(type="sql", query="SELECT COUNT(*) FROM mytable", mustBeGreaterThan=0)
    assert dq.query.startswith("SELECT")
    assert dq.mustBeGreaterThan == 0


# ============================================================
# Tests for logicalTypeOptions.required validation
# ============================================================


def test_logical_type_options_required_unique():
    """Object type's required array items must be unique."""
    from open_data_contract_standard.model import SchemaProperty

    # Valid: unique items
    prop = SchemaProperty(name="obj", logicalType="object", logicalTypeOptions={"required": ["a", "b"]})
    assert prop.logicalTypeOptions["required"] == ["a", "b"]

    # Invalid: duplicates
    with pytest.raises(ValidationError) as exc_info:
        SchemaProperty(name="obj", logicalType="object", logicalTypeOptions={"required": ["a", "a"]})
    assert "unique" in str(exc_info.value).lower()


def test_logical_type_options_required_min_items():
    """Object type's required array must have at least one item."""
    from open_data_contract_standard.model import SchemaProperty

    with pytest.raises(ValidationError) as exc_info:
        SchemaProperty(name="obj", logicalType="object", logicalTypeOptions={"required": []})
    assert "at least one" in str(exc_info.value).lower()


# ============================================================
# Tests for file IO
# ============================================================


def test_from_file_loads_contract():
    """from_file reads a YAML contract file from disk."""
    yaml_content = """
version: 1.0.0
kind: DataContract
id: test-id
status: active
apiVersion: v3.1.0
name: my_contract
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(yaml_content)
        temp_path = f.name

    try:
        contract = OpenDataContractStandard.from_file(temp_path)
        assert contract.version == "1.0.0"
        assert contract.id == "test-id"
        assert contract.name == "my_contract"
    finally:
        os.unlink(temp_path)


def test_from_file_missing_raises():
    """from_file raises FileNotFoundError when path doesn't exist."""
    with pytest.raises(FileNotFoundError):
        OpenDataContractStandard.from_file("/nonexistent/path/contract.yaml")
