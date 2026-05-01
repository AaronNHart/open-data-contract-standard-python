# Open Data Contract Standard (Python)

The pip module `open-data-contract-standard` to read and write YAML files using the [Open Data Contract Standard](https://github.com/bitol-io/open-data-contract-standard). The pip module was extracted from the [Data Contract CLI](https://github.com/datacontract/datacontract-cli), which is its primary user.

The version number of the pip module corresponds to the version of the Open Data Contract Standard it supports.

## Version Mapping

| Open Data Contract Standard Version | Pip Module Version |
|-------------------------------------|--------------------|
| 3.0.1                               | >=3.0.1            |
| 3.0.2                               | >=3.0.4            |
| 3.1.0                               | >=3.1.0            |

**Note**: We mirror major and minor version from the ODCS to the pip module, but not the patch version!

## Installation

```bash
pip install open-data-contract-standard
```

## Usage

```python
from open_data_contract_standard.model import OpenDataContractStandard

# Load a data contract specification from a file
data_contract = OpenDataContractStandard.from_file('path/to/your/data_contract.yaml')
# Print the data contract specification as a YAML string
print(data_contract.to_yaml())
```

```python
from open_data_contract_standard.model import OpenDataContractStandard

# Load a data contract specification from a string
data_contract_str = """
version: 1.0.0
kind: DataContract
id: 53581432-6c55-4ba2-a65f-72344a91553b
status: active
name: my_table
apiVersion: v3.1.0
"""
data_contract = OpenDataContractStandard.from_string(data_contract_str)
# Print the data contract specification as a YAML string
print(data_contract.to_yaml())
```


## Changes in this fork

This fork tightens schema enforcement so the Pydantic model matches the [ODCS JSON schema](src/open_data_contract_standard/schema.json) more strictly. Loading a contract that violates the schema now raises `ValidationError` instead of silently accepting it.

Highlights:

- **Required fields enforced**: `version`, `apiVersion`, `kind`, `id`, `status` at the top level; `name` on schema objects/properties; `username`, `role`, `channel`, `property`/`value`, `url`/`type` on their respective models.
- **Enums enforced**: `kind`, `apiVersion`, `status`, `Server.type`, `DataQuality.type`/`dimension`/`severity`, `Support.tool`/`scope`, `Relationship.type`, `AuthoritativeDefinition.type`, etc.
- **Server type-specific validation**: each `Server.type` (e.g. `bigquery`, `snowflake`, `kafka`) requires its mandatory fields and rejects fields that don't belong to that type per the schema's `unevaluatedProperties: false`.
- **DataQuality type-specific validation**: `library` requires `metric`, `sql` requires `query`, `custom` requires `engine`/`implementation`. Comparison operators (`mustBe`, `mustBeGreaterThan`, ...) are mutually exclusive and only allowed where the schema permits them. `mustBeBetween`/`mustNotBeBetween` must contain unique values.
- **SchemaProperty constraints**: `items` only valid when `logicalType: array`; `properties` only valid when `logicalType: object`. `logicalTypeOptions` keys are validated against the logical type, with `minLength`/`maxLength`/`minItems`/etc. required to be non-negative and `multipleOf` strictly positive. Schema defaults are applied (`primaryKey`/`required`/`unique`/`partitioned`/`criticalDataElement` default to `False`; `primaryKeyPosition`/`partitionKeyPosition` default to `-1`).
- **Relationship constraints**: `to` is required; `type` defaults to `foreignKey` and only accepts `foreignKey`; composite keys must be matching arrays of equal length.
- **Pattern validations**: `id` fields match the `StableId` pattern; relationship `from`/`to` match shorthand or fully-qualified reference patterns; `dateIn`/`dateOut` are `YYYY-MM-DD`; `contractCreatedTs` accepts full ISO-8601 (`Z`, timezone offsets, optional milliseconds).
- **`SchemaItemProperty`**: a new class used for array `items`, where `name` is optional (per the schema's `SchemaItemProperty` definition).
- **Strict extras**: every model uses `extra='forbid'`, so unknown fields are rejected.

These changes are a **breaking change** relative to earlier versions of the package, hence the major version bump to 4.x. Contracts that previously round-tripped silently may now fail validation; the failures point at real schema violations.

## Development

```
uv sync --all-extras
pre-commit install
```

Tests must keep coverage at 80% or higher (`pytest --cov-fail-under=80`).

## Release

- Change version number in `pyproject.toml`
- Run `./release` in your command line
- Wait for the releases on [GitHub](https://github.com/datacontract/open-data-contract-standard-python/releases), [PyPi](https://test.pypi.org/project/open-data-contract-standard/) and [PyPi (test)](https://test.pypi.org/project/open-data-contract-standard/)
