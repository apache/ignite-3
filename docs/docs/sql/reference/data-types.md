---
id: data-types
title: SQL Data Types
sidebar_label: Data Types
---

# Data Types

The page contains a list of SQL data types available in Apache Ignite such as string, numeric, and date/time types.

Every SQL type is mapped to a programming language or driver specific type that is supported by Apache Ignite natively.

## Boolean Types

### BOOLEAN

Possible values: `TRUE` and `FALSE`.

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| BOOLEAN | BOOLEAN | Boolean | bool | bool |

## Numeric Types

### TINYINT

Possible values: `[-128, 127]`.

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| INT8 | TINYINT | Byte | sbyte | std::int8_t |

### SMALLINT

Possible values: [`-32768`, `32767`].

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| INT16 | SMALLINT | Short | short | std::int16_t |

### INT

Possible values: [`-2147483648`, `2147483647`].

Alias: `INTEGER`

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| INT32 | INT | Integer | int | std::int32_t |

### BIGINT

Possible values: [`-9223372036854775808`, `9223372036854775807`].

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| INT64 | BIGINT | Long | long | std::int64_t |

### DECIMAL

Possible values: Exact number of selectable precision.

Default precision: `32767`

Maximum precision: `32767`

Default scale: `0`

Maximum scale: `16383`

#### Decimal Precision and Scale in Apache Ignite

Apache Ignite has the following specifics when handling decimal values:

- You can specify scale that is larger than precision. In this case, the column will only hold fractional values, and the number of 0 digits to the right of the decimal point must be the same as scale minus precision. For example if you use the following declaration:

```sql
DECIMAL(3, 6)
```

You can store values between -0.000999 and 0.000999, inclusive.

- `BigDecimal` data type is derived as `DECIMAL(28, 6)`. If there are more than 6 digits after the decimal point, they will be dropped. If a value larger than the precision is passed, an out of range exception will occur.

To store larger decimal values, cast them with custom precision, for example `CAST(? as DECIMAL(100, 50))`.

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| DECIMAL | DECIMAL | BigDecimal | BigDecimal | big_decimal |

### REAL

Possible values: A single precision (32-bit) IEEE 754 floating-point number.

Special values: `NaN`, `-Infinity`, `+Infinity`

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| FLOAT | REAL | Float | float | float |

### DOUBLE

Possible values: A double precision (64-bit) IEEE 754 floating-point number.

Alias: `DOUBLE PRECISION`

Special values: `NaN`, `-Infinity`, `+Infinity`

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| DOUBLE | DOUBLE | Double | double | double |

## Character String Types

### VARCHAR

Possible values: A Unicode string.

Alias: `CHARACTER VARYING`

Default length: `65536`

Maximum length: `2147483648`

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| STRING | VARCHAR | String | string | std::string |

### CHAR (Limited Support)

:::warning
This type can only be used in expressions (such as CAST('a' AS CHAR(3)), and it cannot be used in DDL statements such as `CREATE TABLE`, `ALTER TABLE`, `ADD COLUMN`, etc. Use [VARCHAR](#varchar) instead.
:::

Fixed length Unicode string padded with spaces.

Default length: `1`

Maximum length: `65536`

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| STRING | CHAR | String | string | std::string |

## Binary String Types

### VARBINARY

Possible values: binary data ("byte array").

Aliases: `BINARY`, `BINARY VARYING`

Default length: `65536`

Maximum length: `2147483648`

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| BYTE_ARRAY | VARBINARY | byte[] | byte[] | std::vector\<std::byte\> |

## Date and Time Types

### TIME

:::note
The following Java types are not supported and cannot be used from [table API](/docs/3.1.0/develop/work-with-data/table-api):

- `java.sql.Time`
- `java.util.Date`
:::

Possible values: The time data type. The format is `hh:mm:ss`.

Default precision: `0`

Maximum precision: `3`

Mapped to: `LocalTime`

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| TIME | TIME | LocalTime | LocalTime | ignite_time |

### DATE

:::note
The following Java types are not supported and cannot be used from [table API](/docs/3.1.0/develop/work-with-data/table-api):

- `java.sql.Time`
- `java.util.Date`
:::

Possible values: The date data type.

The format is `yyyy-MM-dd`.

Mapped to: `LocalDate`

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| DATE | DATE | LocalDate | LocalDate | ignite_date |

### TIMESTAMP

:::warning
The timestamp data type only supports precision up to milliseconds (3 symbols). Any values past the 3rd symbol will be ignored.
:::

:::note
The following Java types are not supported and cannot be used from [table API](/docs/3.1.0/develop/work-with-data/table-api):

- `java.sql.Time`
- `java.util.Date`
:::

Possible values: The timestamp data type. The format is `yyyy-MM-dd hh:mm:ss[.mmm]`.

Default precision: `6`

Maximum precision: `9`

Mapped to:

- `LocalDateTime` when used without time zone.
- `Instant` when used with time zone.

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| DATETIME | TIMESTAMP | LocalDateTime | LocalDateTime | ignite_date_time |

### TIMESTAMP WITH LOCAL TIMEZONE

:::warning
The timestamp with local time zone data type only supports precision up to milliseconds (3 symbols). Any values past the 3rd symbol will be ignored.
:::

:::note
The following Java types are not supported and cannot be used from [table API](/docs/3.1.0/develop/work-with-data/table-api):

- `java.sql.Time`
- `java.util.Date`
:::

Possible values: The timestamp data type that accounts for the user's local time zone offset. The time zone offset is not stored as part of column data. When retrieved, the value is automatically converted to the time zone of the session. The format is `yyyy-MM-dd hh:mm:ss[.mmm]`.

Default precision: `6`

Maximum precision: `9`

Mapped to:

- `LocalDateTime` when used without time zone.
- `Instant` when used with time zone.

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| DATETIME | TIMESTAMP | LocalDateTime | LocalDateTime | ignite_date_time |

## Other Types

### UUID

Possible values: Universally unique identifier. This is a 128 bit value.

Example UUID: `7d24b70e-25d5-45ed-a5fa-39d8e1d966b9`

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| UUID | UUID | UUID | Guid | uuid |

### NULL

A field containing the null value.

#### Type Mapping

| ColumnType | SQL | Java | .NET | C++ |
|------------|-----|------|------|-----|
| NULL | NULL | Void | Null | nullptr |

## Implicit Type Conversion

In Apache Ignite 3, implicit type conversion is limited to types within the same type family. The table below covers the possible implicit conversions:

| Type Family | Available Types |
|-------------|-----------------|
| Boolean | `BOOLEAN` |
| Numeric | `TINYINT`, `SMALLINT`, `INT`, `BIGINT`, `DECIMAL`, `FLOAT`, `DOUBLE` |
| Character String | `VARCHAR`, `CHAR` |
| Binary String | `VARBINARY` `BINARY` |
| Date | `DATE` |
| Time | `TIME` |
| Datetime | `TIMESTAMP`, `TIMESTAMP WITH LOCAL TIME ZONE` |
| UUID | `UUID` |
