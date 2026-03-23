---
title: Supported Operators and Functions
sidebar_label: Operators and Functions
---

{/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/}

:::note
For more information on functions supported by Apache Calcite, see the [product documentation](https://calcite.apache.org/docs/reference.html#operators-and-functions).
:::

## Aggregate Functions

### AVG

```sql
AVG( [ ALL | DISTINCT ] numeric)
```

Returns the average (arithmetic mean) of numeric across all input values. When used, the type of data will be changed in the following way:

| Input type | Result type | Minimum scale |
|---|---|---|
| `DECIMAL`, `BIGINT`, `INTEGER`, `SMALLINT`, `TINYINT` | `DECIMAL` | 16 |
| `DOUBLE`, `REAL` | `DOUBLE` | |

### COUNT

```sql
COUNT( [ ALL | DISTINCT ] value [, value ]*)
```

Returns the number of input rows for which value is not null (wholly not null if value is composite).

### MAX

```sql
MAX( [ ALL | DISTINCT ] value)
```

Returns the maximum value across all input values.

### MIN

```sql
MIN( [ ALL | DISTINCT ] value)
```

Returns the minimum value across all input values.

### SUM

```sql
SUM( [ ALL | DISTINCT ] numeric)
```

Returns the sum of numeric across all input values.

### ANY_VALUE

```sql
ANY_VALUE( [ ALL | DISTINCT ] value)
```

Returns one of the values of value across all input values; this is NOT specified in the SQL standard.

### EVERY

```sql
EVERY(condition)
```

Returns TRUE if all of the values of condition are TRUE.

### SOME

```sql
SOME(condition)
```

Returns TRUE if one or more of the values of condition is TRUE.

### GROUPING

```sql
GROUPING(column_reference [,column_reference])
```

Returns a bit vector of the given grouping expressions.

## JSON Functions

### JSON_TYPE

```sql
JSON_TYPE(jsonValue)
```

Returns a string value indicating the type of jsonValue.

### FORMAT JSON

Indicates that the value is formatted as JSON.

### JSON_VALUE

```sql
JSON_VALUE(jsonValue, path [ RETURNING type ] [ { ERROR | NULL | DEFAULT expr } ON EMPTY ] [ { ERROR | NULL | DEFAULT expr } ON ERROR ] )
```

Extract an SQL scalar from a jsonValue using JSON path expression path.

### JSON_QUERY

```sql
JSON_QUERY(jsonValue, path [ RETURNING type ] [ { WITHOUT [ ARRAY ] | WITH [ CONDITIONAL | UNCONDITIONAL ] [ ARRAY ] } WRAPPER ] [ { ERROR | NULL | EMPTY ARRAY | EMPTY OBJECT } ON EMPTY ] [ { ERROR | NULL | EMPTY ARRAY | EMPTY OBJECT } ON ERROR ] )
```

Extract a JSON object or JSON array from jsonValue using the path JSON path expression.

### JSON_TYPE

```sql
JSON_TYPE(jsonValue)
```

Returns a string value indicating the type of `jsonValue`.

### JSON_EXISTS

```sql
JSON_EXISTS(jsonValue, path [ { TRUE | FALSE | UNKNOWN | ERROR } ON ERROR ] )
```

Whether a jsonValue satisfies a search criterion described using JSON path expression path.

### JSON_DEPTH

```sql
JSON_DEPTH(jsonValue)
```

Returns an integer value indicating the depth of jsonValue.

### JSON_KEYS

```sql
JSON_KEYS(jsonValue [, path ])
```

Returns a string indicating the keys of a JSON jsonValue.

### JSON_PRETTY

```sql
JSON_PRETTY(jsonValue)
```

Returns a pretty-printing of jsonValue.

### JSON_LENGTH

```sql
JSON_LENGTH(jsonValue [, path ])
```

Returns a integer indicating the length of jsonValue.

### JSON_REMOVE

```sql
JSON_REMOVE(jsonValue, path [, path ])
```

Removes data from jsonValue using a series of path expressions and returns the result.

### JSON_STORAGE_SIZE

```sql
JSON_STORAGE_SIZE(jsonValue)
```

Returns the number of bytes used to store the binary representation of jsonValue.

### JSON_OBJECT

```sql
JSON_OBJECT( jsonKeyVal [, jsonKeyVal ]* [ nullBehavior ] )
```

Construct JSON object using a series of key-value pairs.

### JSON_ARRAY

```sql
JSON_ARRAY( [ jsonVal [, jsonVal ]* ] [ nullBehavior ] )
```

Construct a JSON array using a series of values.

### IS JSON VALUE

```sql
jsonValue IS JSON [ VALUE ]
```

Whether jsonValue is a JSON value.

### IS JSON OBJECT

```sql
jsonValue IS JSON OBJECT
```

Whether jsonValue is a JSON object.

### IS JSON ARRAY

```sql
jsonValue IS JSON ARRAY
```

Whether jsonValue is a JSON array.

### IS JSON SCALAR

```sql
jsonValue IS JSON SCALAR
```

Whether jsonValue is a JSON scalar value.

## Regular Expression Functions

### POSIX REGEX CASE INSENSITIVE

```sql
value 1 POSIX REGEX CASE INSENSITIVE value 2
```

Case-sensitive POSIX regular expression.

### POSIX REGEX CASE SENSITIVE

```sql
value 1 POSIX REGEX CASE SENSITIVE value 2
```

Case-sensitive POSIX regular expression.

### REGEXP_REPLACE

```sql
REGEXP_REPLACE(string, regexp, rep [, pos [, occurrence [, matchType]]])
```

Replaces all substrings of string that match regexp with rep at the starting pos in expr (if omitted, the default is 1), occurrence specifies which occurrence of a match to search for (if omitted, the default is 1), matchType specifies how to perform matching

```sql
REGEXP_REPLACE(string, regexp)
```

Replaces all substrings of value that match regexp with an empty string and returns modified value.

## Numeric Functions

### MOD

```sql
MOD(numeric1, numeric2)
```

Returns the remainder (modulus) of numeric1 divided by numeric2. The result is negative only if numeric1 is negative.

### EXP

```sql
EXP(numeric)
```

Returns e raised to the power of numeric.

### POWER

```sql
POWER(numeric1, numeric2)
```

Returns numeric1 raised to the power of numeric2.

### LN

```sql
LN(numeric)
```

Returns the natural logarithm (base e) of numeric.

### LOG10

```sql
LOG10(numeric)
```

Returns the base 10 logarithm of numeric.

### ABS

```sql
ABS(numeric)
```

Returns the absolute value of numeric.

### RAND

```sql
RAND([seed])
```

Generates a random double between 0 and 1 inclusive, optionally initializing the random number generator with seed.

### RAND_INTEGER

```sql
RAND_INTEGER([seed, ] numeric)
```

Generates a random integer between 0 and numeric - 1 inclusive, optionally initializing the random number generator with seed.

### ACOS

```sql
ACOS(numeric)
```

Returns the arc cosine of numeric.

### ASIN

```sql
ASIN(numeric)
```

Returns the arc sine of numeric.

### ATAN

```sql
ATAN(numeric)
```

Returns the arc tangent of numeric.

### ATAN2

```sql
ATAN2(numeric, numeric)
```

Returns the arc tangent of the numeric coordinates.

### SQRT

```sql
SQRT(numeric)
```

Returns the square root of numeric.

### CBRT

```sql
CBRT(numeric)
```

Returns the cube root of numeric.

### COS

```sql
COS(numeric)
```

Returns the cosine of numeric.

### COSH

```sql
COSH(numeric)
```

Returns the hyperbolic cosine of numeric.

### COT

```sql
COT(numeric)
```

Returns the cotangent of numeric.

### DEGREES

```sql
DEGREES(numeric)
```

Converts numeric from radians to degrees.

### RADIANS

```sql
RADIANS(numeric)
```

Converts numeric from degrees to radians.

### ROUND

```sql
ROUND(numeric1 [, integer2])
```

Rounds numeric1 to optionally integer2 (if not specified 0) places right to the decimal point.

### SIGN

```sql
SIGN(numeric)
```

Returns the signum of numeric.

### SIN

```sql
SIN(numeric)
```

Returns the sine of numeric.

### SINH

```sql
SINH(numeric)
```

Returns the hyperbolic sine of numeric.

### TAN

```sql
TAN(numeric)
```

Returns the tangent of numeric.

### TANH

```sql
TANH(numeric)
```

Returns the hyperbolic tangent of numeric.

### TRUNCATE

```sql
TRUNCATE(numeric1 [, integer2])
```

Truncates numeric1 to optionally integer2 (if not specified 0) places right to the decimal point.

### PI

```sql
PI()
```

Returns a value that is closer than any other value to Pi.

## String Functions

### UPPER

```sql
UPPER(string)
```

Returns a character string converted to upper case.

### LOWER

```sql
LOWER(string)
```

Returns a character string converted to lower case.

### INITCAP

```sql
INITCAP(string)
```

Returns string with the first letter of each word converter to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.

### TO_BASE64

```sql
TO_BASE64(string)
```
Converts the string to base-64 encoded form and returns an encoded string

### FROM_BASE64

```sql
FROM_BASE64(string)
```

Returns the decoded result of a base-64 string as a string.

### MD5

```sql
MD5(string)
```

Calculates an MD5 128-bit checksum of string and returns it as a hex string.

### SHA1

```sql
SHA1(string)
```

Calculates a SHA-1 hash value of string and returns it as a hex string.

### SUBSTRING

```sql
SUBSTRING(string FROM integer)
```

Returns a substring of a character string starting at a given point.

```sql
SUBSTRING(string FROM integer FOR integer)
```

Returns a substring of a character string starting at a given point with a given length.

```sql
SUBSTRING(binary FROM integer)
```

Returns a substring of binary starting at a given point.

```sql
SUBSTRING(binary FROM integer FOR integer)

```

Returns a substring of binary starting at a given point with a given length.

### LEFT

```sql
LEFT(string, length)
```

Returns the leftmost length characters from the string.

### RIGHT

```sql
RIGHT(string, length)
```

Returns the rightmost length characters from the string.

### REPLACE

```sql
REPLACE(char, search_string [, replace_string])
```

Replaces search_string with replace_string.

### TRANSLATE

```sql
TRANSLATE(expr, fromString, toString)
```
Returns expr with all occurrences of each character in fromString replaced by its corresponding character in toString. Characters in expr that are not in fromString are not replaced.

### CHR

```sql
CHR(integer)
```

Returns the character whose UTF-8 code is integer.

### CHAR_LENGTH

```sql
CHAR_LENGTH(string)
```

Returns the number of characters in a character string.

### CHARACTER_LENGTH

```sql
CHARACTER_LENGTH(string)
```

Returns the number of characters in a character string.

### ||

```sql
string || string
```

Concatenates two character strings.

### CONCAT

```sql
CONCAT(string, string)
```

Concatenates two strings, returns null only when both string arguments are null, otherwise treats null as empty string.

```sql
CONCAT(string [, string ]*)
```

Concatenates one or more strings, returns null if any of the arguments is null.

```sql
CONCAT(string [, string ]*)
```

Concatenates one or more strings, null is treated as empty string.

### OVERLAY

```sql
OVERLAY(string1 PLACING string2 FROM integer [ FOR integer2 ])
```

Replaces a substring of string1 with string2.

```sql
OVERLAY(binary1 PLACING binary2 FROM integer [ FOR integer2 ])
```

Replaces a substring of binary1 with binary2.

### POSITION

```sql
POSITION(substring IN string)
```

Returns the position of the first occurrence of substring in string.

```sql
POSITION(substring IN string FROM integer)
```

Returns the position of the first occurrence of substring in string starting at a given point (not standard SQL).

```sql
POSITION(binary1 IN binary2)
```

Returns the position of the first occurrence of binary1 in binary2.

```sql
POSITION(binary1 IN binary2 FROM integer)
```

Returns the position of the first occurrence of binary1 in binary2 starting at a given point (not standard SQL).

### ASCII

```sql
ASCII(string)
```

Returns the ASCII code of the first character of string; if the first character is a non-ASCII character, returns its Unicode code point; returns 0 if string is empty.

### REPEAT

```sql
REPEAT(string, integer)
```

Returns a string consisting of string repeated of integer times; returns an empty string if integer is less than 1.

### SPACE

```sql
SPACE(integer)
```
Returns a string with an integer number of spaces; returns an empty string if integer is less than 1.

### STRCMP

```sql
STRCMP(string, string)
```

Returns 0 if both of the strings are same and returns -1 when the first argument is smaller than the second and 1 when the second one is smaller than the first one.

### SOUNDEX

```sql
SOUNDEX(string)
```

* Returns the phonetic representation of string; throws if string is encoded with multi-byte encoding such as UTF-8; or
* Returns the phonetic representation of string; return original string if string is encoded with multi-byte encoding such as UTF-8

### DIFFERENCE

```sql
DIFFERENCE(string, string)
```

Returns a measure of the similarity of two strings, namely the number of character positions that their SOUNDEX values have in common: 4 if the SOUNDEX values are same and 0 if the SOUNDEX values are totally different.

### REVERSE

```sql
REVERSE(string)
```

Returns string with the order of the characters reversed.

### TRIM

```sql
TRIM( { BOTH | LEADING | TRAILING } string1 FROM string2)
```

Removes the longest string containing only the characters in string1 from the start/end/both ends of string1.

### LTRIM

```sql
LTRIM(string)
```

Returns string with all blanks removed from the start.

### RTRIM

```sql
RTRIM(string)
```

Returns string with all blanks removed from the end.

### SUBSTR

```sql
SUBSTR(string, position [, substringLength ])
```

Returns a portion of string, beginning at character position, substringLength characters long. SUBSTR calculates lengths using characters as defined by the input character set.

### LENGTH

```sql
LENGTH(string)
```

Equivalent to CHAR_LENGTH(string).

### OCTET_LENGTH

```sql
OCTET_LENGTH(binary)
```

Returns the number of bytes in binary.

### LIKE

```sql
string1 LIKE string2 [ ESCAPE string3 ]
```

Whether string1 matches pattern string2.

### SIMILAR TO

```sql
string1 SIMILAR TO string2 [ ESCAPE string3 ]
```

Whether string1 matches regular expression string2.

## Date/Time Functions

### EXTRACT

```sql
EXTRACT(timeUnit FROM datetime)
```

Extracts and returns the value of a specified datetime field from a datetime value expression.

### FLOOR

```sql
FLOOR(datetime TO timeUnit)
```

Rounds datetime down to timeUnit.

### CEIL

```sql
CEIL(datetime TO timeUnit)
```

Rounds datetime up to timeUnit.

### TIMESTAMPDIFF

```sql
TIMESTAMPDIFF(timeUnit, datetime, datetime2)
```

Returns the (signed) number of timeUnit intervals between datetime and datetime2. Equivalent to (datetime2 - datetime) timeUnit.

### LAST_DAY

```sql
LAST_DAY(date)
```

Returns the date of the last day of the month in a value of datatype DATE; For example, it returns DATE'2020-02-29' for both DATE'2020-02-10' and TIMESTAMP'2020-02-10 10:10:10'.

### DAYNAME

```sql
DAYNAME(datetime)
```

Returns the name of the day of the week based on the datetime value.

### MONTHNAME

```sql
MONTHNAME(date)
```

Returns the name, in the connection's locale, of the month in datetime; for example, it returns '二月' for both DATE '2020-02-10' and TIMESTAMP '2020-02-10 10:10:10'.

### DAYOFMONTH

```sql
DAYOFMONTH(date)
```

Equivalent to EXTRACT(DAY FROM date). Returns an integer between 1 and 31.

### DAYOFWEEK

```sql
DAYOFWEEK(date)
```

Equivalent to EXTRACT(DOW FROM date). Returns an integer between 1 and 7.

### DAYOFYEAR

```sql
DAYOFYEAR(date)
```

Equivalent to EXTRACT(DOY FROM date). Returns an integer between 1 and 366.

### YEAR

```sql
YEAR(date)
```

Equivalent to EXTRACT(YEAR FROM date). Returns an integer.

### QUARTER

```sql
QUARTER(date)
```

Equivalent to EXTRACT(QUARTER FROM date). Returns an integer between 1 and 4.

### MONTH

```sql
MONTH(date)
```

Equivalent to EXTRACT(MONTH FROM date). Returns an integer between 1 and 12.

### WEEK

```sql
WEEK(date)
```

Equivalent to EXTRACT(WEEK FROM date). Returns an integer between 1 and 53.

### HOUR

```sql
HOUR(date)
```

Equivalent to EXTRACT(HOUR FROM date). Returns an integer between 0 and 23.

### MINUTE

```sql
MINUTE(date)
```

Equivalent to EXTRACT(MINUTE FROM date). Returns an integer between 0 and 59.

### SECOND

```sql
SECOND(date)
```

Equivalent to EXTRACT(SECOND FROM date). Returns an integer between 0 and 59.

### TIMESTAMP_SECONDS

```sql
TIMESTAMP_SECONDS(integer)
```

Returns the TIMESTAMP that is integer seconds after 1970-01-01 00:00:00.

### TIMESTAMP_MILLIS

```sql
TIMESTAMP_MILLIS(integer)
```

Returns the TIMESTAMP that is integer milliseconds after 1970-01-01 00:00:00.

### TIMESTAMP_MICROS

```sql
TIMESTAMP_MICROS(integer)
```

Returns the TIMESTAMP that is integer microseconds after 1970-01-01 00:00:00.

### UNIX_SECONDS

```sql
UNIX_SECONDS(timestamp)
```

Returns the number of seconds since 1970-01-01 00:00:00.

### UNIX_MILLIS

```sql
UNIX_MILLIS(timestamp)
```

Returns the number of milliseconds since 1970-01-01 00:00:00.

### UNIX_MICROS

```sql
UNIX_MICROS(timestamp)
```

Returns the number of microseconds since 1970-01-01 00:00:00.

### UNIX_DATE

```sql
UNIX_DATE(date)
```

Returns the number of days since 1970-01-01

### DATE_FROM_UNIX_DATE

```sql
DATE_FROM_UNIX_DATE(integer)
```

Returns the DATE that is integer days after 1970-01-01.

### DATE

```sql
DATE(timestamp)
```

Extracts the DATE from a timestamp.

```sql
DATE(timestampLtz)
```

Extracts the DATE from timestampLtz (an instant; BigQuery's TIMESTAMP type), assuming UTC.

```sql
DATE(timestampLtz, timeZone)
```

Extracts the DATE from timestampLtz (an instant; BigQuery's TIMESTAMP type) in timeZone.

```sql
DATE(string)
```

Equivalent to CAST(string AS DATE).

```sql
DATE(year, month, day)
```

Returns a DATE value for year, month, and day (all of type INTEGER).

### CURRENT_TIMESTAMP

```sql
CURRENT_TIMESTAMP
```

Returns the current date and time in the session time zone, in a value of datatype TIMESTAMP WITH LOCAL TIME ZONE.

### CURRENT_DATE

```sql
CURRENT_DATE
```

Returns the current date in the session time zone, in a value of datatype DATE.

### LOCALTIME

```sql
LOCALTIME
```

Returns the current date and time in the session time zone in a value of datatype TIME.

```sql
LOCALTIME(precision)
```

Returns the current date and time in the session time zone in a value of datatype TIME, with precision digits of precision.

### LOCALTIMESTAMP

```sql
LOCALTIMESTAMP
```

Returns the current date and time in the session time zone in a value of datatype TIMESTAMP.

```sql
LOCALTIMESTAMP(precision)
```

Returns the current date and time in the session time zone in a value of datatype TIMESTAMP, with precision digits of precision.

## Other Functions

### CAST

```sql
CAST(value AS type)
```

Converts a value to a given type. Casts between integer types truncate towards 0.

### COALESCE

```sql
COALESCE(value, value [, value ]*)
```

Provides a value if the first value is null. For example, COALESCE(NULL, 5) returns 5.

### GREATEST

```sql
GREATEST(expr [, expr ]*)
```

Returns the greatest of the expressions.

### NULLIF

```sql
NULLIF(value, value)
```

Returns NULL if the values are the same. For example, NULLIF(5, 5) returns NULL; NULLIF(5, 0) returns 5.

### NVL

```sql
NVL(value1, value2)
```

Returns value1 if value1 is not null, otherwise value2.

### CASE

```sql
CASE value
WHEN value1 [, value11 ]* THEN result1
[ WHEN valueN [, valueN1 ]* THEN resultN ]*
[ ELSE resultZ ]
END
```

Simple case.

```sql
CASE
WHEN condition1 THEN result1
[ WHEN conditionN THEN resultN ]*
[ ELSE resultZ ]
END
```

Searched case.

### DECODE

```sql
DECODE(value, value1, result1 [, valueN, resultN ]* [, default ])
```

Compares value to each valueN value one by one; if value is equal to a valueN, returns the corresponding resultN, else returns default, or NULL if default is not specified.

### LEAST

```sql
LEAST(expr [, expr ]* )
```

Returns the least of the expressions.

### COMPRESS

```sql
COMPRESS(string)
```

Compresses a string using zlib compression and returns the result as a binary string.

### TYPEOF

```sql
TYPEOF value
```

Returns the type of the specified value.

### RAND_UUID

```sql
RAND_UUID
```

Generates a random UUID.

### SYSTEM_RANGE

```sql
SYSTEM_RANGE(start, end[, increment])
```

Returns a range from the table, with an optional increment.

## Security Functions

### CURRENT_USER

```sql
CURRENT_USER
```

Returns the name of the current database user. When security is disabled, returns the system user name instead.
