/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <ignite/common/ignite_type.h>

#include <cstdint>
#include <optional>
#include <string>

namespace ignite {

#ifdef _DEBUG

/**
 * Convert statement attribute ID to string containing its name.
 * Debug function.
 *
 * @param id Attribute ID.
 * @return Null-terminated string containing attribute name.
 */
const char *statement_attr_id_to_string(long id);

#endif //_DEBUG

/**
 * ODBC type aliases.
 *
 * We use these so we will not be needed to include system-specific
 * headers in our header files.
 */
enum class odbc_native_type {
    /** Alias for the SQL_C_CHAR type. */
    AI_CHAR,

    /** Alias for the SQL_C_WCHAR type. */
    AI_WCHAR,

    /** Alias for the SQL_C_SSHORT type. */
    AI_SIGNED_SHORT,

    /** Alias for the SQL_C_USHORT type. */
    AI_UNSIGNED_SHORT,

    /** Alias for the SQL_C_SLONG type. */
    AI_SIGNED_LONG,

    /** Alias for the SQL_C_ULONG type. */
    AI_UNSIGNED_LONG,

    /** Alias for the SQL_C_FLOAT type. */
    AI_FLOAT,

    /** Alias for the SQL_C_DOUBLE type. */
    AI_DOUBLE,

    /** Alias for the SQL_C_BIT type. */
    AI_BIT,

    /** Alias for the SQL_C_STINYINT type. */
    AI_SIGNED_TINYINT,

    /** Alias for the SQL_C_UTINYINT type. */
    AI_UNSIGNED_TINYINT,

    /** Alias for the SQL_C_SBIGINT type. */
    AI_SIGNED_BIGINT,

    /** Alias for the SQL_C_UBIGINT type. */
    AI_UNSIGNED_BIGINT,

    /** Alias for the SQL_C_BINARY type. */
    AI_BINARY,

    /** Alias for the SQL_C_TDATE type. */
    AI_TDATE,

    /** Alias for the SQL_C_TTIME type. */
    AI_TTIME,

    /** Alias for the SQL_C_TTIMESTAMP type. */
    AI_TTIMESTAMP,

    /** Alias for the SQL_C_NUMERIC type. */
    AI_NUMERIC,

    /** Alias for the SQL_C_GUID type. */
    AI_GUID,

    /** Alias for the SQL_DEFAULT. */
    AI_DEFAULT,

    /** Alias for all unsupported types. */
    AI_UNSUPPORTED
};

/**
 * Get SQL type name for the ignite type.
 *
 * @param typ Ignite type.
 * @return Corresponding SQL type name.
 */
const std::string &ignite_type_to_sql_type_name(ignite_type typ);

/**
 * Check if the SQL type supported by the current implementation.
 *
 * @param type Application type.
 * @return True if the type is supported.
 */
bool is_sql_type_supported(std::int16_t type);

/**
 * Get corresponding binary type for ODBC SQL type.
 *
 * @param sql_type SQL type.
 * @return Binary type.
 */
ignite_type sql_type_to_ignite_type(std::int16_t sql_type);

/**
 * Convert ODBC type to driver type alias.
 *
 * @param type ODBC type;
 * @return Internal driver type.
 */
odbc_native_type to_driver_type(std::int16_t type);

/**
 * Convert Ignite data type to SQL data type.
 *
 * @param typ Data type.
 * @return SQL data type.
 */
std::int16_t ignite_type_to_sql_type(ignite_type typ);

/**
 * Get Ignite type SQL nullability.
 *
 * @param type Ignite data type.
 * @return SQL_NO_NULLS if the column could not include NULL values.
 *         SQL_NULLABLE if the column accepts NULL values.
 *         SQL_NULLABLE_UNKNOWN if it is not known whether the
 *         column accepts NULL values.
 */
std::int16_t ignite_type_nullability(ignite_type typ);

/**
 * Get SQL type display size.
 *
 * @param type SQL type.
 * @return Display size.
 */
std::int32_t sql_type_display_size(std::int16_t type);

/**
 * Get Ignite type display size.
 *
 * @param typ Ignite type.
 * @return Display size.
 */
std::int32_t ignite_type_display_size(ignite_type typ);

/**
 * Get SQL type column size.
 *
 * @param type SQL type.
 * @return Column size.
 */
std::int32_t sql_type_column_size(std::int16_t type);

/**
 * Get Ignite type column size.
 *
 * @param typ Ignite type.
 * @return Column size.
 */
std::int32_t ignite_type_max_column_size(ignite_type typ);

/**
 * Get SQL type transfer octet length.
 *
 * @param type SQL type.
 * @return Transfer octet length.
 */
std::int32_t sql_type_transfer_length(std::int16_t type);

/**
 * Get Ignite type transfer octet length.
 *
 * @param typ Ignite type.
 * @return Transfer octet length.
 */
std::int32_t ignite_type_transfer_length(ignite_type typ);

/**
 * Get SQL type numeric precision radix.
 *
 * @param type SQL type.
 * @return Numeric precision radix.
 */
std::int32_t sql_type_num_precision_radix(int8_t type);

/**
 * Get Ignite type numeric precision radix.
 *
 * @param typ Ignite type.
 * @return Numeric precision radix.
 */
std::int32_t ignite_type_num_precision_radix(ignite_type typ);

/**
 * Get SQL type decimal digits.
 *
 * @param type SQL type.
 * @param scale Scale if applies. Negative value means scale is not available.
 * @return big_decimal digits.
 */
std::int32_t sql_type_decimal_digits(std::int16_t type, std::int32_t scale);

/**
 * Get Ignite type decimal digits.
 *
 * @param typ Ignite type.
 * @param scale Scale if applies. Negative value means scale is not available.
 * @return big_decimal digits.
 */
std::int32_t ignite_type_decimal_digits(ignite_type typ, std::int32_t scale);

/**
 * Checks if the SQL type is unsigned.
 *
 * @param type SQL type.
 * @return True if unsigned or non-numeric.
 */
bool is_sql_type_unsigned(std::int16_t type);

/**
 * Checks if the Ignite type is unsigned.
 *
 * @param typ Ignite type.
 * @return True if unsigned or non-numeric.
 */
bool is_ignite_type_unsigned(ignite_type typ);

/**
 * Get literal prefix for an ignite type.
 *
 * @param typ Type.
 * @return Prefix.
 */
std::optional<std::string> ignite_type_literal_prefix(ignite_type typ);

/**
 * Get literal suffix for an ignite type.
 *
 * @param typ Type.
 * @return Suffix.
 */
std::optional<std::string> ignite_type_literal_suffix(ignite_type typ);

} // namespace ignite
