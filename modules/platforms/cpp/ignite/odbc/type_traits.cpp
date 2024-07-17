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

#include "ignite/odbc/type_traits.h"
#include "ignite/common/detail/config.h"
#include "ignite/odbc/system/odbc_constants.h"

namespace ignite {

#ifdef _DEBUG

# define DBG_STR_CASE(x)                                                                                               \
  case x:                                                                                                              \
   return #x

const char *statement_attr_id_to_string(long id) {
    switch (id) {
        DBG_STR_CASE(SQL_ATTR_APP_PARAM_DESC);
        DBG_STR_CASE(SQL_ATTR_APP_ROW_DESC);
        DBG_STR_CASE(SQL_ATTR_ASYNC_ENABLE);
        DBG_STR_CASE(SQL_ATTR_CONCURRENCY);
        DBG_STR_CASE(SQL_ATTR_CURSOR_SCROLLABLE);
        DBG_STR_CASE(SQL_ATTR_CURSOR_SENSITIVITY);
        DBG_STR_CASE(SQL_ATTR_CURSOR_TYPE);
        DBG_STR_CASE(SQL_ATTR_ENABLE_AUTO_IPD);
        DBG_STR_CASE(SQL_ATTR_FETCH_BOOKMARK_PTR);
        DBG_STR_CASE(SQL_ATTR_IMP_PARAM_DESC);
        DBG_STR_CASE(SQL_ATTR_IMP_ROW_DESC);
        DBG_STR_CASE(SQL_ATTR_KEYSET_SIZE);
        DBG_STR_CASE(SQL_ATTR_MAX_LENGTH);
        DBG_STR_CASE(SQL_ATTR_MAX_ROWS);
        DBG_STR_CASE(SQL_ATTR_METADATA_ID);
        DBG_STR_CASE(SQL_ATTR_NOSCAN);
        DBG_STR_CASE(SQL_ATTR_PARAM_BIND_OFFSET_PTR);
        DBG_STR_CASE(SQL_ATTR_PARAM_BIND_TYPE);
        DBG_STR_CASE(SQL_ATTR_PARAM_OPERATION_PTR);
        DBG_STR_CASE(SQL_ATTR_PARAM_STATUS_PTR);
        DBG_STR_CASE(SQL_ATTR_PARAMS_PROCESSED_PTR);
        DBG_STR_CASE(SQL_ATTR_PARAMSET_SIZE);
        DBG_STR_CASE(SQL_ATTR_QUERY_TIMEOUT);
        DBG_STR_CASE(SQL_ATTR_RETRIEVE_DATA);
        DBG_STR_CASE(SQL_ATTR_ROW_ARRAY_SIZE);
        DBG_STR_CASE(SQL_ATTR_ROW_BIND_OFFSET_PTR);
        DBG_STR_CASE(SQL_ATTR_ROW_BIND_TYPE);
        DBG_STR_CASE(SQL_ATTR_ROW_NUMBER);
        DBG_STR_CASE(SQL_ATTR_ROW_OPERATION_PTR);
        DBG_STR_CASE(SQL_ATTR_ROW_STATUS_PTR);
        DBG_STR_CASE(SQL_ATTR_ROWS_FETCHED_PTR);
        DBG_STR_CASE(SQL_ATTR_SIMULATE_CURSOR);
        DBG_STR_CASE(SQL_ATTR_USE_BOOKMARKS);
        default:
            break;
    }
    return "<< UNKNOWN ID >>";
}

# undef DBG_STR_CASE
#endif // _DEBUG

/**
 * SQL type name constants.
 */
namespace sql_type_name {

/** BOOLEAN SQL type name constant. */
const inline std::string BOOLEAN("BOOLEAN");

/** TINYINT SQL type name constant. */
const inline std::string TINYINT("TINYINT");

/** SMALLINT SQL type name constant. */
const inline std::string SMALLINT("SMALLINT");

/** INTEGER SQL type name constant. */
const inline std::string INTEGER("INTEGER");

/** BIGINT SQL type name constant. */
const inline std::string BIGINT("BIGINT");

/** FLOAT SQL type name constant. */
const inline std::string REAL("REAL");

/** DOUBLE SQL type name constant. */
const inline std::string DOUBLE("DOUBLE");

/** VARCHAR SQL type name constant. */
const inline std::string VARCHAR("VARCHAR");

/** BINARY SQL type name constant. */
const inline std::string BINARY("VARBINARY");

/** TIME SQL type name constant. */
const inline std::string TIME("TIME");

/** TIMESTAMP SQL type name constant. */
const inline std::string TIMESTAMP("TIMESTAMP");

/** DATE SQL type name constant. */
const inline std::string DATE("DATE");

/** DECIMAL SQL type name constant. */
const inline std::string DECIMAL("DECIMAL");

/** UUID SQL type name constant. */
const inline std::string UUID("UUID");

} // namespace sql_type_name

const std::string &ignite_type_to_sql_type_name(ignite_type typ) {
    switch (typ) {
        case ignite_type::BOOLEAN:
            return sql_type_name::BOOLEAN;

        case ignite_type::INT8:
            return sql_type_name::TINYINT;

        case ignite_type::INT16:
            return sql_type_name::SMALLINT;

        case ignite_type::INT32:
            return sql_type_name::INTEGER;

        case ignite_type::INT64:
            return sql_type_name::BIGINT;

        case ignite_type::FLOAT:
            return sql_type_name::REAL;

        case ignite_type::DOUBLE:
            return sql_type_name::DOUBLE;

        case ignite_type::STRING:
            return sql_type_name::VARCHAR;

        case ignite_type::DECIMAL:
        case ignite_type::NUMBER:
            return sql_type_name::DECIMAL;

        case ignite_type::UUID:
            return sql_type_name::UUID;

        case ignite_type::DATE:
            return sql_type_name::DATE;

        case ignite_type::TIMESTAMP:
        case ignite_type::DATETIME:
            return sql_type_name::TIMESTAMP;

        case ignite_type::TIME:
            return sql_type_name::TIME;

        case ignite_type::BITMASK:
        case ignite_type::BYTE_ARRAY:
        case ignite_type::PERIOD:
        case ignite_type::DURATION:
        default:
            // TODO: IGNITE-19969 implement support for period, duration and big_integer
            break;
    }

    return sql_type_name::BINARY;
}

bool is_sql_type_supported(std::int16_t type) {
    switch (type) {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
        case SQL_SMALLINT:
        case SQL_INTEGER:
        case SQL_FLOAT:
        case SQL_DOUBLE:
        case SQL_BIT:
        case SQL_TINYINT:
        case SQL_BIGINT:
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
        case SQL_GUID:
        case SQL_DECIMAL:
        case SQL_TYPE_DATE:
        case SQL_TYPE_TIMESTAMP:
        case SQL_TYPE_TIME:
            return true;

        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR:
        case SQL_REAL:
        case SQL_NUMERIC:
        case SQL_INTERVAL_MONTH:
        case SQL_INTERVAL_YEAR:
        case SQL_INTERVAL_YEAR_TO_MONTH:
        case SQL_INTERVAL_DAY:
        case SQL_INTERVAL_HOUR:
        case SQL_INTERVAL_MINUTE:
        case SQL_INTERVAL_SECOND:
        case SQL_INTERVAL_DAY_TO_HOUR:
        case SQL_INTERVAL_DAY_TO_MINUTE:
        case SQL_INTERVAL_DAY_TO_SECOND:
        case SQL_INTERVAL_HOUR_TO_MINUTE:
        case SQL_INTERVAL_HOUR_TO_SECOND:
        case SQL_INTERVAL_MINUTE_TO_SECOND:
        default:
            return false;
    }
}

ignite_type sql_type_to_ignite_type(std::int16_t sql_type) {
    switch (sql_type) {
        case SQL_CHAR:
        case SQL_VARCHAR:
        case SQL_LONGVARCHAR:
            return ignite_type::STRING;

        case SQL_TINYINT:
            return ignite_type::INT8;

        case SQL_SMALLINT:
            return ignite_type::INT16;

        case SQL_INTEGER:
            return ignite_type::INT32;

        case SQL_BIGINT:
            return ignite_type::INT64;

        case SQL_FLOAT:
            return ignite_type::FLOAT;

        case SQL_DOUBLE:
            return ignite_type::DOUBLE;

        case SQL_BIT:
            return ignite_type::BOOLEAN;

        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
            return ignite_type::BYTE_ARRAY;

        case SQL_DECIMAL:
            return ignite_type::DECIMAL;

        case SQL_GUID:
            return ignite_type::UUID;

        case SQL_TYPE_DATE:
            return ignite_type::DATE;

        case SQL_TYPE_TIMESTAMP:
            return ignite_type::DATETIME;

        case SQL_TYPE_TIME:
            return ignite_type::TIME;

        default:
            // TODO: IGNITE-19969 implement support for period, duration and big_integer
            break;
    }

    return ignite_type::UNDEFINED;
}

odbc_native_type to_driver_type(std::int16_t type) {
    switch (type) {
        case SQL_C_CHAR:
            return odbc_native_type::AI_CHAR;

        case SQL_C_WCHAR:
            return odbc_native_type::AI_WCHAR;

        case SQL_C_SSHORT:
        case SQL_C_SHORT:
            return odbc_native_type::AI_SIGNED_SHORT;

        case SQL_C_USHORT:
            return odbc_native_type::AI_UNSIGNED_SHORT;

        case SQL_C_SLONG:
        case SQL_C_LONG:
            return odbc_native_type::AI_SIGNED_LONG;

        case SQL_C_ULONG:
            return odbc_native_type::AI_UNSIGNED_LONG;

        case SQL_C_FLOAT:
            return odbc_native_type::AI_FLOAT;

        case SQL_C_DOUBLE:
            return odbc_native_type::AI_DOUBLE;

        case SQL_C_BIT:
            return odbc_native_type::AI_BIT;

        case SQL_C_STINYINT:
        case SQL_C_TINYINT:
            return odbc_native_type::AI_SIGNED_TINYINT;

        case SQL_C_UTINYINT:
            return odbc_native_type::AI_UNSIGNED_TINYINT;

        case SQL_C_SBIGINT:
            return odbc_native_type::AI_SIGNED_BIGINT;

        case SQL_C_UBIGINT:
            return odbc_native_type::AI_UNSIGNED_BIGINT;

        case SQL_C_BINARY:
            return odbc_native_type::AI_BINARY;

        case SQL_C_DATE:
        case SQL_C_TYPE_DATE:
            return odbc_native_type::AI_TDATE;

        case SQL_C_TIME:
        case SQL_C_TYPE_TIME:
            return odbc_native_type::AI_TTIME;

        case SQL_C_TIMESTAMP:
        case SQL_C_TYPE_TIMESTAMP:
            return odbc_native_type::AI_TTIMESTAMP;

        case SQL_C_NUMERIC:
            return odbc_native_type::AI_NUMERIC;

        case SQL_C_GUID:
            return odbc_native_type::AI_GUID;

        case SQL_C_DEFAULT:
            return odbc_native_type::AI_DEFAULT;

        default:
            return odbc_native_type::AI_UNSUPPORTED;
    }
}

std::int16_t ignite_type_to_sql_type(ignite_type typ) {
    switch (typ) {
        case ignite_type::INT8:
            return SQL_TINYINT;

        case ignite_type::INT16:
            return SQL_SMALLINT;

        case ignite_type::INT32:
            return SQL_INTEGER;

        case ignite_type::INT64:
            return SQL_BIGINT;

        case ignite_type::FLOAT:
            return SQL_FLOAT;

        case ignite_type::DOUBLE:
            return SQL_DOUBLE;

        case ignite_type::BOOLEAN:
            return SQL_BIT;

        case ignite_type::DECIMAL:
        case ignite_type::NUMBER:
            return SQL_DECIMAL;

        case ignite_type::STRING:
            return SQL_VARCHAR;

        case ignite_type::UUID:
            return SQL_GUID;

        case ignite_type::DATE:
            return SQL_TYPE_DATE;

        case ignite_type::TIMESTAMP:
        case ignite_type::DATETIME:
            return SQL_TYPE_TIMESTAMP;

        case ignite_type::TIME:
            return SQL_TYPE_TIME;

        case ignite_type::BITMASK:
        case ignite_type::BYTE_ARRAY:
        case ignite_type::PERIOD:
        case ignite_type::DURATION:
        default:
            // TODO: IGNITE-19969 implement support for period, duration and big_integer
            break;
    }

    return SQL_BINARY;
}

std::int16_t ignite_type_nullability(ignite_type typ) {
    UNUSED_VALUE(typ);
    return SQL_NULLABLE;
}

std::int32_t sql_type_display_size(std::int16_t type) {
    switch (type) {
        case SQL_VARCHAR:
        case SQL_CHAR:
        case SQL_WCHAR:
        case SQL_LONGVARBINARY:
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARCHAR:
        case SQL_DECIMAL:
        case SQL_NUMERIC:
            return SQL_NO_TOTAL;

        case SQL_BIT:
            return 1;

        case SQL_TINYINT:
            return 4;

        case SQL_SMALLINT:
            return 6;

        case SQL_INTEGER:
            return 11;

        case SQL_BIGINT:
            return 20;

        case SQL_REAL:
            return 14;

        case SQL_FLOAT:
        case SQL_DOUBLE:
            return 24;

        case SQL_TYPE_DATE:
            return 10;

        case SQL_TYPE_TIME:
            return 8;

        case SQL_TYPE_TIMESTAMP:
            return 19;

        case SQL_GUID:
            return 36;

        default:
            return SQL_NO_TOTAL;
    }
}

std::int32_t ignite_type_display_size(ignite_type typ) {
    std::int16_t sql_type = ignite_type_to_sql_type(typ);
    return sql_type_display_size(sql_type);
}

std::int32_t sql_type_column_size(std::int16_t type) {
    switch (type) {
        case SQL_BIT:
            return 1;

        case SQL_TINYINT:
            return 3;

        case SQL_SMALLINT:
            return 5;

        case SQL_INTEGER:
            return 10;

        case SQL_BIGINT:
            return 19;

        case SQL_REAL:
            return 7;

        case SQL_FLOAT:
        case SQL_DOUBLE:
            return 15;

        case SQL_TYPE_DATE:
            return 10;

        case SQL_TYPE_TIME:
            return 8;

        case SQL_TYPE_TIMESTAMP:
            return 19;

        case SQL_GUID:
            return 36;

        case SQL_VARCHAR:
        case SQL_CHAR:
        case SQL_WCHAR:
        case SQL_LONGVARBINARY:
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARCHAR:
        case SQL_DECIMAL:
        case SQL_NUMERIC:
        default:
            return SQL_NO_TOTAL;
    }
}

std::int32_t ignite_type_max_column_size(ignite_type typ) {
    std::int16_t sql_type = ignite_type_to_sql_type(typ);
    return sql_type_column_size(sql_type);
}

std::int32_t sql_type_transfer_length(std::int16_t type) {
    switch (type) {
        case SQL_VARCHAR:
        case SQL_CHAR:
        case SQL_WCHAR:
        case SQL_LONGVARBINARY:
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARCHAR:
        case SQL_DECIMAL:
        case SQL_NUMERIC:
            return SQL_NO_TOTAL;

        case SQL_BIT:
        case SQL_TINYINT:
            return 1;

        case SQL_SMALLINT:
            return 2;

        case SQL_INTEGER:
        case SQL_REAL:
        case SQL_FLOAT:
            return 4;

        case SQL_BIGINT:
        case SQL_DOUBLE:
            return 8;

        case SQL_TYPE_DATE:
        case SQL_TYPE_TIME:
            return 6;

        case SQL_TYPE_TIMESTAMP:
        case SQL_GUID:
            return 16;

        default:
            return SQL_NO_TOTAL;
    }
}

std::int32_t ignite_type_transfer_length(ignite_type typ) {
    std::int16_t sql_type = ignite_type_to_sql_type(typ);

    return sql_type_transfer_length(sql_type);
}

std::int32_t sql_type_num_precision_radix(std::int16_t type) {
    switch (type) {
        case SQL_REAL:
        case SQL_FLOAT:
        case SQL_DOUBLE:
            return 2;

        case SQL_BIT:
        case SQL_TINYINT:
        case SQL_SMALLINT:
        case SQL_INTEGER:
        case SQL_BIGINT:
            return 10;

        default:
            return 0;
    }
}

std::int32_t ignite_type_num_precision_radix(ignite_type typ) {
    std::int16_t sql_type = ignite_type_to_sql_type(typ);

    return sql_type_num_precision_radix(sql_type);
}

std::int32_t sql_type_decimal_digits(std::int16_t type, std::int32_t scale) {
    switch (type) {
        case SQL_BIT:
        case SQL_TINYINT:
        case SQL_SMALLINT:
        case SQL_INTEGER:
        case SQL_BIGINT:
        case SQL_REAL:
        case SQL_FLOAT:
        case SQL_DOUBLE:
            return sql_type_column_size(type);

        case SQL_TYPE_TIME:
        case SQL_TYPE_TIMESTAMP:
            return 9;

        case SQL_DECIMAL:
        case SQL_NUMERIC:
            if (scale >= 0)
                return scale;
            break;

        case SQL_GUID:
        case SQL_TYPE_DATE:
        case SQL_VARCHAR:
        case SQL_CHAR:
        case SQL_WCHAR:
        case SQL_LONGVARBINARY:
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARCHAR:
        default:
            break;
    }
    return -1;
}

std::int32_t ignite_type_decimal_digits(ignite_type typ, std::int32_t scale) {
    std::int16_t sql_type = ignite_type_to_sql_type(typ);

    return sql_type_decimal_digits(sql_type, scale);
}

bool is_sql_type_unsigned(std::int16_t type) {
    switch (type) {
        case SQL_TINYINT:
        case SQL_SMALLINT:
        case SQL_INTEGER:
        case SQL_BIGINT:
        case SQL_REAL:
        case SQL_FLOAT:
        case SQL_DOUBLE:
            return false;

        default:
            return true;
    }
}

bool is_ignite_type_unsigned(ignite_type typ) {
    std::int16_t sql_type = ignite_type_to_sql_type(typ);

    return is_sql_type_unsigned(sql_type);
}

std::optional<std::string> ignite_type_literal_prefix(ignite_type typ) {
    switch (typ) {
        case ignite_type::STRING:
            return "'";
        case ignite_type::BYTE_ARRAY:
            return "0x";
        case ignite_type::DATE:
            return "DATE '";
        case ignite_type::TIME:
            return "TIME '";
        case ignite_type::TIMESTAMP:
            return "TIMESTAMP '";
        default:
            break;
    }
    return {};
}

std::optional<std::string> ignite_type_literal_suffix(ignite_type typ) {
    switch (typ) {
        case ignite_type::STRING:
        case ignite_type::DATE:
        case ignite_type::TIME:
        case ignite_type::TIMESTAMP:
            return "'";
        default:
            break;
    }
    return {};
}

} // namespace ignite
