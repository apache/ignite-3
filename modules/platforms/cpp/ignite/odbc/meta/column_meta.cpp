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

#include "ignite/common/utils.h"

#include "ignite/odbc/log.h"
#include "ignite/odbc/meta/column_meta.h"
#include "ignite/odbc/string_utils.h"
#include "ignite/odbc/system/odbc_constants.h"
#include "ignite/odbc/type_traits.h"

#include <cstring>

namespace ignite {

#define DBG_STR_CASE(x)                                                                                                \
 case x:                                                                                                               \
  return #x

const char *column_meta::attr_id_to_string(uint16_t id) {
    switch (id) {
        DBG_STR_CASE(SQL_DESC_LABEL);
        DBG_STR_CASE(SQL_DESC_BASE_COLUMN_NAME);
        DBG_STR_CASE(SQL_DESC_NAME);
        DBG_STR_CASE(SQL_DESC_TABLE_NAME);
        DBG_STR_CASE(SQL_DESC_BASE_TABLE_NAME);
        DBG_STR_CASE(SQL_DESC_SCHEMA_NAME);
        DBG_STR_CASE(SQL_DESC_CATALOG_NAME);
        DBG_STR_CASE(SQL_DESC_LITERAL_PREFIX);
        DBG_STR_CASE(SQL_DESC_LITERAL_SUFFIX);
        DBG_STR_CASE(SQL_DESC_TYPE_NAME);
        DBG_STR_CASE(SQL_DESC_LOCAL_TYPE_NAME);
        DBG_STR_CASE(SQL_DESC_FIXED_PREC_SCALE);
        DBG_STR_CASE(SQL_DESC_AUTO_UNIQUE_VALUE);
        DBG_STR_CASE(SQL_DESC_CASE_SENSITIVE);
        DBG_STR_CASE(SQL_DESC_CONCISE_TYPE);
        DBG_STR_CASE(SQL_DESC_TYPE);
        DBG_STR_CASE(SQL_DESC_DISPLAY_SIZE);
        DBG_STR_CASE(SQL_DESC_LENGTH);
        DBG_STR_CASE(SQL_DESC_OCTET_LENGTH);
        DBG_STR_CASE(SQL_DESC_NULLABLE);
        DBG_STR_CASE(SQL_DESC_NUM_PREC_RADIX);
        DBG_STR_CASE(SQL_DESC_PRECISION);
        DBG_STR_CASE(SQL_DESC_SCALE);
        DBG_STR_CASE(SQL_DESC_SEARCHABLE);
        DBG_STR_CASE(SQL_DESC_UNNAMED);
        DBG_STR_CASE(SQL_DESC_UNSIGNED);
        DBG_STR_CASE(SQL_DESC_UPDATABLE);
        DBG_STR_CASE(SQL_COLUMN_LENGTH);
        DBG_STR_CASE(SQL_COLUMN_PRECISION);
        DBG_STR_CASE(SQL_COLUMN_SCALE);
        default:
            break;
    }
    return "<< UNKNOWN ID >>";
}

#undef DBG_STR_CASE

nullability nullability_from_int(std::int8_t int_value) {
    switch (int_value) {
        case std::int8_t(nullability::NO_NULL):
            return nullability::NO_NULL;
        case std::int8_t(nullability::NULLABLE):
            return nullability::NULLABLE;
        default:
            return nullability::NULLABILITY_UNKNOWN;
    }
}

SQLLEN nullability_to_sql(nullability value) {
    switch (value) {
        case nullability::NO_NULL:
            return SQL_NO_NULLS;

        case nullability::NULLABLE:
            return SQL_NULLABLE;

        case nullability::NULLABILITY_UNKNOWN:
            return SQL_NULLABLE_UNKNOWN;

        default:
            break;
    }

    assert(false);
    return SQL_NULLABLE_UNKNOWN;
}

bool column_meta::get_attribute(uint16_t field_id, std::string &value) const {
    switch (field_id) {
        case SQL_DESC_LABEL:
        case SQL_DESC_BASE_COLUMN_NAME:
        case SQL_DESC_NAME: {
            value = m_column_name;

            return true;
        }

        case SQL_DESC_TABLE_NAME:
        case SQL_DESC_BASE_TABLE_NAME: {
            value = m_table_name;

            return true;
        }

        case SQL_DESC_SCHEMA_NAME: {
            value = m_schema_name;

            return true;
        }

        case SQL_DESC_CATALOG_NAME: {
            value.clear();

            return true;
        }

        case SQL_DESC_LITERAL_PREFIX:
        case SQL_DESC_LITERAL_SUFFIX: {
            if (m_data_type == ignite_type::STRING)
                value = "'";
            else
                value.clear();

            return true;
        }

        case SQL_DESC_TYPE_NAME:
        case SQL_DESC_LOCAL_TYPE_NAME: {
            value = ignite_type_to_sql_type_name(m_data_type);

            return true;
        }

        case SQL_DESC_PRECISION:
        case SQL_COLUMN_LENGTH:
        case SQL_COLUMN_PRECISION: {
            if (m_precision == -1)
                return false;

            value = lexical_cast<std::string>(m_precision);

            return true;
        }

        case SQL_DESC_SCALE:
        case SQL_COLUMN_SCALE: {
            if (m_scale == -1)
                return false;

            value = lexical_cast<std::string>(m_scale);

            return true;
        }

        default:
            return false;
    }
}

bool column_meta::get_attribute(uint16_t field_id, SQLLEN &value) const {
    switch (field_id) {
        case SQL_DESC_FIXED_PREC_SCALE: {
            if (m_scale == -1)
                value = SQL_FALSE;
            else
                value = SQL_TRUE;

            break;
        }

        case SQL_DESC_AUTO_UNIQUE_VALUE: {
            value = SQL_FALSE;

            break;
        }

        case SQL_DESC_CASE_SENSITIVE: {
            if (m_data_type == ignite_type::STRING)
                value = SQL_TRUE;
            else
                value = SQL_FALSE;

            break;
        }

        case SQL_DESC_CONCISE_TYPE:
        case SQL_DESC_TYPE: {
            value = ignite_type_to_sql_type(m_data_type);

            break;
        }

        case SQL_DESC_DISPLAY_SIZE: {
            value = ignite_type_display_size(m_data_type);

            break;
        }

        case SQL_DESC_LENGTH:
        case SQL_DESC_OCTET_LENGTH:
        case SQL_COLUMN_LENGTH: {
            if (m_precision == -1)
                value = ignite_type_transfer_length(m_data_type);
            else
                value = m_precision;

            break;
        }

        case SQL_COLUMN_NULLABLE:
        case SQL_DESC_NULLABLE: {
            value = nullability_to_sql(m_nullability);

            break;
        }

        case SQL_DESC_NUM_PREC_RADIX: {
            value = ignite_type_num_precision_radix(m_data_type);

            break;
        }

        case SQL_DESC_PRECISION:
        case SQL_COLUMN_PRECISION: {
            value = m_precision < 0 ? 0 : m_precision;

            break;
        }

        case SQL_DESC_SCALE:
        case SQL_COLUMN_SCALE: {
            value = m_scale < 0 ? 0 : m_scale;

            break;
        }

        case SQL_DESC_SEARCHABLE: {
            value = SQL_PRED_BASIC;

            break;
        }

        case SQL_DESC_UNNAMED: {
            value = m_column_name.empty() ? SQL_UNNAMED : SQL_NAMED;

            break;
        }

        case SQL_DESC_UNSIGNED: {
            value = is_ignite_type_unsigned(m_data_type) ? SQL_TRUE : SQL_FALSE;

            break;
        }

        case SQL_DESC_UPDATABLE: {
            value = SQL_ATTR_READWRITE_UNKNOWN;

            break;
        }

        default:
            return false;
    }

    LOG_MSG("value: " << value);

    return true;
}

column_meta_vector read_result_set_meta(protocol::reader &reader) {
    auto size = reader.read_int32();

    column_meta_vector columns;
    columns.reserve(size);

    for (std::int32_t column_idx = 0; column_idx < size; ++column_idx) {
        auto fields_cnt = reader.read_int32();

        assert(fields_cnt >= 6); // Expect at least six fields.

        auto name = reader.read_string();
        auto nullable = reader.read_bool();
        auto typ = ignite_type(reader.read_int32());
        auto scale = reader.read_int32();
        auto precision = reader.read_int32();

        bool origin_present = reader.read_bool();

        if (!origin_present) {
            columns.emplace_back("", "", std::move(name), typ, precision, scale, nullable);
            continue;
        }

        assert(fields_cnt >= 9); // Expect at least three more fields.
        auto origin_name = reader.read_string_nullable();
        auto origin_schema_id = reader.try_read_int32();
        std::string origin_schema;
        if (origin_schema_id) {
            if (*origin_schema_id >= std::int32_t(columns.size())) {
                throw ignite_error("Origin schema ID is too large: " + std::to_string(*origin_schema_id)
                    + ", id=" + std::to_string(column_idx));
            }
            origin_schema = columns[*origin_schema_id].get_schema_name();
        } else {
            origin_schema = reader.read_string();
        }

        auto origin_table_id = reader.try_read_int32();
        std::string origin_table;
        if (origin_table_id) {
            if (*origin_table_id >= std::int32_t(columns.size())) {
                throw ignite_error("Origin table ID is too large: " + std::to_string(*origin_table_id)
                    + ", id=" + std::to_string(column_idx));
            }
            origin_table = columns[*origin_table_id].get_table_name();
        } else {
            origin_table = reader.read_string();
        }

        columns.emplace_back(
            std::move(origin_schema), std::move(origin_table), std::move(name), typ, precision, scale, nullable);
    }

    return columns;
}

} // namespace ignite
