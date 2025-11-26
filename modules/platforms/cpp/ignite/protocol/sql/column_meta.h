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

#include "ignite/common/ignite_type.h"
#include "ignite/protocol/reader.h"

#include <cstdint>
#include <string>
#include <utility>

namespace ignite::protocol {

/**
 * Nullability type.
 */
enum class nullability {
    NO_NULL = 0,

    NULLABLE = 1,

    NULLABILITY_UNKNOWN = 2
};

/**
 * Nullability from int value.
 *
 * @param int_value Int value.
 * @return Nullability.
 */
[[nodiscard]] nullability nullability_from_int(std::int8_t int_value);

/**
 * Column metadata.
 */
class column_meta {
public:
    /**
     * Convert attribute ID to string containing its name.
     * Debug function.
     * @param id Attribute ID.
     * @return Null-terminated string containing attribute name.
     */
    static const char *attr_id_to_string(std::uint16_t id);

    // Default
    column_meta() = default;

    /**
     * Constructor.
     *
     * @param schema_name Schema name.
     * @param table_name Table name.
     * @param column_name Column name.
     * @param data_type Data type.
     * @param precision Precision.
     * @param scale Scale.
     * @param nullable Nullable.
     */
    column_meta(std::string schema_name, std::string table_name, std::string column_name, ignite_type data_type,
        std::int32_t precision, std::int32_t scale, bool nullable)
        : m_schema_name(std::move(schema_name))
        , m_table_name(std::move(table_name))
        , m_column_name(std::move(column_name))
        , m_data_type(data_type)
        , m_precision(precision)
        , m_scale(scale)
        , m_nullability(nullable ? nullability::NULLABLE : nullability::NO_NULL) {}

    /**
     * Constructor.
     *
     * @param schema_name Schema name.
     * @param table_name Table name.
     * @param column_name Column name.
     * @param data_type Data type.
     */
    column_meta(std::string schema_name, std::string table_name, std::string column_name, ignite_type data_type)
        : m_schema_name(std::move(schema_name))
        , m_table_name(std::move(table_name))
        , m_column_name(std::move(column_name))
        , m_data_type(data_type) {}

    /**
     * Get schema name.
     *
     * @return Schema name.
     */
    [[nodiscard]] const std::string &get_schema_name() const { return m_schema_name; }

    /**
     * Get table name.
     * @return Table name.
     */
    [[nodiscard]] const std::string &get_table_name() const { return m_table_name; }

    /**
     * Get column name.
     * @return Column name.
     */
    [[nodiscard]] const std::string &get_column_name() const { return m_column_name; }

    /**
     * Get data type.
     * @return Data type.
     */
    [[nodiscard]] ignite_type get_data_type() const { return m_data_type; }

    /**
     * Get column precision.
     * @return Column precision.
     */
    [[nodiscard]] std::int32_t get_precision() const { return m_precision; }

    /**
     * Get column scale.
     * @return Column scale.
     */
    [[nodiscard]] std::int32_t get_scale() const { return m_scale; }

    /**
     * Get column nullability.
     * @return Column nullability.
     */
    [[nodiscard]] nullability get_nullability() const { return m_nullability; }

private:
    /** Schema name. */
    std::string m_schema_name;

    /** Table name. */
    std::string m_table_name;

    /** Column name. */
    std::string m_column_name;

    /** Data type. */
    ignite_type m_data_type{ignite_type::UNDEFINED};

    /** Column precision. */
    std::int32_t m_precision{-1};

    /** Column scale. */
    std::int32_t m_scale{-1};

    /** Column nullability. */
    nullability m_nullability{nullability::NULLABILITY_UNKNOWN};
};

/** Column metadata vector alias. */
typedef std::vector<column_meta> column_meta_vector;

/**
 * Reads result set metadata.
 *
 * @param reader Reader.
 * @return Result set meta columns.
 */
column_meta_vector read_result_set_meta(protocol::reader &reader);

} // namespace ignite
