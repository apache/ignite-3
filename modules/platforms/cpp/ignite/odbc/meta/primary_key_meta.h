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

#include "ignite/odbc/utility.h"
#include "ignite/protocol/reader.h"

#include <cstdint>
#include <string>
#include <utility>

namespace ignite {

/**
 * Primary key metadata.
 */
class primary_key_meta {
public:
    // Default
    primary_key_meta() = default;

    /**
     * Constructor.
     *
     * @param catalog Catalog name.
     * @param schema Schema name.
     * @param table Table name.
     * @param column Column name.
     * @param key_seq Column sequence number in key (starting with 1).
     * @param key_name Key name.
     */
    primary_key_meta(std::string catalog, std::string schema, std::string table, std::string column,
        std::int16_t key_seq, std::string key_name)
        : m_catalog(std::move(catalog))
        , m_schema(std::move(schema))
        , m_table(std::move(table))
        , m_column(std::move(column))
        , m_key_seq(key_seq)
        , m_key_name(std::move(key_name)) {}

    /**
     * Get catalog name.
     *
     * @return Catalog name.
     */
    [[nodiscard]] const std::string &get_catalog_name() const { return m_catalog; }

    /**
     * Get schema name.
     *
     * @return Schema name.
     */
    [[nodiscard]] const std::string &get_schema_name() const { return m_schema; }

    /**
     * Get table name.
     *
     * @return Table name.
     */
    [[nodiscard]] const std::string &get_table_name() const { return m_table; }

    /**
     * Get column name.
     *
     * @return Column name.
     */
    [[nodiscard]] const std::string &get_column_name() const { return m_column; }

    /**
     * Get column sequence number in key.
     *
     * @return Sequence number in key.
     */
    [[nodiscard]] std::int16_t get_key_seq() const { return m_key_seq; }

    /**
     * Get key name.
     *
     * @return Key name.
     */
    [[nodiscard]] const std::string &get_key_name() const { return m_key_name; }

private:
    /** Catalog name. */
    std::string m_catalog;

    /** Schema name. */
    std::string m_schema;

    /** Table name. */
    std::string m_table;

    /** Column name. */
    std::string m_column;

    /** Column sequence number in key. */
    std::int16_t m_key_seq{0};

    /** Key name. */
    std::string m_key_name;
};

/** Table metadata vector alias. */
typedef std::vector<primary_key_meta> primary_key_meta_vector;

} // namespace ignite
