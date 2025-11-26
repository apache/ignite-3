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

#include "ignite/odbc/app/application_data_buffer.h"
#include "ignite/odbc/app/parameter_set.h"
#include "ignite/odbc/common_types.h"
#include "ignite/odbc/diagnostic/diagnosable_adapter.h"
#include "ignite/odbc/query/query.h"

#include <cstdint>
#include <map>
#include <memory>

#include "ignite/protocol/sql/column_meta.h"

namespace ignite {
class sql_connection;

/**
 * SQL-statement abstraction. Holds SQL query user buffers data and
 * call result.
 */
class sql_statement : public diagnosable_adapter {
    friend class sql_connection;

public:
    // Delete
    sql_statement(sql_statement &&) = delete;
    sql_statement(const sql_statement &) = delete;
    sql_statement &operator=(sql_statement &&) = delete;
    sql_statement &operator=(const sql_statement &) = delete;

    /**
     * Bind result column to data buffer provided by application
     *
     * @param column_idx Column index.
     * @param target_type Type of target buffer.
     * @param target_value Pointer to target buffer.
     * @param buffer_length Length of target buffer.
     * @param str_length_or_indicator Pointer to the length/indicator buffer.
     */
    void bind_column(uint16_t column_idx, std::int16_t target_type, void *target_value, SQLLEN buffer_length,
        SQLLEN *str_length_or_indicator);

    /**
     * Set column binding offset pointer.
     *
     * @param ptr Column binding offset pointer.
     */
    void set_column_bind_offset_ptr(int *ptr);

    /**
     * Get column binding offset pointer.
     *
     * @return Column binding offset pointer.
     */
    int *get_column_bind_offset_ptr();

    /**
     * Get number of columns in the result set.
     *
     * @return Columns number.
     */
    std::int32_t get_column_number();

    /**
     * Bind parameter.
     *
     * @param param_idx parameter index.
     * @param io_type Type of the parameter (input/output).
     * @param buffer_type The data type of the parameter.
     * @param param_sql_type The SQL data type of the parameter.
     * @param column_size  The size of the column or expression of the corresponding parameter marker.
     * @param dec_digits  The decimal digits of the column or expression of the corresponding parameter marker.
     * @param buffer A pointer to a buffer for the parameter's data.
     * @param buffer_len Length of the ParameterValuePtr buffer in bytes.
     * @param res_len A pointer to a buffer for the parameter's length.
     */
    void bind_parameter(uint16_t param_idx, std::int16_t io_type, std::int16_t buffer_type, std::int16_t param_sql_type,
        SQLULEN column_size, std::int16_t dec_digits, void *buffer, SQLLEN buffer_len, SQLLEN *res_len);

    /**
     * Set statement attribute.
     *
     * @param attr Attribute type.
     * @param value Value pointer.
     * @param value_len Value length.
     */
    void set_attribute(int attr, void *value, SQLINTEGER value_len);

    /**
     * Get statement attribute.
     *
     * @param attr Attribute type.
     * @param buf Buffer for value.
     * @param buf_len Buffer length.
     * @param value_len Resulting value length.
     */
    void get_attribute(int attr, void *buf, SQLINTEGER buf_len, SQLINTEGER *value_len);

    /**
     * Get number m_parameters required by the prepared statement.
     *
     * @param param_num Number of m_parameters.
     */
    void get_parameters_number(uint16_t &param_num);

    /**
     * Set parameter binding offset pointer.
     *
     * @param ptr parameter binding offset pointer.
     */
    void set_param_bind_offset_ptr(int *ptr);

    /**
     * Get value of the column in the result set.
     *
     * @param column_idx Column index.
     * @param buffer Buffer to put column data to.
     */
    void get_column_data(uint16_t column_idx, application_data_buffer &buffer);

    /**
     * Prepare SQL query.
     *
     * @param query SQL query.
     */
    void prepare_sql_query(const std::string &query);

    /**
     * Execute SQL query.
     *
     * @param query SQL query.
     */
    void execute_sql_query(const std::string &query);

    /**
     * Execute SQL query with the custom parameter set.
     *
     * @param query SQL query.
     * @param params Custom parameter set.
     */
    void execute_sql_query(const std::string &query, parameter_set &params);

    /**
     * Execute SQL query.
     */
    void execute_sql_query();

    /**
     * Get columns metadata.
     *
     * @param schema Schema search pattern.
     * @param table Table search pattern.
     * @param column Column search pattern.
     */
    void execute_get_columns_meta_query(const std::string &schema, const std::string &table, const std::string &column);

    /**
     * Get tables metadata.
     *
     * @param catalog Catalog search pattern.
     * @param schema Schema search pattern.
     * @param table Table search pattern.
     * @param table_type Table type search pattern.
     */
    void execute_get_tables_meta_query(
        const std::string &catalog, const std::string &schema, const std::string &table, const std::string &table_type);

    /**
     * Get foreign keys.
     *
     * @param primary_catalog Primary key catalog name.
     * @param primary_schema Primary key schema name.
     * @param primary_table Primary key table name.
     * @param foreign_catalog Foreign key catalog name.
     * @param foreign_schema Foreign key schema name.
     * @param foreign_table Foreign key table name.
     */
    void execute_get_foreign_keys_query(const std::string &primary_catalog, const std::string &primary_schema,
        const std::string &primary_table, const std::string &foreign_catalog, const std::string &foreign_schema,
        const std::string &foreign_table);

    /**
     * Get primary keys.
     *
     * @param catalog Catalog name.
     * @param schema Schema name.
     * @param table Table name.
     */
    void execute_get_primary_keys_query(
        const std::string &catalog, const std::string &schema, const std::string &table);

    /**
     * Get special columns.
     *
     * @param type Special column type.
     * @param catalog Catalog name.
     * @param schema Schema name.
     * @param table Table name.
     * @param scope Minimum required scope of the rowid.
     * @param type Determines whether to return special columns that can have a NULL value.
     */
    void execute_special_columns_query(std::uint16_t type, const std::string &catalog, const std::string &schema,
        const std::string &table, std::uint16_t scope, std::uint16_t nullable);

    /**
     * Get type info.
     *
     * @param sql_type SQL type for which to return info or SQL_ALL_TYPES.
     */
    void execute_get_type_info_query(std::int16_t sql_type);

    /**
     * Free resources
     * @param option indicates what needs to be freed
     */
    void free_resources(std::uint16_t option);

    /**
     * Close statement.
     */
    void close();

    /**
     * Fetch query result row with offset
     * @param orientation Fetch type
     * @param offset Fetch offset
     */
    void fetch_scroll(std::int16_t orientation, std::int64_t offset);

    /**
     * Fetch query result row.
     */
    void fetch_row();

    /**
     * Get column metadata.
     *
     * @return Column metadata.
     */
    const protocol::column_meta_vector *get_meta();

    /**
     * Check if data is available.
     *
     * @return True if data is available.
     */
    [[nodiscard]] bool is_data_available() const;

    /**
     * More results.
     *
     * Move to next result set or affected rows number.
     */
    void more_results();

    /**
     * Get column attribute.
     *
     * @param column_idx Column index.
     * @param attr_id Attribute ID.
     * @param string_buf Buffer for string attribute value.
     * @param buffer_len String buffer size.
     * @param result_len Buffer to put resulting string length to.
     * @param numeric_buf Numeric value buffer.
     */
    void get_column_attribute(uint16_t column_idx, uint16_t attr_id, char *string_buf, std::int16_t buffer_len,
        std::int16_t *result_len, SQLLEN *numeric_buf);

    /**
     * Get number of rows affected by the statement.
     *
     * @return Number of rows affected by the statement.
     */
    [[nodiscard]] std::int64_t affected_rows();

    /**
     * Set rows fetched buffer pointer.
     *
     * @param ptr Rows fetched buffer pointer.
     */
    void set_row_fetched_ptr(SQLINTEGER *ptr);

    /**
     * Get rows fetched buffer pointer.
     *
     * @return Rows fetched buffer pointer.
     */
    SQLINTEGER *get_row_fetched_ptr();

    /**
     * Set row statuses array pointer.
     *
     * @param ptr Row statuses array pointer.
     */
    void set_row_statuses_ptr(SQLUSMALLINT *ptr);

    /**
     * Get row statuses array pointer.
     *
     * @return Row statuses array pointer.
     */
    SQLUSMALLINT *get_row_statuses_ptr();

    /**
     * Select next parameter data for which is required.
     *
     * @param param_ptr Pointer to param id stored here.
     */
    void select_param(void **param_ptr);

    /**
     * Puts data for previously selected parameter or column.
     *
     * @param data Data.
     * @param len Data length.
     */
    void put_data(void *data, SQLLEN len);

    /**
     * Get type info of the parameter of the prepared statement.
     *
     * @param param_num - parameter index.
     * @param data_type - Data type.
     * @param param_size - Size of the parameter.
     * @param decimal_digits - big_decimal digits.
     * @param nullable - Nullability flag.
     */
    void describe_param(std::uint16_t param_num, std::int16_t *data_type, SQLULEN *param_size,
        std::int16_t *decimal_digits, std::int16_t *nullable);

    /**
     * Get a pointer to the current query.
     *
     * @return Current query.
     */
    [[nodiscard]] query *get_query() { return m_current_query.get(); }

private:
    /**
     * Bind result column to specified data buffer.
     *
     * @param column_idx Column index.
     * @param buffer Buffer to put column data to.
     */
    void safe_bind_column(uint16_t column_idx, const application_data_buffer &buffer);

    /**
     * Unbind specified column buffer.
     *
     * @param column_idx Column index.
     */
    void safe_unbind_column(uint16_t column_idx);

    /**
     * Unbind all column buffers.
     */
    void safe_unbind_all_columns();

    /**
     * Bind result column to data buffer provided by application
     *
     * @param column_idx Column index.
     * @param target_type Type of target buffer.
     * @param target_value Pointer to target buffer.
     * @param buffer_length Length of target buffer.
     * @param str_length_or_indicator Pointer to the length/indicator buffer.
     * @return Operation result.
     */
    sql_result internal_bind_column(uint16_t column_idx, std::int16_t target_type, void *target_value,
        SQLLEN buffer_length, SQLLEN *str_length_or_indicator);

    /**
     * Bind parameter.
     *
     * @param param_idx parameter index.
     * @param io_type Type of the parameter (input/output).
     * @param buffer_type The data type of the parameter.
     * @param param_sql_type The SQL data type of the parameter.
     * @param column_size  The size of the column or expression of the corresponding parameter marker.
     * @param dec_digits  The decimal digits of the column or expression of the corresponding parameter marker.
     * @param buffer A pointer to a buffer for the parameter's data.
     * @param buffer_len Length of the ParameterValuePtr buffer in bytes.
     * @param res_len A pointer to a buffer for the parameter's length.
     * @return Operation result.
     */
    sql_result internal_bind_parameter(uint16_t param_idx, std::int16_t io_type, std::int16_t buffer_type,
        std::int16_t param_sql_type, SQLULEN column_size, std::int16_t dec_digits, void *buffer, SQLLEN buffer_len,
        SQLLEN *res_len);

    /**
     * Set statement attribute.
     * Internal call.
     *
     * @param attr Attribute type.
     * @param value Value pointer.
     * @param value_len Value length.
     * @return Operation result.
     */
    sql_result internal_set_attribute(int attr, void *value, SQLINTEGER value_len);

    /**
     * Get statement attribute.
     * Internal call.
     *
     * @param attr Attribute type.
     * @param buf Buffer for value.
     * @param buf_len Buffer length.
     * @param value_len Resulting value length.
     * @return Operation result.
     */
    sql_result internal_get_attribute(int attr, void *buf, SQLINTEGER buf_len, SQLINTEGER *value_len);

    /**
     * Get number m_parameters required by the prepared statement.
     *
     * @param param_num Number of m_parameters.
     */
    sql_result internal_get_parameters_number(uint16_t &param_num);

    /**
     * Get value of the column in the result set.
     *
     * @param column_idx Column index.
     * @param buffer Buffer to put column data to.
     * @return Operation result.
     */
    sql_result internal_get_column_data(uint16_t column_idx, application_data_buffer &buffer);

    /**
     * Free resources
     *
     * @param option indicates what needs to be freed
     * @return Operation result.
     */
    sql_result internal_free_resources(std::uint16_t option);

    /**
     * Close statement.
     * Internal call.
     *
     * @return Operation result.
     */
    sql_result internal_close();

    /**
     * Prepare SQL query.
     *
     * @param query SQL query.
     * @return Operation result.
     */
    sql_result internal_prepare_sql_query(const std::string &query);

    /**
     * Execute SQL query.
     *
     * @param query SQL query.
     * @return Operation result.
     */
    sql_result internal_execute_sql_query(const std::string &query);

    /**
     * Execute SQL query.
     *
     * @param query SQL query.
     * @param params Custom parameter set.
     * @return Operation result.
     */
    sql_result internal_execute_sql_query(const std::string &query, parameter_set &params);

    /**
     * Execute SQL query.
     *
     * @return Operation result.
     */
    sql_result internal_execute_sql_query();

    /**
     * Fetch query result row with offset
     * @param orientation Fetch type
     * @param offset Fetch offset
     * @return Operation result.
     */
    sql_result internal_fetch_scroll(std::int16_t orientation, std::int64_t offset);

    /**
     * Fetch query result row.
     *
     * @return Operation result.
     */
    sql_result internal_fetch_row();

    /**
     * Get number of columns in the result set.
     *
     * @param res Columns number.
     * @return Operation result.
     */
    sql_result internal_get_column_number(std::int32_t &res);

    /**
     * Get columns metadata.
     *
     * @param schema Schema search pattern.
     * @param table Table search pattern.
     * @param column Column search pattern.
     * @return Operation result.
     */
    sql_result internal_execute_get_columns_meta_query(
        const std::string &schema, const std::string &table, const std::string &column);

    /**
     * Get tables metadata.
     *
     * @param catalog Catalog search pattern.
     * @param schema Schema search pattern.
     * @param table Table search pattern.
     * @param table_type Table type search pattern.
     * @return Operation result.
     */
    sql_result internal_execute_get_tables_meta_query(
        const std::string &catalog, const std::string &schema, const std::string &table, const std::string &table_type);

    /**
     * Get foreign keys.
     *
     * @param primary_catalog Primary key catalog name.
     * @param primary_schema Primary key schema name.
     * @param primary_table Primary key table name.
     * @param foreign_catalog Foreign key catalog name.
     * @param foreign_schema Foreign key schema name.
     * @param foreign_table Foreign key table name.
     * @return Operation result.
     */
    sql_result internal_execute_get_foreign_keys_query(const std::string &primary_catalog,
        const std::string &primary_schema, const std::string &primary_table, const std::string &foreign_catalog,
        const std::string &foreign_schema, const std::string &foreign_table);

    /**
     * Get primary keys.
     *
     * @param catalog Catalog name.
     * @param schema Schema name.
     * @param table Table name.
     * @return Operation result.
     */
    sql_result internal_execute_get_primary_keys_query(
        const std::string &catalog, const std::string &schema, const std::string &table);

    /**
     * Get special columns.
     *
     * @param type Special column type.
     * @param catalog Catalog name.
     * @param schema Schema name.
     * @param table Table name.
     * @param scope Minimum required scope of the rowid.
     * @param nullable Determines whether to return special columns that can have a NULL value.
     * @return Operation result.
     */
    sql_result internal_execute_special_columns_query(std::uint16_t type, const std::string &catalog,
        const std::string &schema, const std::string &table, std::uint16_t scope, std::uint16_t nullable);

    /**
     * Get type info.
     *
     * @param sql_type SQL type for which to return info or SQL_ALL_TYPES.
     * @return Operation result.
     */
    sql_result internal_execute_get_type_info_query(std::int16_t sql_type);

    /**
     * Next results.
     *
     * Move to next result set or affected rows number.
     *
     * @return Operation result.
     */
    sql_result internal_more_results();

    /**
     * Get column attribute.
     *
     * @param column_idx Column index.
     * @param attr_id Attribute ID.
     * @param string_buf Buffer for string attribute value.
     * @param buffer_len String buffer size.
     * @param result_len Buffer to put resulting string length to.
     * @param numeric_buf Numeric value buffer.
     * @return Operation result.
     */
    sql_result internal_get_column_attribute(uint16_t column_idx, uint16_t attr_id, char *string_buf,
        std::int16_t buffer_len, std::int16_t *result_len, SQLLEN *numeric_buf);

    /**
     * Get number of rows affected by the statement.
     *
     * @param row_count Number of rows affected by the statement.
     * @return Operation result.
     */
    sql_result internal_affected_rows(std::int64_t &row_count);

    /**
     * Select next parameter data for which is required.
     *
     * @param param_ptr Pointer to param id stored here.
     * @return Operation result.
     */
    sql_result internal_select_param(void **param_ptr);

    /**
     * Puts data for previously selected parameter or column.
     *
     * @param data Data.
     * @param len Data length.
     * @return Operation result.
     */
    sql_result internal_put_data(void *data, SQLLEN len);

    /**
     * Get type info of the parameter of the prepared statement.
     *
     * @param param_num - parameter index.
     * @param data_type - Data type.
     * @param param_size - Size of the parameter.
     * @param decimal_digits - big_decimal digits.
     * @param nullable - Nullability flag.
     * @return Operation result.
     */
    sql_result internal_describe_param(std::uint16_t param_num, std::int16_t *data_type, SQLULEN *param_size,
        std::int16_t *decimal_digits, std::int16_t *nullable);

    /**
     * Convert sql_result to SQL_ROW_RESULT.
     *
     * @return Operation result.
     */
    static uint16_t sql_result_to_row_result(sql_result value);

    /**
     * Constructor.
     * Called by friend classes.
     *
     * @param parent Connection associated with the statement.
     */
    sql_statement(sql_connection &parent)
        : m_connection(parent) {}

    /** Connection associated with the statement. */
    sql_connection &m_connection;

    /** Column bindings. */
    column_binding_map m_column_bindings;

    /** Underlying query. */
    std::unique_ptr<query> m_current_query;

    /** Buffer to store number of rows fetched by the last fetch. */
    SQLINTEGER *m_rows_fetched{nullptr};

    /** Array to store statuses of rows fetched by the last fetch. */
    SQLUSMALLINT *m_row_statuses{nullptr};

    /** Offset added to pointers to change binding of column data. */
    int *m_column_bind_offset{nullptr};

    /** Row array size. */
    SQLULEN m_row_array_size{1};

    /** Parameters. */
    parameter_set_impl m_parameters;

    /** Query timeout in seconds. */
    std::int32_t m_timeout{0};
};

} // namespace ignite