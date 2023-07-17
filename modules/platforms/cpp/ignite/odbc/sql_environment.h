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

#include "ignite/odbc/diagnostic/diagnosable_adapter.h"

#include <set>

namespace ignite {
class sql_connection;

/**
 * ODBC environment.
 */
class sql_environment : public diagnosable_adapter {
public:
    /** Connection set type. */
    typedef std::set<sql_connection *> connection_set;

    // Delete
    sql_environment(sql_environment &&) = delete;
    sql_environment(const sql_environment &) = delete;
    sql_environment &operator=(sql_environment &&) = delete;
    sql_environment &operator=(const sql_environment &) = delete;

    // Default
    sql_environment() = default;

    /**
     * Create connection associated with the environment.
     *
     * @return Pointer to valid instance on success or NULL on failure.
     */
    sql_connection *create_connection();

    /**
     * Deregister connection.
     *
     * @param conn Connection to deregister.
     */
    void deregister_connection(sql_connection *conn);

    /**
     * Perform transaction commit on all the associated connections.
     */
    void transaction_commit();

    /**
     * Perform transaction rollback on all the associated connections.
     */
    void transaction_rollback();

    /**
     * Set attribute.
     *
     * @param attr Attribute to set.
     * @param value Value.
     * @param len Value length if the attribute is of string type.
     */
    void set_attribute(int32_t attr, void *value, int32_t len);

    /**
     * Get attribute.
     *
     * @param attr Attribute to set.
     * @param buffer Buffer to put value to.
     */
    void get_attribute(int32_t attr, application_data_buffer &buffer);

private:
    /**
     * Create connection associated with the environment.
     * Internal call.
     *
     * @return Pointer to valid instance on success or NULL on failure.
     * @return Operation result.
     */
    sql_result internal_create_connection(sql_connection *&connection);

    /**
     * Perform transaction commit on all the associated connections.
     * Internal call.
     *
     * @return Operation result.
     */
    sql_result internal_transaction_commit();

    /**
     * Perform transaction rollback on all the associated connections.
     * Internal call.
     *
     * @return Operation result.
     */
    sql_result internal_transaction_rollback();

    /**
     * Set attribute.
     * Internal call.
     *
     * @param attr Attribute to set.
     * @param value Value.
     * @param len Value length if the attribute is of string type.
     * @return Operation result.
     */
    sql_result internal_set_attribute(int32_t attr, void *value, int32_t len);

    /**
     * Get attribute.
     * Internal call.
     *
     * @param attr Attribute to set.
     * @param buffer Buffer to put value to.
     * @return Operation result.
     */
    sql_result internal_get_attribute(int32_t attr, application_data_buffer &buffer);

    /** Associated connections. */
    connection_set m_connections;

    /** ODBC version. */
    int32_t m_odbc_version{SQL_OV_ODBC3_80};

    /** ODBC null-termination of string behaviour. */
    int32_t m_odbc_nts{SQL_TRUE};
};

} // namespace ignite