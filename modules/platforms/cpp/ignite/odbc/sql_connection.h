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

#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/config/connection_info.h"
#include "ignite/odbc/diagnostic/diagnosable_adapter.h"
#include "ignite/odbc/odbc_error.h"

#include "ignite/network/tcp_range.h"

#include <cstdint>
#include <vector>

namespace ignite {

class sql_environment;
class sql_statement;

/**
 * ODBC node connection.
 */
class sql_connection : public diagnosable_adapter {
    friend class sql_environment;
public:
    /**
     * Operation with timeout result.
     */
    enum class operation_result
    {
        SUCCESS,
        FAIL,
        TIMEOUT
    };

    /** Default connection timeout in seconds. */
    enum
    {
        DEFAULT_CONNECT_TIMEOUT = 5
    };

    // Delete
    sql_connection(sql_connection &&) = delete;
    sql_connection(const sql_connection &) = delete;
    sql_connection &operator=(sql_connection &&) = delete;
    sql_connection &operator=(const sql_connection &) = delete;

    /**
     * Get connection info.
     *
     * @return Connection info.
     */
    [[nodiscard]] const connection_info& get_info() const
    {
        return m_info;
    }

    /**
     * Get info of any type.
     *
     * @param type Info type.
     * @param buf Result buffer pointer.
     * @param buffer_len Result buffer length.
     * @param result_len Result value length pointer.
     */
    void get_info(connection_info::info_type type, void* buf, short buffer_len, short* result_len);

    /**
     * Establish connection to ODBC server.
     *
     * @param connectStr Connection string.
     * @param parentWindow Parent window pointer.
     */
    void establish(const std::string& connectStr, void* parentWindow);

    /**
     * Establish connection to ODBC server.
     *
     * @param cfg Configuration.
     */
    void establish(const configuration& cfg);

    /**
     * Release established connection.
     *
     * @return Operation result.
     */
    void release();

    /**
     * Deregister self from the parent.
     */
    void deregister();

    /**
     * Create statement associated with the connection.
     *
     * @return Pointer to valid instance on success and NULL on failure.
     */
    sql_statement * create_statement();

    /**
     * Send data by established connection.
     * Uses connection timeout.
     *
     * @param data Data buffer.
     * @param len Data length.
     * @return @c true on success, @c false on timeout.
     * @throw odbc_error on error.
     */
    bool send(const std::int8_t* data, std::size_t len)
    {
        return send(data, len, m_timeout);
    }

    /**
     * Send data by established connection.
     *
     * @param data Data buffer.
     * @param len Data length.
     * @param timeout Timeout.
     * @return @c true on success, @c false on timeout.
     * @throw odbc_error on error.
     */
    bool send(const std::int8_t* data, std::size_t len, std::int32_t timeout);

    /**
     * Receive next message.
     *
     * @param msg Buffer for message.
     * @param timeout Timeout.
     * @return @c true on success, @c false on timeout.
     * @throw odbc_error on error.
     */
    bool receive(std::vector<std::int8_t>& msg, std::int32_t timeout);

    /**
     * Get configuration.
     *
     * @return Connection configuration.
     */
    [[nodiscard]] const configuration& get_configuration() const;

    /**
     * Is auto commit.
     *
     * @return @c true if the auto commit is enabled.
     */
    [[nodiscard]] bool is_auto_commit() const;

    /**
     * Synchronously send request message and receive response.
     * Uses provided timeout.
     *
     * @param req Request message.
     * @param rsp response message.
     * @param timeout Timeout. 0 means disabled.
     * @return @c true on success, @c false on timeout.
     * @throw odbc_error on error.
     */
    template<typename ReqT, typename RspT>
    bool sync_message(const ReqT& req, RspT& rsp, std::int32_t timeout)
    {
        ensure_connected();

        std::vector<std::int8_t> temp_buffer;

        // TODO: IGNITE-19204 Implement encoding and decoding of messages.
        //parser.Encode(req, temp_buffer);

        bool success = send(temp_buffer.data(), temp_buffer.size(), timeout);

        if (!success)
            return false;

        success = receive(temp_buffer, timeout);

        if (!success)
            return false;

        // TODO: IGNITE-19204 Implement encoding and decoding of messages.
        //parser.Decode(rsp, temp_buffer);

        return true;
    }

    /**
     * Synchronously send request message and receive response.
     * Uses connection timeout.
     *
     * @param req Request message.
     * @param rsp response message.
     * @throw odbc_error on error.
     */
    template<typename ReqT, typename RspT>
    void sync_message(const ReqT& req, RspT& rsp)
    {
        ensure_connected();

        std::vector<std::int8_t> temp_buffer;

        // TODO: IGNITE-19204 Implement encoding and decoding of messages.
        //parser.Encode(req, temp_buffer);

        bool success = send(temp_buffer.data(), temp_buffer.size(), m_timeout);

        if (!success)
            throw odbc_error(sql_state::SHYT01_CONNECTION_TIMEOUT, "Send operation timed out");

        success = receive(temp_buffer, m_timeout);

        if (!success)
            throw odbc_error(sql_state::SHYT01_CONNECTION_TIMEOUT, "Receive operation timed out");

        // TODO: IGNITE-19204 Implement encoding and decoding of messages.
        //parser.Decode(rsp, temp_buffer);
    }

    /**
     * Perform transaction commit.
     */
    void transaction_commit();

    /**
     * Perform transaction rollback.
     */
    void transaction_rollback();

    /**
     * Get connection attribute.
     *
     * @param attr Attribute type.
     * @param buf Buffer for value.
     * @param buf_len Buffer length.
     * @param value_len Resulting value length.
     */
    void get_attribute(int attr, void* buf, SQLINTEGER buf_len, SQLINTEGER *value_len);

    /**
     * Set connection attribute.
     *
     * @param attr Attribute type.
     * @param value Value pointer.
     * @param value_len Value length.
     */
    void set_attribute(int attr, void* value, SQLINTEGER value_len);

private:

    /**
     * Init connection socket, using configuration.
     *
     * @return Operation result.
     */
    sql_result init_socket();

    /**
     * Synchronously send request message and receive response.
     * Uses provided timeout. Does not try to restore connection on
     * fail.
     *
     * @param req Request message.
     * @param rsp response message.
     * @param timeout Timeout.
     * @return @c true on success, @c false on timeout.
     * @throw odbc_error on error.
     */
    template<typename ReqT, typename RspT>
    bool internal_sync_message(const ReqT& req, RspT& rsp, std::int32_t timeout)
    {
        std::vector<std::int8_t> temp_buffer;

        // TODO: IGNITE-19204 Implement encoding and decoding of messages.
        //parser.Encode(req, temp_buffer);

        bool success = send(temp_buffer.data(), temp_buffer.size(), timeout);

        if (!success)
            return false;

        success = receive(temp_buffer, timeout);

        if (!success)
            return false;

        // TODO: IGNITE-19204 Implement encoding and decoding of messages.
        //parser.Decode(rsp, temp_buffer);

        return true;
    }

    /**
     * Establish connection to ODBC server.
     * Internal call.
     *
     * @param connectStr Connection string.
     * @param parentWindow Parent window.
     * @return Operation result.
     */
    sql_result internal_establish(const std::string& connectStr, void* parentWindow);

    /**
     * Establish connection to ODBC server.
     * Internal call.
     *
     * @param cfg Configuration.
     * @return Operation result.
     */
    sql_result internal_establish(const configuration& cfg);

    /**
     * Release established connection.
     * Internal call.
     *
     * @return Operation result.
     */
    sql_result internal_release();

    /**
     * Close connection.
     */
    void close();

    /**
     * Get info of any type.
     * Internal call.
     *
     * @param type Info type.
     * @param buf Result buffer pointer.
     * @param buffer_len Result buffer length.
     * @param result_len Result value length pointer.
     * @return Operation result.
     */
    sql_result internal_get_info(connection_info::info_type type, void* buf, short buffer_len, short* result_len);

    /**
     * Create statement associated with the connection.
     * Internal call.
     *
     * @param statement Pointer to valid instance on success and NULL on failure.
     * @return Operation result.
     */
    sql_result internal_create_statement(sql_statement *& statement);

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
     * Get connection attribute.
     * Internal call.
     *
     * @param attr Attribute type.
     * @param buf Buffer for value.
     * @param buf_len Buffer length.
     * @param value_len Resulting value length.
     * @return Operation result.
     */
    sql_result internal_get_attribute(int attr, void* buf, SQLINTEGER buf_len, SQLINTEGER* value_len);

    /**
     * Set connection attribute.
     * Internal call.
     *
     * @param attr Attribute type.
     * @param value Value pointer.
     * @param value_len Value length.
     * @return Operation result.
     */
    sql_result internal_set_attribute(int attr, void* value, SQLINTEGER value_len);

    /**
     * Receive specified number of bytes.
     *
     * @param dst Buffer for data.
     * @param len Number of bytes to receive.
     * @param timeout Timeout.
     * @return Operation result.
     */
    operation_result receive_all(void* dst, std::size_t len, std::int32_t timeout);

    /**
     * Send specified number of bytes.
     *
     * @param data Data buffer.
     * @param len Data length.
     * @param timeout Timeout.
     * @return Operation result.
     */
    operation_result send_all(const std::int8_t* data, std::size_t len, std::int32_t timeout);

    /**
     * Perform handshake request.
     *
     * @return Operation result.
     */
    sql_result make_request_handshake();

    /**
     * Ensure there is a connection to the cluster.
     *
     * @throw odbc_error on failure.
     */
    void ensure_connected();

    /**
     * Try to restore connection to the cluster.
     *
     * @throw IgniteError on failure.
     * @return @c true on success and @c false otherwise.
     */
    bool try_restore_connection();

    /**
     * Retrieve timeout from parameter.
     *
     * @param value parameter.
     * @return Timeout.
     */
    std::int32_t retrieve_timeout(void* value);

    /**
     * Constructor.
     */
    explicit sql_connection(sql_environment * env)
        : m_env(env)
        , m_config()
        , m_info(m_config) {}

    /** Parent. */
    sql_environment * m_env;

    /** Connection timeout in seconds. */
    std::int32_t m_timeout{0};

    /** Login timeout in seconds. */
    std::int32_t m_login_timeout{DEFAULT_CONNECT_TIMEOUT};

    /** Autocommit flag. */
    bool m_auto_commit{true};

    /** Configuration. */
    configuration m_config;

    /** Connection info. */
    connection_info m_info;
};

} // namespace ignite