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
#include "ignite/protocol/protocol_version.h"

#include "ignite/network/data_buffer.h"
#include "ignite/network/socket_client.h"
#include "ignite/network/tcp_range.h"
#include "ignite/protocol/buffer_adapter.h"
#include "ignite/protocol/client_operation.h"
#include "ignite/protocol/writer.h"

#include <atomic>
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
    enum class operation_result { SUCCESS, FAIL, TIMEOUT };

    /** Default connection timeout in seconds. */
    enum { DEFAULT_CONNECT_TIMEOUT = 5 };

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
    [[nodiscard]] const connection_info &get_info() const { return m_info; }

    /**
     * Get info of any type.
     *
     * @param type Info type.
     * @param buf Result buffer pointer.
     * @param buffer_len Result buffer length.
     * @param result_len Result value length pointer.
     */
    void get_info(connection_info::info_type type, void *buf, short buffer_len, short *result_len);

    /**
     * Establish connection to ODBC server.
     *
     * @param connectStr Connection string.
     * @param parentWindow Parent window pointer.
     */
    void establish(const std::string &connectStr, void *parentWindow);

    /**
     * Establish connection to ODBC server.
     *
     * @param cfg Configuration.
     */
    void establish(const configuration &cfg);

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
    sql_statement *create_statement();

    /**
     * Send data by established connection.
     * Uses connection timeout.
     *
     * @param data Data buffer.
     * @param len Data length.
     * @return @c true on success, @c false on timeout.
     * @throw odbc_error on error.
     */
    bool send(const std::byte *data, std::size_t len) { return send(data, len, m_timeout); }

    /**
     * Send data by established connection.
     *
     * @param data Data buffer.
     * @param len Data length.
     * @param timeout Timeout.
     * @return @c true on success, @c false on timeout.
     * @throw odbc_error on error.
     */
    bool send(const std::byte *data, std::size_t len, std::int32_t timeout);

    /**
     * Receive next message.
     *
     * @param msg Buffer for message.
     * @param timeout Timeout.
     * @return @c true on success, @c false on timeout.
     * @throw odbc_error on error.
     */
    bool receive(std::vector<std::byte> &msg, std::int32_t timeout);

    /**
     * Receive next message.
     *
     * @param msg Buffer for message.
     * @param timeout Timeout.
     * @return @c true on success, @c false on timeout.
     */
    bool receive_and_check_magic(std::vector<std::byte> &msg, std::int32_t timeout);

    /**
     * Synchronously send request message.
     * Uses provided timeout.
     *
     * @param req Request message.
     * @param timeout Timeout. 0 means disabled.
     * @throw OdbcError on error.
     */
    void send_message(bytes_view req, std::int32_t timeout);

    /**
     * Synchronously send request message.
     *
     * @param req Request message.
     * @throw OdbcError on error.
     */
    void send_message(bytes_view req) { send_message(req, m_timeout); }

    /**
     * Receive message.
     *
     * @param id Expected message ID.
     * @param timeout Timeout.
     * @return Message.
     */
    network::data_buffer_owning receive_message(std::int64_t id, std::int32_t timeout);

    /**
     * Receive message.
     *
     * @param id Expected message ID.
     * @param timeout Timeout.
     * @return Message and error.
     */
    std::pair<network::data_buffer_owning, std::optional<odbc_error>> receive_message_nothrow(std::int64_t id,
        std::int32_t timeout);

    /**
     * Receive message.
     *
     * @param id Expected message ID.
     * @return Message.
     */
    network::data_buffer_owning receive_message(std::int64_t id) { return receive_message(id, m_timeout); }

    /**
     * Receive message.
     *
     * @param id Expected message ID.
     * @return Message and error.
     */
    std::pair<network::data_buffer_owning, std::optional<odbc_error>> receive_message_nothrow(std::int64_t id) {
        return receive_message_nothrow(id, m_timeout);
    }

    /**
     * Get configuration.
     *
     * @return Connection configuration.
     */
    [[nodiscard]] const configuration &get_configuration() const;

    /**
     * Is auto commit.
     *
     * @return @c true if the auto commit is enabled.
     */
    [[nodiscard]] bool is_auto_commit() const;

    /**
     * Perform transaction commit.
     */
    void transaction_commit();

    /**
     * Perform transaction rollback.
     */
    void transaction_rollback();

    /**
     * Start transaction.
     *
     * @return Operation result.
     */
    void transaction_start();

    /**
     * Get connection attribute.
     *
     * @param attr Attribute type.
     * @param buf Buffer for value.
     * @param buf_len Buffer length.
     * @param value_len Resulting value length.
     */
    void get_attribute(int attr, void *buf, SQLINTEGER buf_len, SQLINTEGER *value_len);

    /**
     * Set connection attribute.
     *
     * @param attr Attribute type.
     * @param value Value pointer.
     * @param value_len Value length.
     */
    void set_attribute(int attr, void *value, SQLINTEGER value_len);

    /**
     * Make new request.
     *
     * @param id Request ID.
     * @param op Operation.
     * @param func Function.
     */
    [[nodiscard]] static std::vector<std::byte> make_request(
        std::int64_t id, protocol::client_operation op, const std::function<void(protocol::writer &)> &func);

    /**
     * Get connection schema.
     *
     * @return Schema.
     */
    const std::string &get_schema() const { return m_config.get_schema().get_value(); }

    /**
     * Get timeout.
     *
     * @return Timeout.
     */
    std::int32_t get_timeout() const { return m_timeout; }

    /**
     * Make a synchronous request and get a response.
     *
     * @param op Operation.
     * @param wr Payload writing function.
     * @return Response.
     */
    network::data_buffer_owning sync_request(
        protocol::client_operation op, const std::function<void(protocol::writer &)> &wr) {
        auto req_id = generate_next_req_id();
        auto request = make_request(req_id, op, wr);

        send_message(request);
        return receive_message(req_id);
    }

    /**
     * Make a synchronous request and get a response.
     *
     * @param op Operation.
     * @param wr Payload writing function.
     * @return Response and error.
     */
    std::pair<network::data_buffer_owning, std::optional<odbc_error>> sync_request_nothrow(
        protocol::client_operation op, const std::function<void(protocol::writer &)> &wr) {
        auto req_id = generate_next_req_id();
        auto request = make_request(req_id, op, wr);

        send_message(request);
        return receive_message_nothrow(req_id);
    }

    /**
     * Get transaction ID.
     *
     * @return Transaction ID.
     */
    [[nodiscard]] std::optional<std::int64_t> get_transaction_id() const { return m_transaction_id; }

    /**
     * Mark transaction non-empty.
     *
     * After this call connection assumes there is at least one operation performed with this transaction.
     */
    void mark_transaction_non_empty() { m_transaction_empty = false; }

    /**
     * Get observable timestamp.
     *
     * @return Observable timestamp.
     */
    std::int64_t get_observable_timestamp() const { return m_observable_timestamp.load(); }

private:
    /**
     * Generate and get next request ID.
     *
     * @return Request ID.
     */
    std::int64_t generate_next_req_id() { return m_req_id_gen.fetch_add(1); }

    /**
     * Init connection socket, using configuration.
     *
     * @return Operation result.
     */
    void init_socket();

    /**
     * Establish connection to ODBC server.
     * Internal call.
     *
     * @param connectStr Connection string.
     * @param parentWindow Parent window.
     * @return Operation result.
     */
    sql_result internal_establish(const std::string &connectStr, void *parentWindow);

    /**
     * Establish connection to ODBC server.
     * Internal call.
     *
     * @param cfg Configuration.
     * @return Operation result.
     */
    sql_result internal_establish(const configuration &cfg);

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
    sql_result internal_get_info(connection_info::info_type type, void *buf, short buffer_len, short *result_len);

    /**
     * Create statement associated with the connection.
     * Internal call.
     *
     * @param statement Pointer to valid instance on success and NULL on failure.
     * @return Operation result.
     */
    sql_result internal_create_statement(sql_statement *&statement);

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
     * Enable autocommit.
     *
     * @return Operation result.
     */
    sql_result enable_autocommit();

    /**
     * Disable autocommit.
     *
     * @return Operation result.
     */
    sql_result disable_autocommit();

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
    sql_result internal_get_attribute(int attr, void *buf, SQLINTEGER buf_len, SQLINTEGER *value_len);

    /**
     * Set connection attribute.
     * Internal call.
     *
     * @param attr Attribute type.
     * @param value Value pointer.
     * @param value_len Value length.
     * @return Operation result.
     */
    sql_result internal_set_attribute(int attr, void *value, SQLINTEGER value_len);

    /**
     * Receive specified number of bytes.
     *
     * @param dst Buffer for data.
     * @param len Number of bytes to receive.
     * @param timeout Timeout.
     * @return Operation result.
     */
    operation_result receive_all(void *dst, std::size_t len, std::int32_t timeout);

    /**
     * Send specified number of bytes.
     *
     * @param data Data buffer.
     * @param len Data length.
     * @param timeout Timeout.
     * @return Operation result.
     */
    operation_result send_all(const std::byte *data, std::size_t len, std::int32_t timeout);

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
     * @throw ignite_error on failure.
     * @return @c true on success and @c false otherwise.
     */
    bool try_restore_connection();

    /**
     * Safe connect.
     *
     * @param addr Address.
     * @return @c true if connection was successful.
     */
    bool safe_connect(const end_point &addr);

    /**
     * Retrieve timeout from parameter.
     *
     * @param value parameter.
     * @return Timeout.
     */
    std::int32_t retrieve_timeout(void *value);

    /**
     * Handle new observable timestamp.
     *
     * @param timestamp Timestamp.
     */
    void on_observable_timestamp(std::int64_t timestamp);

    /**
     * Constructor.
     */
    explicit sql_connection(sql_environment *env)
        : m_env(env)
        , m_config()
        , m_info(m_config) {}

    /** Parent. */
    sql_environment *m_env;

    /** Connection timeout in seconds. */
    std::int32_t m_timeout{0};

    /** Login timeout in seconds. */
    std::int32_t m_login_timeout{DEFAULT_CONNECT_TIMEOUT};

    /** Autocommit flag. */
    bool m_auto_commit{true};

    /** Current transaction ID. */
    std::optional<std::int64_t> m_transaction_id;

    /** Current transaction empty. */
    bool m_transaction_empty{true};

    /** Configuration. */
    configuration m_config;

    /** Connection info. */
    connection_info m_info;

    /** Socket client. */
    std::unique_ptr<network::socket_client> m_socket;

    /** Protocol version. */
    protocol::protocol_version m_protocol_version;

    /** Request ID generator. */
    std::atomic_int64_t m_req_id_gen{0};

    /** Observable timestamp. */
    std::atomic_int64_t m_observable_timestamp{0};
};

} // namespace ignite