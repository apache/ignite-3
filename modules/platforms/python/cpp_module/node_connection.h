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

#include "ignite/common/end_point.h"
#include "ignite/common/detail/bytes.h"
#include "ignite/common/detail/utils.h"
#include "ignite/network/socket_client.h"
#include "ignite/network/network.h"
#include "ignite/protocol/client_operation.h"
#include "ignite/protocol/protocol_version.h"
#include "ignite/protocol/messages.h"
#include "ignite/protocol/reader.h"
#include "ignite/protocol/writer.h"

#include <atomic>
#include <cstdint>
#include <cassert>
#include <optional>
#include <random>
#include <string>

#include "ssl_config.h"
#include "type_conversion.h"


/**
 * A single node connection.
 * TODO: https://issues.apache.org/jira/browse/IGNITE-25744 Move connection logic to the protocol library.
 */
class node_connection final {
public:
    static constexpr std::int32_t DEFAULT_TIMEOUT_SECONDS = 30;

    /**
     * Destructor.
     */
    ~node_connection() {
        close();
    }

    /**
     * Get schema.
     *
     * @return Schema.
     */
    [[nodiscard]] const std::string &get_schema() const { return m_schema; }

    /**
     * Get page size.
     *
     * @return Page size.
     */
    [[nodiscard]] std::int32_t get_page_size() const { return m_page_size; }

    /**
     * Get timeout.
     *
     * @return Timeout.
     */
    [[nodiscard]] std::int32_t get_timeout() const { return m_timeout; }

    /**
     * Constructor.
     *
     * @param addresses Addresses.
     * @param schema Schema. Can be empty.
     * @param auth_identity Auth identity. Can be empty.
     * @param auth_secret Auth secret. Can be empty.
     * @param page_size Page size.
     * @param timeout Timeout.
     * @param auto_commit Auto commit flag.
     * @param ssl_cfg SSL Configuration.
     */
    node_connection(std::vector<ignite::end_point> addresses, std::string schema, std::string auth_identity,
            std::string auth_secret, std::int32_t page_size, std::int32_t timeout, bool auto_commit, ssl_config ssl_cfg)
        : m_addresses(std::move(addresses))
        , m_schema(schema.empty() ? "PUBLIC" : std::move(schema))
        , m_auth_identity(std::move(auth_identity))
        , m_auth_secret(std::move(auth_secret))
        , m_page_size(page_size > 0 ? page_size : 1024)
        , m_timeout(timeout > 0 ? timeout : DEFAULT_TIMEOUT_SECONDS)
        , m_auto_commit(auto_commit)
        , m_ssl_config(std::move(ssl_cfg))
    {
        std::random_device device;
        std::mt19937 generator(device());
        std::uniform_int_distribution<std::uint32_t> distribution(0, m_addresses.size());
        m_current_address_idx = distribution(generator);
    }

    /**
     * Close the current connection.
     */
    void close() noexcept {
        if (m_socket) {
            m_socket->close();
            m_socket.reset();

            m_transaction_id = std::nullopt;
            m_transaction_empty = true;
        }
    }

    /**
     * Get autocommit flag.
     *
     * @return Autocommit flag.
     */
    bool is_autocommit() const noexcept { return m_auto_commit; }

    /**
     * Set autocommit flag.
     *
     * @param autocommit New value.
     */
    void set_autocommit(bool autocommit) {
        if (!m_auto_commit && autocommit) {
            enable_autocommit();
        } else if (m_auto_commit && !autocommit) {
            disable_autocommit();
        }
    }

    /**
     * Commit a current transaction.
     */
    void transaction_commit() {
        if (!m_transaction_id) {
            return;
        }

        sync_request(ignite::protocol::client_operation::TX_COMMIT,
                     [&](ignite::protocol::writer &writer) { writer.write(*m_transaction_id); });

        m_transaction_id = std::nullopt;
        m_transaction_empty = true;
    }

    /**
     * Rollback a current transaction.
     */
    void transaction_rollback() {
        if (!m_transaction_id) {
            return;
        }

        sync_request(ignite::protocol::client_operation::TX_ROLLBACK,
                     [&](ignite::protocol::writer &writer) { writer.write(*m_transaction_id); });

        m_transaction_id = std::nullopt;
        m_transaction_empty = true;
    }

    /**
     * Establish a connection.
     */
    void establish() {
        try_restore_connection();
    }

    /**
     * Get observable timestamp.
     *
     * @return Observable timestamp.
     */
    std::int64_t get_observable_timestamp() const { return m_observable_timestamp.load(); }

    /**
     * Mark transaction non-empty.
     *
     * After this call connection assumes there is at least one operation performed with this transaction.
     */
    void mark_transaction_non_empty() { m_transaction_empty = false; }

    /**
     * Start a new transaction.
     */
    void transaction_start() {
        ignite::network::data_buffer_owning response =
            sync_request(ignite::protocol::client_operation::TX_BEGIN, [&](ignite::protocol::writer &writer) {
                writer.write_bool(false); // read_only.
                writer.write(std::int64_t(0)); // timeout_millis.
                writer.write(get_observable_timestamp());
            });

        ignite::protocol::reader reader(response.get_bytes_view());
        m_transaction_id = reader.read_int64();
    }

    /**
     * Is auto commit.
     *
     * @return @c true if the auto commit is enabled.
     */
    [[nodiscard]] bool is_auto_commit() const { return m_auto_commit; }

    /**
     * Get transaction ID.
     *
     * @return Transaction ID.
     */
    [[nodiscard]] std::optional<std::int64_t> get_transaction_id() const { return m_transaction_id; }

    /**
     * Make a synchronous request and get a response.
     *
     * @param op Operation.
     * @param wr Payload writing function.
     * @return Response and error.
     */
    std::pair<ignite::network::data_buffer_owning, std::optional<ignite::ignite_error>> sync_request_nothrow(
        ignite::protocol::client_operation op, const std::function<void(ignite::protocol::writer &)> &wr) {
        auto req_id = generate_next_req_id();
        auto request = make_request(req_id, op, wr);

        send_message(request, m_timeout);
        return receive_message_nothrow(req_id, m_timeout);
    }

private:
    /**
     * Send all data by connection.
     *
     * @param data Pointer to data to be sent.
     * @param size Size of the data in bytes.
     * @param timeout Timeout.
     */
    void send_all(const std::byte *data, std::size_t size, std::int32_t timeout) {
        std::int64_t sent = 0;
        while (sent != static_cast<std::int64_t>(size)) {
            int res = m_socket->send(data + sent, size - sent, timeout);

            if (res < 0 || res == ignite::network::socket_client::wait_result::TIMEOUT) {
                close();
                throw ignite::ignite_error(ignite::error::code::CONNECTION,
                    "Can not send a message to the server due to "
                    + std::string(res < 0 ? "connection error" : "operation timed out"));
            }

            sent += res;
        }

        assert(static_cast<std::size_t>(sent) == size);
    }

    /**
     * Receive exactly the specified number of bytes.
     *
     * @param dst A buffer pointer.
     * @param size A message size to receive exactly.
     * @param timeout Timeout.
     */
    void receive_all(void *dst, std::size_t size, std::int32_t timeout) {
        std::size_t remain = size;
        auto *buffer = static_cast<std::byte *>(dst);

        while (remain) {
            std::size_t received = size - remain;

            int res = m_socket->receive(buffer + received, remain, timeout);

            if (res < 0 || res == ignite::network::socket_client::wait_result::TIMEOUT) {
                close();
                throw ignite::ignite_error(ignite::error::code::CONNECTION,
                    "Can not receive a message from the server due to " +
                    std::string(res < 0 ? "connection error" : "operation timed out"));
            }

            remain -= static_cast<std::size_t>(res);
        }
    }

    /**
     * Receive the next protocol message.
     *
     * @param msg A buffer for the message.
     * @param timeout Timeout.
     */
    void receive_message(std::vector<std::byte> &msg, std::int32_t timeout) {
        if (!m_socket)
            throw ignite::ignite_error(ignite::error::code::CONNECTION, "Connection is not established");

        msg.clear();

        std::byte len_buffer[ignite::protocol::HEADER_SIZE];
        receive_all(&len_buffer, sizeof(len_buffer), timeout);

        static_assert(sizeof(std::int32_t) == ignite::protocol::HEADER_SIZE);
        std::int32_t len = ignite::detail::bytes::load<ignite::detail::endian::BIG, std::int32_t>(len_buffer);
        if (len <= 0) {
            close();
            throw ignite::ignite_error(ignite::error::code::PROTOCOL,
                "Protocol error: Unexpected message length: " + std::to_string(len));
        }

        msg.resize(len);

        receive_all(msg.data(), len, timeout);
    }

    /**
     * Send a message.
     *
     * @param req Request.
     * @param timeout Timeout.
     */
    void send_message(ignite::bytes_view req, std::int32_t timeout) {
        ensure_connected();

        send_all(req.data(), req.size(), timeout);
    }

    /**
     * Receive a message.
     *
     * @param id Message ID.
     * @param timeout Timeout.
     * @return A received message.
     */
    ignite::network::data_buffer_owning receive_message(std::int64_t id, std::int32_t timeout) {
        auto res = receive_message_nothrow(id, timeout);
        if (res.second) {
            throw std::move(*res.second);
        }
        return std::move(res.first);
    }

    /**
     * Receives a message from server, but returns it as a value if it contains error.
     *
     * @param id Expected message ID.
     * @param timeout Timeout.
     * @return A message buffer and server error if any.
     */
    std::pair<ignite::network::data_buffer_owning, std::optional<ignite::ignite_error>> receive_message_nothrow(
        std::int64_t id, std::int32_t timeout) {
        ensure_connected();
        std::vector<std::byte> res;

        while (true) {
            receive_message(res, timeout);

            ignite::protocol::reader reader(res);
            auto req_id = reader.read_int64();
            if (req_id != id) {
                throw ignite::ignite_error(ignite::error::code::SERVER_TO_CLIENT_REQUEST,
                    "Response with unknown ID is received: " + std::to_string(req_id));
            }

            auto flags = reader.read_int32();
            if (test_flag(flags, ignite::protocol::response_flag::PARTITION_ASSIGNMENT_CHANGED)) {
                auto assignment_ts = reader.read_int64();

                UNUSED_VALUE assignment_ts;
            }

            auto observable_timestamp = reader.read_int64();
            on_observable_timestamp(observable_timestamp);

            std::optional<ignite::ignite_error> err;
            if (test_flag(flags, ignite::protocol::response_flag::ERROR_FLAG)) {
                err = read_error(reader);
            }

            return {ignite::network::data_buffer_owning{std::move(res), reader.position()}, err};
        }
    }

    /**
     * Make new request.
     *
     * @param id Request ID.
     * @param op Operation.
     * @param func Function.
     */
    std::vector<std::byte> make_request(std::int64_t id, ignite::protocol::client_operation op,
        const std::function<void(ignite::protocol::writer &)> &func) {
        std::vector<std::byte> req;
        ignite::protocol::buffer_adapter buffer(req);
        buffer.reserve_length_header();

        ignite::protocol::writer writer(buffer);
        writer.write(std::int32_t(op));
        writer.write(id);
        func(writer);

        buffer.write_length_header();

        return req;
    }

    /**
     * Make a synchronous request and get a response.
     *
     * @param op Operation.
     * @param wr Payload writing function.
     * @return Response.
     */
    ignite::network::data_buffer_owning sync_request(
        ignite::protocol::client_operation op, const std::function<void(ignite::protocol::writer &)> &wr) {
        auto req_id = generate_next_req_id();
        auto request = make_request(req_id, op, wr);

        send_message(request, m_timeout);
        return receive_message(req_id, m_timeout);
    }

    /**
     * Generate and get the next request ID.
     *
     * @return Request ID.
     */
    std::int64_t generate_next_req_id() { return m_req_id_gen.fetch_add(1); }

    /**
     * Ensure the connection is established.
     */
    void ensure_connected() {
        if (m_socket)
            return;

        return try_restore_connection();
    }

    /**
     * Try and re-establish connection.
     *
     * @return @c true on success and @c false on failure.
     */
    void try_restore_connection() {
        if (!m_socket) {
            if (m_ssl_config.m_enabled) {
                try
                {
                    ignite::network::ensure_ssl_loaded();
                }
                catch (const ignite::ignite_error &err)
                {
                    auto openssl_home = ignite::detail::get_env("OPENSSL_HOME");
                    std::string openssl_home_str{"OPENSSL_HOME"};
                    if (openssl_home.has_value()) {
                        openssl_home_str += "='" + openssl_home.value() + '\'';
                    } else {
                        openssl_home_str += " is not set";
                    }

                    throw ignite::ignite_error(ignite::error::code::CLIENT_SSL_CONFIGURATION,
                        "Can not load OpenSSL library. [" + openssl_home_str + "]");
                }

                ignite::network::secure_configuration cfg;
                cfg.key_path = m_ssl_config.m_ssl_keyfile;
                cfg.cert_path = m_ssl_config.m_ssl_certfile;
                cfg.ca_path = m_ssl_config.m_ssl_ca_certfile;

                m_socket = ignite::network::make_secure_socket_client(std::move(cfg));
            } else {
                m_socket = ignite::network::make_tcp_socket_client();
            }
        }

        std::stringstream msgs;
        bool connected = false;
        for (std::int32_t i = 0; i < m_addresses.size(); ++i) {
            uint32_t idx = (m_current_address_idx + i) % m_addresses.size();
            const ignite::end_point &address = m_addresses[idx];

            try {
                bool success = m_socket->connect(address.host.c_str(), address.port, m_timeout);
                if (!success) {
                    continue;
                }
            } catch (const ignite::ignite_error &err) {
                msgs << "Error while trying connect to " << address.host << ":" << address.port << ", " << err.what_str();
                continue;
            }

            try {
                make_request_handshake();
                connected = true;
                break;
            } catch (const ignite::ignite_error &err) {
                msgs << "Error during handshake with " << address.host << ":" << address.port << ", " << err.what_str();
            }
        }

        if (!connected) {
            close();
            throw ignite::ignite_error(ignite::error::code::CONNECTION,
                "Failed to establish connection with the cluster: " + msgs.str());
        }
    }

    /**
     * Make a handshake.
     */
    void make_request_handshake() {
        static constexpr std::int8_t CLIENT_CODE = 4;
        m_protocol_version = ignite::protocol::protocol_version::get_current();

        std::map<std::string, std::string> extensions;
        if (!m_auth_identity.empty()) {
            static const std::string AUTH_TYPE{"basic"};

            extensions.emplace("authn-type", AUTH_TYPE);
            extensions.emplace("authn-identity", m_auth_identity);
            extensions.emplace("authn-secret", m_auth_secret);
        }

        std::vector<std::byte> message = make_handshake_request(CLIENT_CODE, m_protocol_version, extensions);

        send_all(message.data(), message.size(), m_timeout);
        receive_and_check_magic(message, m_timeout);
        receive_message(message, m_timeout);

        auto response = ignite::protocol::parse_handshake_response(message);
        auto const &ver = response.context.get_version();

        // We now only support a single version
        if (ver != ignite::protocol::protocol_version::get_current()) {
            throw ignite::ignite_error(ignite::error::code::PROTOCOL_COMPATIBILITY, "Unsupported server version: " + ver.to_string() + ".");
        }

        if (response.error) {
            throw ignite::ignite_error(ignite::error::code::HANDSHAKE_HEADER, "Server rejected handshake with error: " + response.error->what_str());
        }
    }

    /**
     * Receive and check magic bytes.
     *
     * @param buffer A buffer for message.
     * @param timeout Timeout.
     * @return @c true on success and @c false on failure.
     */
    void receive_and_check_magic(std::vector<std::byte> &buffer, std::int32_t timeout) {
        buffer.clear();
        buffer.resize(ignite::protocol::MAGIC_BYTES.size());

        receive_all(buffer.data(), buffer.size(), timeout);

        if (!std::equal(buffer.begin(), buffer.end(),
            ignite::protocol::MAGIC_BYTES.begin(), ignite::protocol::MAGIC_BYTES.end()))
        {
            throw ignite::ignite_error(ignite::error::code::HANDSHAKE_HEADER,
                "Failed to receive magic bytes in handshake response. "
                "Possible reasons: wrong port number used, TLS is enabled on server but not on client.");
        }
    }

    /**
     * Enable autocommit.
     */
    void enable_autocommit() {
        assert(!m_auto_commit);

        if (m_transaction_id) {
            if (m_transaction_empty)
                transaction_rollback();
            else
                transaction_commit();
        }

        m_transaction_id = std::nullopt;
        m_transaction_empty = true;
        m_auto_commit = true;
    }

    /**
     * Disable autocommit.
     */
    void disable_autocommit() {
        assert(m_auto_commit);
        assert(!m_transaction_id);

        transaction_start();

        m_transaction_empty = true;
        m_auto_commit = false;
    }

    /**
     * Process received value of the observable timestamp.
     *
     * @param timestamp Timestamp.
     */
    void on_observable_timestamp(std::int64_t timestamp) {
        auto expected = m_observable_timestamp.load();
        while (expected < timestamp) {
            auto success = m_observable_timestamp.compare_exchange_weak(expected, timestamp);
            if (success)
                return;
            expected = m_observable_timestamp.load();
        }
    }

    /** Addresses. */
    const std::vector<ignite::end_point> m_addresses;

    /** Schema. */
    const std::string m_schema;

    /** Identity. */
    const std::string m_auth_identity;

    /** Secret. */
    const std::string m_auth_secret;

    /** Page size. */
    const std::int32_t m_page_size;

    /** Current address index. */
    std::uint32_t m_current_address_idx{0};

    /** Operation timeout in seconds. */
    std::int32_t m_timeout{DEFAULT_TIMEOUT_SECONDS};

    /** Autocommit flag. */
    bool m_auto_commit{true};

    /** Current transaction ID. */
    std::optional<std::int64_t> m_transaction_id;

    /** Current transaction empty. */
    bool m_transaction_empty{true};

    /** Socket client. */
    std::unique_ptr<ignite::network::socket_client> m_socket;

    /** Protocol version. */
    ignite::protocol::protocol_version m_protocol_version;

    /** Request ID generator. */
    std::atomic_int64_t m_req_id_gen{0};

    /** Observable timestamp. */
    std::atomic_int64_t m_observable_timestamp{0};

    /** SSL Configuration. */
    const ssl_config m_ssl_config;
};
