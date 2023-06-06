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

#include "ignite/odbc/sql_connection.h"
#include "ignite/odbc/config/configuration.h"
#include "ignite/odbc/config/connection_string_parser.h"
#include "ignite/odbc/log.h"
#include "ignite/odbc/sql_environment.h"
#include "ignite/odbc/sql_statement.h"
#include "ignite/odbc/ssl_mode.h"
#include "ignite/odbc/utility.h"

#include <ignite/network/network.h>
#include <ignite/common/bytes.h>

#include <algorithm>
#include <cstring>
#include <cstddef>
#include <random>
#include <sstream>

constexpr const std::size_t PROTOCOL_HEADER_SIZE = 4;

namespace
{

/**
 * Get hex dump of binary data in string form.
 * @param data Data.
 * @param count Number of bytes.
 * @return Hex dump string.
 */
std::string hex_dump(const void* data, std::size_t count)
{
    std::stringstream dump;
    std::size_t cnt = 0;
    auto bytes = (const std::uint8_t*)data;
    for (auto *p = bytes, *e = bytes + count; p != e; ++p)
    {
        if (cnt++ % 16 == 0)
        {
            dump << std::endl;
        }
        dump << std::hex << std::setfill('0') << std::setw(2) << (int)*p << " ";
    }
    return dump.str();
}

}

std::vector<ignite::network::tcp_range> collect_addresses(const ignite::configuration& cfg)
{
    std::vector<ignite::network::tcp_range> end_points = cfg.get_addresses();

    std::random_device device;
    std::mt19937 generator(device());
    std::shuffle(end_points.begin(), end_points.end(), generator);

    return end_points;
}

namespace ignite {

void sql_connection::get_info(connection_info::info_type type, void* buf, short buffer_len, short* result_len)
{
    LOG_MSG("SQLGetInfo called: "
        << type << " ("
        << connection_info::info_type_to_string(type) << "), "
        << std::hex << reinterpret_cast<size_t>(buf) << ", "
        << buffer_len << ", "
        << std::hex << reinterpret_cast<size_t>(result_len)
        << std::dec);

    IGNITE_ODBC_API_CALL(internal_get_info(type, buf, buffer_len, result_len));
}

sql_result sql_connection::internal_get_info(connection_info::info_type type, void* buf, short buffer_len, short* result_len)
{
    const connection_info& info = get_info();

    sql_result res = info.get_info(type, buf, buffer_len, result_len);

    if (res != sql_result::AI_SUCCESS)
        add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Not implemented.");

    return res;
}

void sql_connection::establish(const std::string& connect_str, void* parent_window)
{
    IGNITE_ODBC_API_CALL(internal_establish(connect_str, parent_window));
}

sql_result sql_connection::internal_establish(const std::string& connect_str, void* parent_window)
{
    connection_string_parser parser(m_config);
    parser.parse_connection_string(connect_str, &get_diagnostic_records());

    if (parent_window)
    {
        // TODO: IGNITE-19210 Implement UI for connection
        add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Connection using UI is not supported");

        return sql_result::AI_ERROR;
    }

    // TODO: IGNITE-19210 Add DSN support

    return internal_establish(m_config);
}

void sql_connection::establish(const configuration& cfg)
{
    IGNITE_ODBC_API_CALL(internal_establish(cfg));
}

void sql_connection::init_socket()
{
    if (!m_socket)
        m_socket = network::make_tcp_socket_client();
}

sql_result sql_connection::internal_establish(const configuration& cfg)
{
    m_config = cfg;

    if (!m_config.is_addresses_set() || m_config.get_addresses().empty())
    {
        add_status_record("No valid address to connect.");

        return sql_result::AI_ERROR;
    }

    bool connected = try_restore_connection();

    if (!connected)
    {
        add_status_record(sql_state::S08001_CANNOT_CONNECT, "Failed to establish connection with the host.");

        return sql_result::AI_ERROR;
    }

    bool errors = get_diagnostic_records().get_status_records_number() > 0;

    return errors ? sql_result::AI_SUCCESS_WITH_INFO : sql_result::AI_SUCCESS;
}

void sql_connection::release()
{
    IGNITE_ODBC_API_CALL(internal_release());
}

void sql_connection::deregister()
{
    m_env->deregister_connection(this);
}

sql_result sql_connection::internal_release()
{
    if (!m_socket)
    {
        add_status_record(sql_state::S08003_NOT_CONNECTED, "Connection is not open.");

        // It is important to return SUCCESS_WITH_INFO and not ERROR here, as if we return an error, Windows
        // Driver Manager may decide that connection is not valid anymore which results in memory leak.
        return sql_result::AI_SUCCESS_WITH_INFO;
    }

    close();

    return sql_result::AI_SUCCESS;
}

void sql_connection::close()
{
    if (m_socket)
    {
        m_socket->close();
        m_socket.reset();
    }
}

sql_statement *sql_connection::create_statement()
{
    sql_statement * statement;

    IGNITE_ODBC_API_CALL(internal_create_statement(statement));

    return statement;
}

sql_result sql_connection::internal_create_statement(sql_statement *& statement)
{
    statement = new sql_statement(*this);

    if (!statement)
    {
        add_status_record(sql_state::SHY001_MEMORY_ALLOCATION, "Not enough memory.");

        return sql_result::AI_ERROR;
    }

    return sql_result::AI_SUCCESS;
}

bool sql_connection::send(const std::byte* data, std::size_t len, std::int32_t timeout)
{
    if (!m_socket)
        throw odbc_error(sql_state::S08003_NOT_CONNECTED, "Connection is not established");

    auto new_len = len + PROTOCOL_HEADER_SIZE;
    std::vector<std::byte> msg(new_len);

    bytes::store<endian::BIG, std::int32_t>(msg.data(), std::int32_t(len));
    memcpy(msg.data() + PROTOCOL_HEADER_SIZE, data, len);

    operation_result res = send_all(msg.data(), msg.size(), timeout);
    if (res == operation_result::TIMEOUT)
        return false;

    if (res == operation_result::FAIL)
        throw odbc_error(sql_state::S08S01_LINK_FAILURE, "Can not send message due to connection failure");

    TRACE_MSG("message sent: (" <<  msg.size() << " bytes)" << hex_dump(msg.data(), msg.size()));

    return true;
}

sql_connection::operation_result sql_connection::send_all(const std::byte* data, std::size_t len, std::int32_t timeout)
{
    std::int64_t sent = 0;
    while (sent != static_cast<std::int64_t>(len))
    {
        int res = m_socket->send(data + sent, len - sent, timeout);
        LOG_MSG("Send result: " << res);

        if (res < 0 || res == network::socket_client::wait_result::TIMEOUT)
        {
            close();
            return res < 0 ? operation_result::FAIL : operation_result::TIMEOUT;
        }
        sent += res;
    }

    assert(static_cast<std::size_t>(sent) == len);

    return operation_result::SUCCESS;
}

bool sql_connection::receive(std::vector<std::byte>& msg, std::int32_t timeout)
{
    if (!m_socket)
        throw odbc_error(sql_state::S08003_NOT_CONNECTED, "Connection is not established");

    msg.clear();

    std::byte len_buffer[PROTOCOL_HEADER_SIZE];
    operation_result res = receive_all(&len_buffer, sizeof(len_buffer), timeout);

    if (res == operation_result::TIMEOUT)
        return false;

    if (res == operation_result::FAIL)
        throw odbc_error(sql_state::S08S01_LINK_FAILURE, "Can not receive message header");

    static_assert(sizeof(std::int32_t) == PROTOCOL_HEADER_SIZE);
    std::int32_t len = bytes::load<endian::BIG, std::int32_t>(len_buffer);
    if (len < 0)
    {
        close();
        throw odbc_error(sql_state::SHY000_GENERAL_ERROR, "Protocol error: Message length is negative");
    }

    if (len == 0)
        return false;

    msg.resize(len);
    res = receive_all(&msg[0], len, timeout);
    if (res == operation_result::TIMEOUT)
        return false;

    if (res == operation_result::FAIL)
        throw odbc_error(sql_state::S08S01_LINK_FAILURE, "Can not receive message body");

    TRACE_MSG("Message received: " << hex_dump(&msg[0], msg.size()));

    return true;
}

sql_connection::operation_result sql_connection::receive_all(void* dst, std::size_t len, std::int32_t timeout)
{
    std::size_t remain = len;
    auto* buffer = reinterpret_cast<std::byte*>(dst);

    while (remain)
    {
        std::size_t received = len - remain;

        int res = m_socket->receive(buffer + received, remain, timeout);
        LOG_MSG("Receive res: " << res << ", remain: " << remain);

        if (res < 0 || res == network::socket_client::wait_result::TIMEOUT)
        {
            close();
            return res < 0 ? operation_result::FAIL : operation_result::TIMEOUT;
        }

        remain -= static_cast<std::size_t>(res);
    }

    return operation_result::SUCCESS;
}

const configuration&sql_connection::get_configuration() const
{
    return m_config;
}

bool sql_connection::is_auto_commit() const
{
    return m_auto_commit;
}

void sql_connection::transaction_commit()
{
    IGNITE_ODBC_API_CALL(internal_transaction_commit());
}

sql_result sql_connection::internal_transaction_commit()
{
    // TODO: IGNITE-19399: Implement transaction support

    add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Transactions are not supported.");
    return sql_result::AI_ERROR;
}

void sql_connection::transaction_rollback()
{
    IGNITE_ODBC_API_CALL(internal_transaction_rollback());
}

sql_result sql_connection::internal_transaction_rollback()
{
    // TODO: IGNITE-19399: Implement transaction support

    add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Transactions are not supported.");
    return sql_result::AI_ERROR;
}

void sql_connection::get_attribute(int attr, void* buf, SQLINTEGER buf_len, SQLINTEGER* value_len)
{
    IGNITE_ODBC_API_CALL(internal_get_attribute(attr, buf, buf_len, value_len));
}

sql_result sql_connection::internal_get_attribute(int attr, void* buf, SQLINTEGER, SQLINTEGER* value_len)
{
    if (!buf)
    {
        add_status_record(sql_state::SHY009_INVALID_USE_OF_NULL_POINTER, "Data buffer is null.");

        return sql_result::AI_ERROR;
    }

    switch (attr)
    {
        case SQL_ATTR_CONNECTION_DEAD:
        {
            auto *val = reinterpret_cast<SQLUINTEGER*>(buf);

            *val = m_socket ? SQL_CD_FALSE : SQL_CD_TRUE;
            if (value_len)
                *value_len = SQL_IS_INTEGER;

            break;
        }

        case SQL_ATTR_CONNECTION_TIMEOUT:
        {
            auto *val = reinterpret_cast<SQLUINTEGER*>(buf);

            *val = static_cast<SQLUINTEGER>(m_timeout);

            if (value_len)
                *value_len = SQL_IS_INTEGER;

            break;
        }

        case SQL_ATTR_LOGIN_TIMEOUT:
        {
            auto *val = reinterpret_cast<SQLUINTEGER*>(buf);

            *val = static_cast<SQLUINTEGER>(m_login_timeout);

            if (value_len)
                *value_len = SQL_IS_INTEGER;

            break;
        }

        case SQL_ATTR_AUTOCOMMIT:
        {
            auto *val = reinterpret_cast<SQLUINTEGER*>(buf);

            *val = m_auto_commit ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;

            if (value_len)
                *value_len = SQL_IS_INTEGER;

            break;
        }

        default:
        {
            add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                "Specified attribute is not supported.");

            return sql_result::AI_ERROR;
        }
    }

    return sql_result::AI_SUCCESS;
}

void sql_connection::set_attribute(int attr, void* value, SQLINTEGER value_len)
{
    IGNITE_ODBC_API_CALL(internal_set_attribute(attr, value, value_len));
}

sql_result sql_connection::internal_set_attribute(int attr, void* value, SQLINTEGER)
{
    switch (attr)
    {
        case SQL_ATTR_CONNECTION_DEAD:
        {
            add_status_record(sql_state::SHY092_OPTION_TYPE_OUT_OF_RANGE, "Attribute is read only.");

            return sql_result::AI_ERROR;
        }

        case SQL_ATTR_CONNECTION_TIMEOUT:
        {
            m_timeout = retrieve_timeout(value);

            if (get_diagnostic_records().get_status_records_number() != 0)
                return sql_result::AI_SUCCESS_WITH_INFO;

            break;
        }

        case SQL_ATTR_LOGIN_TIMEOUT:
        {
            m_login_timeout = retrieve_timeout(value);

            if (get_diagnostic_records().get_status_records_number() != 0)
                return sql_result::AI_SUCCESS_WITH_INFO;

            break;
        }

        case SQL_ATTR_AUTOCOMMIT:
        {
            auto mode = static_cast<SQLUINTEGER>(reinterpret_cast<ptrdiff_t>(value));

            if (mode != SQL_AUTOCOMMIT_ON && mode != SQL_AUTOCOMMIT_OFF)
            {
                add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                    "Specified attribute is not supported.");

                return sql_result::AI_ERROR;
            }

            m_auto_commit = mode == SQL_AUTOCOMMIT_ON;

            break;
        }

        default:
        {
            add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                "Specified attribute is not supported.");

            return sql_result::AI_ERROR;
        }
    }

    return sql_result::AI_SUCCESS;
}

sql_result sql_connection::make_request_handshake()
{
    static constexpr int8_t ODBC_CLIENT = 3;
    m_protocol_version = protocol_version::get_current();

    try
    {
        std::vector<std::byte> message;
        {
            protocol::buffer_adapter buffer(message);
            buffer.write_raw(bytes_view(protocol::MAGIC_BYTES));

            protocol::write_message_to_buffer(buffer, [&ver = m_protocol_version](protocol::writer &writer) {
                writer.write(ver.get_major());
                writer.write(ver.get_minor());
                writer.write(ver.get_maintenance());

                writer.write(ODBC_CLIENT);

                // Features.
                writer.write_binary_empty();

                // Extensions.
                writer.write_map_empty();
            });
        }

        auto res = send_all(message.data(), message.size(), m_login_timeout);
        if (res != operation_result::SUCCESS)
        {
            add_status_record(sql_state::S08001_CANNOT_CONNECT, "Failed to send handshake request");
            return sql_result::AI_ERROR;
        }

        message.clear();
        message.resize(protocol::MAGIC_BYTES.size());

        res = receive_all(message.data(), message.size(), m_login_timeout);
        if (res != operation_result::SUCCESS)
        {
            add_status_record(sql_state::S08001_CANNOT_CONNECT,
                "Failed to get handshake response (Did you forget to enable SSL?).");

            return sql_result::AI_ERROR;
        }

        if (!std::equal(message.begin(), message.end(), protocol::MAGIC_BYTES.begin(), protocol::MAGIC_BYTES.end()))
        {
            add_status_record(sql_state::S08001_CANNOT_CONNECT, "Failed to receive magic bytes in handshake response. "
                "Possible reasons: wrong port number used, TLS is enabled on server but not on client.");
            return sql_result::AI_ERROR;
        }

        bool received = receive(message, m_login_timeout);
        if (!received)
        {
            add_status_record(sql_state::S08001_CANNOT_CONNECT, "Failed to get handshake response.");
            return sql_result::AI_ERROR;
        }

        protocol::reader reader(message);

        auto ver_major = reader.read_int16();
        auto ver_minor = reader.read_int16();
        auto ver_patch = reader.read_int16();

        protocol_version ver(ver_major, ver_minor, ver_patch);
        LOG_MSG("Server-side protocol version: " << ver.to_string());

        // We now only support a single version
        if (ver != protocol_version::get_current())
        {
            add_status_record(sql_state::S08004_CONNECTION_REJECTED,
                "Unsupported server version: " + ver.to_string() + ".");
            return sql_result::AI_ERROR;
        }

        auto err = protocol::read_error(reader);
        if (err)
        {
            add_status_record(sql_state::S08004_CONNECTION_REJECTED,
                "Server rejected handshake with error: " + err->what_str());
            return sql_result::AI_ERROR;
        }
    }
    catch (const odbc_error& err)
    {
        add_status_record(err);

        return sql_result::AI_ERROR;
    }
    catch (const ignite_error& err)
    {
        add_status_record(sql_state::S08004_CONNECTION_REJECTED, err.what_str());

        return sql_result::AI_ERROR;
    }

    return sql_result::AI_SUCCESS;
}

void sql_connection::ensure_connected()
{
    if (!m_socket)
        return;

    bool success = try_restore_connection();
    if (!success)
        throw odbc_error(sql_state::S08001_CANNOT_CONNECT, "Failed to establish connection with any provided hosts");
}

bool sql_connection::try_restore_connection()
{
    std::vector<network::tcp_range> addrs = collect_addresses(m_config);

    if (!m_socket)
        init_socket();

    bool connected = false;
    while (!addrs.empty() && !connected)
    {
        const network::tcp_range& addr = addrs.back();

        for (auto port = addr.port; port <= addr.port + addr.range; ++port)
        {
            try
            {
                connected = m_socket->connect(addr.host.c_str(), port, m_login_timeout);
            }
            catch (const ignite_error& err)
            {
                std::stringstream msgs;
                msgs << "Error while trying connect to " << addr.host << ":" << addr.port <<", " << err.what_str();
                add_status_record(sql_state::S08001_CANNOT_CONNECT, msgs.str());
            }

            if (connected)
            {
                sql_result res = make_request_handshake();

                connected = res != sql_result::AI_ERROR;
                if (connected)
                    break;
            }
        }

        addrs.pop_back();
    }

    if (!connected)
        close();

    return connected;
}

std::int32_t sql_connection::retrieve_timeout(void* value)
{
    auto u_timeout = static_cast<SQLUINTEGER>(reinterpret_cast<ptrdiff_t>(value));
    if (u_timeout > INT32_MAX)
    {
        std::stringstream ss;

        ss << "Value is too big: " << u_timeout << ", changing to " << m_timeout << ".";

        add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, ss.str());

        return INT32_MAX;
    }

    return static_cast<std::int32_t>(u_timeout);
}

} // namespace ignite
