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
#include "ignite/odbc/message.h"
#include "ignite/odbc/sql_environment.h"
#include "ignite/odbc/sql_statement.h"
#include "ignite/odbc/ssl_mode.h"
#include "ignite/odbc/utility.h"

#include <ignite/network/network.h>

#include <sstream>
#include <algorithm>
#include <cstring>
#include <cstddef>

// Uncomment for per-byte debug.
// TODO: Change to m_env variable
//#define PER_BYTE_DEBUG

namespace
{
#pragma pack(push, 1)
    struct odbc_protocol_header
    {
        std::int32_t len;
    };
#pragma pack(pop)
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

void sql_connection::establish(const std::string& connectStr, void* parentWindow)
{
    IGNITE_ODBC_API_CALL(internal_establish(connectStr, parentWindow));
}

sql_result sql_connection::internal_establish(const std::string& connectStr, void* parentWindow)
{
    configuration config;
    connection_string_parser parser(config);
    parser.parse_connection_string(connectStr, &get_diagnostic_records());

    if (parentWindow)
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

sql_result sql_connection::init_socket()
{
    // TODO: IGNITE-19204 Implement connection establishment.
    add_status_record(sql_state::S08001_CANNOT_CONNECT, "Connection is not implemented");
    return sql_result::AI_ERROR;
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
    // TODO: IGNITE-19204 Implement connection management.

    close();

    return sql_result::AI_SUCCESS;
}

void sql_connection::close()
{
    // TODO: IGNITE-19204 Implement connection management.
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

bool sql_connection::send(const std::int8_t* data, size_t len, std::int32_t timeout)
{
    // TODO: IGNITE-19204 Implement connection management.

    auto new_len = static_cast<std::int32_t>(len + sizeof(odbc_protocol_header));
    std::vector<std::int8_t> msg(new_len);
    auto *hdr = reinterpret_cast<odbc_protocol_header*>(msg.data());

    // TODO: Re-factor
    hdr->len = static_cast<std::int32_t>(len);

    memcpy(msg.data() + sizeof(odbc_protocol_header), data, len);

    operation_result res = send_all(msg.data(), msg.size(), timeout);

    if (res == operation_result::TIMEOUT)
        return false;

    if (res == operation_result::FAIL)
        throw odbc_error(sql_state::S08S01_LINK_FAILURE, "Can not send message due to connection failure");

#ifdef PER_BYTE_DEBUG
    LOG_MSG("message sent: (" <<  msg.get_size() << " bytes)" << HexDump(msg.get_data(), msg.get_size()));
#endif //PER_BYTE_DEBUG

    return true;
}

sql_connection::operation_result sql_connection::send_all(const std::int8_t* data, size_t len, std::int32_t timeout)
{
    // TODO: IGNITE-19204 Implement connection operations.
    return operation_result::FAIL;
}

bool sql_connection::receive(std::vector<std::int8_t>& msg, std::int32_t timeout)
{
    // TODO: IGNITE-19204 Implement connection management.

    msg.clear();

    odbc_protocol_header hdr{};

    operation_result res = receive_all(reinterpret_cast<std::int8_t*>(&hdr), sizeof(hdr), timeout);

    if (res == operation_result::TIMEOUT)
        return false;

    if (res == operation_result::FAIL)
        throw odbc_error(sql_state::S08S01_LINK_FAILURE, "Can not receive message header");

    if (hdr.len < 0)
    {
        close();

        throw odbc_error(sql_state::SHY000_GENERAL_ERROR, "Protocol error: Message length is negative");
    }

    if (hdr.len == 0)
        return false;

    msg.resize(hdr.len);

    res = receive_all(&msg[0], hdr.len, timeout);

    if (res == operation_result::TIMEOUT)
        return false;

    if (res == operation_result::FAIL)
        throw odbc_error(sql_state::S08S01_LINK_FAILURE, "Can not receive message body");

#ifdef PER_BYTE_DEBUG
    LOG_MSG("Message received: " << HexDump(&msg[0], msg.size()));
#endif //PER_BYTE_DEBUG

    return true;
}

sql_connection::operation_result sql_connection::receive_all(void* dst, size_t len, std::int32_t timeout)
{
    // TODO: IGNITE-19204 Implement connection operations.

    return operation_result::FAIL;
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

            // TODO: IGNITE-19204 Implement connection management.
            *val = SQL_CD_TRUE;

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
    protocol_version m_protocol_version = m_config.get_protocol_version();

    if (!m_protocol_version.is_supported())
    {
        add_status_record(sql_state::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE,
            "Protocol version is not supported: " + m_protocol_version.to_string());

        return sql_result::AI_ERROR;
    }

    handshake_request req(m_config);
    handshake_response rsp;

    try
    {
        // Workaround for some Linux systems that report connection on non-blocking
        // sockets as successful but fail to establish real connection.
        bool sent = internal_sync_message(req, rsp, m_login_timeout);

        if (!sent)
        {
            add_status_record(sql_state::S08001_CANNOT_CONNECT,
                "Failed to get handshake response (Did you forget to enable SSL?).");

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

    if (!rsp.is_accepted())
    {
        LOG_MSG("Handshake message has been rejected.");

        std::stringstream constructor;

        constructor << "Node rejected handshake message. ";

        if (!rsp.get_error().empty())
            constructor << "Additional info: " << rsp.get_error() << " ";

        constructor << "Current version of the protocol, used by the server node is "
                    << rsp.get_current_ver().to_string() << ", "
                    << "driver protocol version introduced in version "
                    << m_protocol_version.to_string() << ".";

        add_status_record(sql_state::S08004_CONNECTION_REJECTED, constructor.str());

        return sql_result::AI_ERROR;
    }

    return sql_result::AI_SUCCESS;
}

void sql_connection::ensure_connected()
{
    // TODO: IGNITE-19204 Implement connection management.
    bool success = try_restore_connection();

    if (!success)
        throw odbc_error(sql_state::S08001_CANNOT_CONNECT, "Failed to establish connection with any provided hosts");
}

bool sql_connection::try_restore_connection()
{
    // TODO: IGNITE-19204 Implement connection management.
    return false;
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
