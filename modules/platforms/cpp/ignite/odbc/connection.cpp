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

#include <cstring>
#include <cstddef>

#include <sstream>
#include <algorithm>

#include <ignite/common/fixed_size_array.h>

#include <ignite/network/network.h>

#include "config/configuration.h"
#include "config/connection_string_parser.h"
#include "connection.h"
#include "dsn_config.h"
#include "environment.h"
#include "ignite/odbc/system/system_dsn.h"
#include "log.h"
#include "message.h"
#include "ssl_mode.h"
#include "statement.h"
#include "utility.h"

// Uncomment for per-byte debug.
//#define PER_BYTE_DEBUG

namespace
{
#pragma pack(push, 1)
    struct OdbcProtocolHeader
    {
        int32_t len;
    };
#pragma pack(pop)
}


namespace ignite
{
    namespace odbc
    {
        connection::connection(Environment* env) :
            env(env),
            socket(),
            timeout(0),
            loginTimeout(DEFAULT_CONNECT_TIMEOUT),
            autoCommit(true),
            parser(),
            config(),
            info(config),
            streamingContext()
        {
            streamingContext.SetConnection(*this);
        }

        connection::~connection()
        {
            // No-op.
        }

        const config::ConnectionInfo& connection::GetInfo() const
        {
            return info;
        }

        void connection::GetInfo(config::ConnectionInfo::InfoType type, void* buf, short buflen, short* reslen)
        {
            LOG_MSG("SQLGetInfo called: "
                << type << " ("
                << config::ConnectionInfo::InfoTypeToString(type) << "), "
                << std::hex << reinterpret_cast<size_t>(buf) << ", "
                << buflen << ", "
                << std::hex << reinterpret_cast<size_t>(reslen)
                << std::dec);

            IGNITE_ODBC_API_CALL(InternalGetInfo(type, buf, buflen, reslen));
        }

        sql_result connection::InternalGetInfo(config::ConnectionInfo::InfoType type, void* buf, short buflen, short* reslen)
        {
            const config::ConnectionInfo& info = GetInfo();

            sql_result res = info.GetInfo(type, buf, buflen, reslen);

            if (res != sql_result::AI_SUCCESS)
                add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED, "Not implemented.");

            return res;
        }

        void connection::Establish(const std::string& connectStr, void* parentWindow)
        {
            IGNITE_ODBC_API_CALL(InternalEstablish(connectStr, parentWindow));
        }

        sql_result connection::InternalEstablish(const std::string& connectStr, void* parentWindow)
        {
            config::configuration config;
            config::ConnectionStringParser parser(config);
            parser.ParseConnectionString(connectStr, &get_diagnostic_records());

            if (parentWindow)
            {
                LOG_MSG("Parent window is passed. Creating configuration window.");
                if (!DisplayConnectionWindow(parentWindow, config))
                {
                    add_status_record(sql_state::SHY008_OPERATION_CANCELED, "Connection canceled by user");

                    return sql_result::AI_ERROR;
                }
            }

            if (config.IsDsnSet())
            {
                std::string dsn = config.GetDsn();

                ReadDsnConfiguration(dsn.c_str(), config, &get_diagnostic_records());
            }

            return InternalEstablish(config);
        }

        void connection::Establish(const config::configuration& cfg)
        {
            IGNITE_ODBC_API_CALL(InternalEstablish(cfg));
        }

        sql_result connection::InitSocket()
        {
            ssl::ssl_mode sslMode = config.GetSslMode();

            if (sslMode == ssl::ssl_mode::DISABLE)
            {
                socket.reset(network::MakeTcpSocketClient());

                return sql_result::AI_SUCCESS;
            }

            try
            {
                network::ssl::EnsureSslLoaded();
            }
            catch (const IgniteError &err)
            {
                LOG_MSG("Can not load OpenSSL library: " << err.GetText());

                add_status_record("Can not load OpenSSL library (did you set OPENSSL_HOME environment variable?)");

                return sql_result::AI_ERROR;
            }

            network::ssl::SecureConfiguration sslCfg;
            sslCfg.certPath = config.GetSslCertFile();
            sslCfg.keyPath = config.GetSslKeyFile();
            sslCfg.caPath = config.GetSslCaFile();

            socket.reset(network::ssl::MakeSecureSocketClient(sslCfg));

            return sql_result::AI_SUCCESS;
        }

        sql_result connection::InternalEstablish(const config::configuration& cfg)
        {
            using ssl::ssl_mode;

            config = cfg;

            if (socket.get() != 0)
            {
                add_status_record(sql_state::S08002_ALREADY_CONNECTED, "Already connected.");

                return sql_result::AI_ERROR;
            }

            if (!config.IsHostSet() && config.is_addresses_set() && config.get_addresses().empty())
            {
                add_status_record("No valid address to connect.");

                return sql_result::AI_ERROR;
            }

            bool connected = TryRestoreConnection();

            if (!connected)
            {
                add_status_record(sql_state::S08001_CANNOT_CONNECT, "Failed to establish connection with the host.");

                return sql_result::AI_ERROR;
            }

            bool errors = get_diagnostic_records().get_status_records_number() > 0;

            return errors ? sql_result::AI_SUCCESS_WITH_INFO : sql_result::AI_SUCCESS;
        }

        void connection::Release()
        {
            IGNITE_ODBC_API_CALL(InternalRelease());
        }

        void connection::Deregister()
        {
            env->DeregisterConnection(this);
        }

        sql_result connection::InternalRelease()
        {
            if (socket.get() == 0)
            {
                add_status_record(sql_state::S08003_NOT_CONNECTED, "Connection is not open.");

                // It is important to return SUCCESS_WITH_INFO and not ERROR here, as if we return an error, Windows
                // Driver Manager may decide that connection is not valid anymore which results in memory leak.
                return sql_result::AI_SUCCESS_WITH_INFO;
            }

            Close();

            return sql_result::AI_SUCCESS;
        }

        void connection::Close()
        {
            if (socket.get() != 0)
            {
                socket->Close();

                socket.reset();
            }
        }

        Statement* connection::CreateStatement()
        {
            Statement* statement;

            IGNITE_ODBC_API_CALL(InternalCreateStatement(statement));

            return statement;
        }

        sql_result connection::InternalCreateStatement(Statement*& statement)
        {
            statement = new Statement(*this);

            if (!statement)
            {
                add_status_record(sql_state::SHY001_MEMORY_ALLOCATION, "Not enough memory.");

                return sql_result::AI_ERROR;
            }

            return sql_result::AI_SUCCESS;
        }

        bool connection::Send(const int8_t* data, size_t len, int32_t timeout)
        {
            if (socket.get() == 0)
                throw odbc_error(sql_state::S08003_NOT_CONNECTED, "Connection is not established");

            int32_t newLen = static_cast<int32_t>(len + sizeof(OdbcProtocolHeader));

            FixedSizeArray<int8_t> msg(newLen);

            OdbcProtocolHeader *hdr = reinterpret_cast<OdbcProtocolHeader*>(msg.get_data());

            hdr->len = static_cast<int32_t>(len);

            memcpy(msg.get_data() + sizeof(OdbcProtocolHeader), data, len);

            OperationResult::T res = SendAll(msg.get_data(), msg.get_size(), timeout);

            if (res == OperationResult::TIMEOUT)
                return false;

            if (res == OperationResult::FAIL)
                throw odbc_error(sql_state::S08S01_LINK_FAILURE, "Can not send message due to connection failure");

#ifdef PER_BYTE_DEBUG
            LOG_MSG("message sent: (" <<  msg.get_size() << " bytes)" << HexDump(msg.get_data(), msg.get_size()));
#endif //PER_BYTE_DEBUG

            return true;
        }

        connection::OperationResult::T connection::SendAll(const int8_t* data, size_t len, int32_t timeout)
        {
            int sent = 0;

            while (sent != static_cast<int64_t>(len))
            {
                int res = socket->Send(data + sent, len - sent, timeout);

                LOG_MSG("Sent: " << res);

                if (res < 0 || res == network::SocketClient::WaitResult::TIMEOUT)
                {
                    Close();

                    return res < 0 ? OperationResult::FAIL : OperationResult::TIMEOUT;
                }

                sent += res;
            }

            assert(static_cast<size_t>(sent) == len);

            return OperationResult::SUCCESS;
        }

        bool connection::Receive(std::vector<int8_t>& msg, int32_t timeout)
        {
            if (socket.get() == 0)
                throw odbc_error(sql_state::S08003_NOT_CONNECTED, "Connection is not established");

            msg.clear();

            OdbcProtocolHeader hdr;

            OperationResult::T res = ReceiveAll(reinterpret_cast<int8_t*>(&hdr), sizeof(hdr), timeout);

            if (res == OperationResult::TIMEOUT)
                return false;

            if (res == OperationResult::FAIL)
                throw odbc_error(sql_state::S08S01_LINK_FAILURE, "Can not receive message header");

            if (hdr.len < 0)
            {
                Close();

                throw odbc_error(sql_state::SHY000_GENERAL_ERROR, "Protocol error: Message length is negative");
            }

            if (hdr.len == 0)
                return false;

            msg.resize(hdr.len);

            res = ReceiveAll(&msg[0], hdr.len, timeout);

            if (res == OperationResult::TIMEOUT)
                return false;

            if (res == OperationResult::FAIL)
                throw odbc_error(sql_state::S08S01_LINK_FAILURE, "Can not receive message body");

#ifdef PER_BYTE_DEBUG
            LOG_MSG("Message received: " << HexDump(&msg[0], msg.size()));
#endif //PER_BYTE_DEBUG

            return true;
        }

        connection::OperationResult::T connection::ReceiveAll(void* dst, size_t len, int32_t timeout)
        {
            size_t remain = len;
            int8_t* buffer = reinterpret_cast<int8_t*>(dst);

            while (remain)
            {
                size_t received = len - remain;

                int res = socket->Receive(buffer + received, remain, timeout);
                LOG_MSG("Receive res: " << res << " remain: " << remain);

                if (res < 0 || res == network::SocketClient::WaitResult::TIMEOUT)
                {
                    Close();

                    return res < 0 ? OperationResult::FAIL : OperationResult::TIMEOUT;
                }

                remain -= static_cast<size_t>(res);
            }

            return OperationResult::SUCCESS;
        }

        const std::string& connection::GetSchema() const
        {
            return config.GetSchema();
        }

        const config::configuration& connection::GetConfiguration() const
        {
            return config;
        }

        bool connection::IsAutoCommit() const
        {
            return autoCommit;
        }

        diagnostic_record connection::CreateStatusRecord(sql_state sql_state,
            const std::string& message, int32_t row_num, int32_t column_num)
        {
            return diagnostic_record(sql_state, message, "", "", row_num, column_num);
        }

        void connection::TransactionCommit()
        {
            IGNITE_ODBC_API_CALL(InternalTransactionCommit());
        }

        sql_result connection::InternalTransactionCommit()
        {
            std::string schema = config.GetSchema();

            parameter_set empty;

            QueryExecuteRequest req(schema, "COMMIT", empty, timeout, autoCommit);
            QueryExecuteResponse rsp;

            try
            {
                bool sent = SyncMessage(req, rsp, timeout);

                if (!sent)
                {
                    add_status_record(sql_state::S08S01_LINK_FAILURE, "Failed to send commit request.");

                    return sql_result::AI_ERROR;
                }
            }
            catch (const odbc_error& err)
            {
                add_status_record(err);

                return sql_result::AI_ERROR;
            }
            catch (const IgniteError& err)
            {
                add_status_record(err.GetText());

                return sql_result::AI_ERROR;
            }

            return sql_result::AI_SUCCESS;
        }

        void connection::TransactionRollback()
        {
            IGNITE_ODBC_API_CALL(InternalTransactionRollback());
        }

        sql_result connection::InternalTransactionRollback()
        {
            std::string schema = config.GetSchema();

            parameter_set empty;

            QueryExecuteRequest req(schema, "ROLLBACK", empty, timeout, autoCommit);
            QueryExecuteResponse rsp;

            try
            {
                bool sent = SyncMessage(req, rsp, timeout);

                if (!sent)
                {
                    add_status_record(sql_state::S08S01_LINK_FAILURE, "Failed to send rollback request.");

                    return sql_result::AI_ERROR;
                }
            }
            catch (const odbc_error& err)
            {
                add_status_record(err);

                return sql_result::AI_ERROR;
            }
            catch (const IgniteError& err)
            {
                add_status_record(err.GetText());

                return sql_result::AI_ERROR;
            }

            return sql_result::AI_SUCCESS;
        }

        void connection::GetAttribute(int attr, void* buf, SQLINTEGER bufLen, SQLINTEGER* valueLen)
        {
            IGNITE_ODBC_API_CALL(InternalGetAttribute(attr, buf, bufLen, valueLen));
        }

        sql_result connection::InternalGetAttribute(int attr, void* buf, SQLINTEGER, SQLINTEGER* valueLen)
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
                    SQLUINTEGER *val = reinterpret_cast<SQLUINTEGER*>(buf);

                    *val = socket.get() != 0 ? SQL_CD_FALSE : SQL_CD_TRUE;

                    if (valueLen)
                        *valueLen = SQL_IS_INTEGER;

                    break;
                }

                case SQL_ATTR_CONNECTION_TIMEOUT:
                {
                    SQLUINTEGER *val = reinterpret_cast<SQLUINTEGER*>(buf);

                    *val = static_cast<SQLUINTEGER>(timeout);

                    if (valueLen)
                        *valueLen = SQL_IS_INTEGER;

                    break;
                }

                case SQL_ATTR_LOGIN_TIMEOUT:
                {
                    SQLUINTEGER *val = reinterpret_cast<SQLUINTEGER*>(buf);

                    *val = static_cast<SQLUINTEGER>(loginTimeout);

                    if (valueLen)
                        *valueLen = SQL_IS_INTEGER;

                    break;
                }

                case SQL_ATTR_AUTOCOMMIT:
                {
                    SQLUINTEGER *val = reinterpret_cast<SQLUINTEGER*>(buf);

                    *val = autoCommit ? SQL_AUTOCOMMIT_ON : SQL_AUTOCOMMIT_OFF;

                    if (valueLen)
                        *valueLen = SQL_IS_INTEGER;

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

        void connection::SetAttribute(int attr, void* value, SQLINTEGER valueLen)
        {
            IGNITE_ODBC_API_CALL(InternalSetAttribute(attr, value, valueLen));
        }

        sql_result connection::InternalSetAttribute(int attr, void* value, SQLINTEGER)
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
                    timeout = RetrieveTimeout(value);

                    if (get_diagnostic_records().get_status_records_number() != 0)
                        return sql_result::AI_SUCCESS_WITH_INFO;

                    break;
                }

                case SQL_ATTR_LOGIN_TIMEOUT:
                {
                    loginTimeout = RetrieveTimeout(value);

                    if (get_diagnostic_records().get_status_records_number() != 0)
                        return sql_result::AI_SUCCESS_WITH_INFO;

                    break;
                }

                case SQL_ATTR_AUTOCOMMIT:
                {
                    SQLUINTEGER mode = static_cast<SQLUINTEGER>(reinterpret_cast<ptrdiff_t>(value));

                    if (mode != SQL_AUTOCOMMIT_ON && mode != SQL_AUTOCOMMIT_OFF)
                    {
                        add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
                            "Specified attribute is not supported.");

                        return sql_result::AI_ERROR;
                    }

                    autoCommit = mode == SQL_AUTOCOMMIT_ON;

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

        sql_result connection::MakeRequestHandshake()
        {
            ProtocolVersion protocolVersion = config.GetProtocolVersion();

            if (!protocolVersion.IsSupported())
            {
                add_status_record(sql_state::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE,
                    "Protocol version is not supported: " + protocolVersion.ToString());

                return sql_result::AI_ERROR;
            }

            if (protocolVersion < ProtocolVersion::VERSION_2_5_0 && !config.GetUser().empty())
            {
                add_status_record(sql_state::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE,
                    "Authentication is not allowed for protocol version below 2.5.0");

                return sql_result::AI_ERROR;
            }

            HandshakeRequest req(config);
            HandshakeResponse rsp;

            try
            {
                // Workaround for some Linux systems that report connection on non-blocking
                // sockets as successful but fail to establish real connection.
                bool sent = InternalSyncMessage(req, rsp, loginTimeout);

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
            catch (const IgniteError& err)
            {
                add_status_record(sql_state::S08004_CONNECTION_REJECTED, err.GetText());

                return sql_result::AI_ERROR;
            }

            if (!rsp.IsAccepted())
            {
                LOG_MSG("Handshake message has been rejected.");

                std::stringstream constructor;

                constructor << "Node rejected handshake message. ";

                if (!rsp.GetError().empty())
                    constructor << "Additional info: " << rsp.GetError() << " ";

                constructor << "Current version of the protocol, used by the server node is "
                            << rsp.GetCurrentVer().ToString() << ", "
                            << "driver protocol version introduced in version "
                            << protocolVersion.ToString() << ".";

                add_status_record(sql_state::S08004_CONNECTION_REJECTED, constructor.str());

                return sql_result::AI_ERROR;
            }

            return sql_result::AI_SUCCESS;
        }

        void connection::EnsureConnected()
        {
            if (socket.get() != 0)
                return;

            bool success = TryRestoreConnection();

            if (!success)
                throw odbc_error(sql_state::S08001_CANNOT_CONNECT,
                    "Failed to establish connection with any provided hosts");
        }

        bool connection::TryRestoreConnection()
        {
            std::vector<EndPoint> addrs;

            CollectAddresses(config, addrs);

            if (socket.get() == 0)
            {
                sql_result res = InitSocket();

                if (res != sql_result::AI_SUCCESS)
                    return false;
            }

            bool connected = false;

            while (!addrs.empty() && !connected)
            {
                const EndPoint& addr = addrs.back();

                for (uint16_t port = addr.port; port <= addr.port + addr.range; ++port)
                {
                    try
                    {
                        connected = socket->Connect(addr.host.c_str(), port, loginTimeout);
                    }
                    catch (const IgniteError& err)
                    {
                        LOG_MSG("Error while trying connect to " << addr.host << ":" << addr.port <<", " << err.GetText());
                    }

                    if (connected)
                    {
                        sql_result res = MakeRequestHandshake();

                        connected = res != sql_result::AI_ERROR;

                        if (connected)
                            break;
                    }
                }

                addrs.pop_back();
            }

            if (!connected)
                Close();
            else
                parser.SetProtocolVersion(config.GetProtocolVersion());

            return connected;
        }

        void connection::CollectAddresses(const config::configuration& cfg, std::vector<EndPoint>& end_points)
        {
            end_points.clear();

            if (!cfg.is_addresses_set())
            {
                LOG_MSG("'Address' is not set. Using legacy connection method.");

                end_points.push_back(EndPoint(cfg.GetHost(), cfg.get_tcp_port()));

                return;
            }

            end_points = cfg.get_addresses();

            std::random_shuffle(end_points.begin(), end_points.end());
        }

        int32_t connection::RetrieveTimeout(void* value)
        {
            SQLUINTEGER uTimeout = static_cast<SQLUINTEGER>(reinterpret_cast<ptrdiff_t>(value));

            if (uTimeout != 0 && socket.get() != 0 && socket->IsBlocking())
            {
                add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, "Can not set timeout, because can not "
                    "enable non-blocking mode on TCP connection. Setting to 0.");

                return 0;
            }

            if (uTimeout > INT32_MAX)
            {
                std::stringstream ss;

                ss << "Value is too big: " << uTimeout << ", changing to " << timeout << ".";

                add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED, ss.str());

                return INT32_MAX;
            }

            return static_cast<int32_t>(uTimeout);
        }
    }
}

