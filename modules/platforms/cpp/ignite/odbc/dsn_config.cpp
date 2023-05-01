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

#include <ignite/common/fixed_size_array.h>

#include "config/config_tools.h"
#include "config/connection_string_parser.h"
#include "dsn_config.h"
#include "ignite/odbc/system/odbc_constants.h"
#include "utility.h"

using namespace ignite::config;

#define BUFFER_SIZE (1024 * 1024)
#define CONFIG_FILE "ODBC.INI"

namespace ignite
{
    namespace odbc
    {
        void ThrowLastSetupError()
        {
            DWORD code;
            FixedSizeArray<char> msg(BUFFER_SIZE);

            SQLInstallerError(1, &code, msg.get_data(), msg.get_size(), NULL);

            std::stringstream buf;

            buf << "Message: \"" << msg.get_data() << "\", Code: " << code;

            throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
        }

        void WriteDsnString(const char* dsn, const char* key, const char* value)
        {
            if (!SQLWritePrivateProfileString(dsn, key, value, CONFIG_FILE))
                ThrowLastSetupError();
        }

        settable_value<std::string> ReadDsnString(const char* dsn, const std::string& key, const std::string& dflt = "")
        {
            static const char* unique = "35a920dd-8837-43d2-a846-e01a2e7b5f84";

            settable_value<std::string> val(dflt);

            FixedSizeArray<char> buf(BUFFER_SIZE);

            int ret = SQLGetPrivateProfileString(dsn, key.c_str(), unique, buf.get_data(), buf.get_size(), CONFIG_FILE);

            if (ret > BUFFER_SIZE)
            {
                buf.reset(ret + 1);

                ret = SQLGetPrivateProfileString(dsn, key.c_str(), unique, buf.get_data(), buf.get_size(), CONFIG_FILE);
            }

            std::string res(buf.get_data());

            if (res != unique)
                val.set_value(res);

            return val;
        }

        settable_value<int32_t> ReadDsnInt(const char* dsn, const std::string& key, int32_t dflt = 0)
        {
            settable_value<std::string> str = ReadDsnString(dsn, key, "");

            settable_value<int32_t> res(dflt);

            if (str.is_set())
                res.set_value(lexical_cast<int, std::string>(str.get_value()));

            return res;
        }

        settable_value<bool> ReadDsnBool(const char* dsn, const std::string& key, bool dflt = false)
        {
            settable_value<std::string> str = ReadDsnString(dsn, key, "");

            settable_value<bool> res(dflt);

            if (str.is_set())
                res.set_value(str.get_value() == "true");

            return res;
        }

        void ReadDsnConfiguration(const char* dsn, configuration& config, diagnostic_record_storage* diag)
        {
            settable_value<std::string> address = ReadDsnString(dsn, ConnectionStringParser::Key::address);

            if (address.is_set() && !config.is_addresses_set())
            {
                std::vector<EndPoint> end_points;

                parse_address(address.get_value(), end_points, diag);

                config.set_addresses(end_points);
            }

            settable_value<std::string> server = ReadDsnString(dsn, ConnectionStringParser::Key::server);

            if (server.is_set() && !config.IsHostSet())
                config.SetHost(server.get_value());

            settable_value<int32_t> port = ReadDsnInt(dsn, ConnectionStringParser::Key::port);

            if (port.is_set() && !config.is_tcp_port_set())
                config.set_tcp_port(static_cast<uint16_t>(port.get_value()));

            settable_value<std::string> schema = ReadDsnString(dsn, ConnectionStringParser::Key::schema);

            if (schema.is_set() && !config.IsSchemaSet())
                config.SetSchema(schema.get_value());

            settable_value<bool> distributedJoins = ReadDsnBool(dsn, ConnectionStringParser::Key::distributedJoins);

            if (distributedJoins.is_set() && !config.IsDistributedJoinsSet())
                config.SetDistributedJoins(distributedJoins.get_value());

            settable_value<bool> enforceJoinOrder = ReadDsnBool(dsn, ConnectionStringParser::Key::enforceJoinOrder);

            if (enforceJoinOrder.is_set() && !config.IsEnforceJoinOrderSet())
                config.SetEnforceJoinOrder(enforceJoinOrder.get_value());

            settable_value<bool> replicatedOnly = ReadDsnBool(dsn, ConnectionStringParser::Key::replicatedOnly);

            if (replicatedOnly.is_set() && !config.IsReplicatedOnlySet())
                config.SetReplicatedOnly(replicatedOnly.get_value());

            settable_value<bool> collocated = ReadDsnBool(dsn, ConnectionStringParser::Key::collocated);

            if (collocated.is_set() && !config.IsCollocatedSet())
                config.SetCollocated(collocated.get_value());

            settable_value<bool> lazy = ReadDsnBool(dsn, ConnectionStringParser::Key::lazy);

            if (lazy.is_set() && !config.IsLazySet())
                config.SetLazy(lazy.get_value());

            settable_value<bool> skipReducerOnUpdate = ReadDsnBool(dsn, ConnectionStringParser::Key::skipReducerOnUpdate);

            if (skipReducerOnUpdate.is_set() && !config.IsSkipReducerOnUpdateSet())
                config.SetSkipReducerOnUpdate(skipReducerOnUpdate.get_value());

            settable_value<std::string> versionStr = ReadDsnString(dsn, ConnectionStringParser::Key::protocolVersion);

            if (versionStr.is_set() && !config.IsProtocolVersionSet())
            {
                ProtocolVersion version = ProtocolVersion::FromString(versionStr.get_value());

                if (!version.IsSupported())
                    version = configuration::default_value::protocolVersion;

                config.SetProtocolVersion(version);
            }

            settable_value<int32_t> pageSize = ReadDsnInt(dsn, ConnectionStringParser::Key::pageSize);

            if (pageSize.is_set() && !config.is_page_size_set() && pageSize.get_value() > 0)
                config.set_page_size(pageSize.get_value());

            settable_value<std::string> sslModeStr = ReadDsnString(dsn, ConnectionStringParser::Key::sslMode);

            if (sslModeStr.is_set() && !config.IsSslModeSet())
            {
                ssl::ssl_mode sslMode = ssl::ssl_mode_from_string(sslModeStr.get_value(), ssl::ssl_mode::DISABLE);

                config.SetSslMode(sslMode);
            }

            settable_value<std::string> sslKeyFile = ReadDsnString(dsn, ConnectionStringParser::Key::sslKeyFile);

            if (sslKeyFile.is_set() && !config.IsSslKeyFileSet())
                config.SetSslKeyFile(sslKeyFile.get_value());

            settable_value<std::string> sslCertFile = ReadDsnString(dsn, ConnectionStringParser::Key::sslCertFile);

            if (sslCertFile.is_set() && !config.IsSslCertFileSet())
                config.SetSslCertFile(sslCertFile.get_value());

            settable_value<std::string> sslCaFile = ReadDsnString(dsn, ConnectionStringParser::Key::sslCaFile);

            if (sslCaFile.is_set() && !config.IsSslCaFileSet())
                config.SetSslCaFile(sslCaFile.get_value());

            settable_value<std::string> user = ReadDsnString(dsn, ConnectionStringParser::Key::user);

            if (user.is_set() && !config.IsUserSet())
                config.SetUser(user.get_value());

            settable_value<std::string> password = ReadDsnString(dsn, ConnectionStringParser::Key::password);

            if (password.is_set() && !config.IsPasswordSet())
                config.SetPassword(password.get_value());

            settable_value<std::string> nestedTxModeStr = ReadDsnString(dsn, ConnectionStringParser::Key::nestedTxMode);

            if (nestedTxModeStr.is_set() && !config.IsNestedTxModeSet())
                config.SetNestedTxMode(nested_tx_mode::FromString(nestedTxModeStr.get_value(), config.GetNestedTxMode()));

            settable_value<std::string> engineModeStr = ReadDsnString(dsn, ConnectionStringParser::Key::engineMode);

            if (engineModeStr.is_set() && !config.IsEngineModeSet())
                config.SetEngineMode(EngineMode::FromString(engineModeStr.get_value(), config.GetEngineMode()));
        }
    }
}
