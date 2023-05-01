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

#include "../../log.h"
#include "../../utility.h"
#include "ignite/odbc/system/odbc_constants.h"

#include "../../config/connection_string_parser.h"
#include "../../diagnostic/diagnosable_adapter.h"
#include "../../dsn_config.h"
#include "../../odbc_error.h"
#include "ignite/odbc/system/ui/dsn_configuration_window.h"
#include "ui/window.h"

using ignite::config::configuration;

bool DisplayConnectionWindow(void* windowParent, configuration& config)
{
    using namespace ignite::system::ui;

    HWND hwndParent = (HWND) windowParent;

    if (!hwndParent)
        return true;

    try
    {
        Window parent(hwndParent);

        DsnConfigurationWindow window(&parent, config);

        window.Create();

        window.Show();
        window.Update();

        return ProcessMessages(window) == Result::OK;
    }
    catch (const ignite::IgniteError& err)
    {
        std::stringstream buf;

        buf << "Message: " << err.GetText() << ", Code: " << err.GetCode();

        std::string message = buf.str();

        MessageBox(NULL, message.c_str(), "Error!", MB_ICONEXCLAMATION | MB_OK);

        SQLPostInstallerError(err.GetCode(), err.GetText());
    }

    return false;
}

/**
 * Register DSN with specified configuration.
 *
 * @param config Configuration.
 * @param driver Driver.
 * @return True on success and false on fail.
 */
bool RegisterDsn(const configuration& config, LPCSTR driver)
{
    using namespace ignite::config;
    using ignite::lexical_cast;

    typedef configuration::argument_map ArgMap;

    const char* dsn = config.GetDsn().c_str();

    try
    {
        if (!SQLWriteDSNToIni(dsn, driver))
            ignite::ThrowLastSetupError();

        ArgMap map;
        
        config.to_map(map);

        map.erase(ConnectionStringParser::Key::dsn);
        map.erase(ConnectionStringParser::Key::driver);

        for (ArgMap::const_iterator it = map.begin(); it != map.end(); ++it)
        {
            const std::string& key = it->first;
            const std::string& value = it->second;

            ignite::WriteDsnString(dsn, key.c_str(), value.c_str());
        }

        return true;
    }
    catch (ignite::IgniteError& err)
    {
        MessageBox(NULL, err.GetText(), "Error!", MB_ICONEXCLAMATION | MB_OK);

        SQLPostInstallerError(err.GetCode(), err.GetText());
    }

    return false;
}

/**
 * Unregister specified DSN.
 *
 * @param dsn DSN name.
 * @return True on success and false on fail.
 */
bool UnregisterDsn(const char* dsn)
{
    try
    {
        if (!SQLRemoveDSNFromIni(dsn))
            ignite::ThrowLastSetupError();

        return true;
    }
    catch (ignite::IgniteError& err)
    {
        MessageBox(NULL, err.GetText(), "Error!", MB_ICONEXCLAMATION | MB_OK);

        SQLPostInstallerError(err.GetCode(), err.GetText());
    }

    return false;
}

BOOL INSTAPI ConfigDSN(HWND hwndParent, WORD req, LPCSTR driver, LPCSTR attributes)
{
    using namespace ignite::odbc;

    LOG_MSG("ConfigDSN called");

    configuration config;

    LOG_MSG("Attributes: " << attributes);

    config::ConnectionStringParser parser(config);

    diagnostic_record_storage diag;

    parser.ParseConfigAttributes(attributes, &diag);

    if (!SQLValidDSN(config.GetDsn().c_str()))
        return FALSE;

    LOG_MSG("Driver: " << driver);
    LOG_MSG("DSN: " << config.GetDsn());

    switch (req)
    {
        case ODBC_ADD_DSN:
        {
            LOG_MSG("ODBC_ADD_DSN");

            if (!DisplayConnectionWindow(hwndParent, config))
                return FALSE;

            if (!RegisterDsn(config, driver))
                return FALSE;

            break;
        }

        case ODBC_CONFIG_DSN:
        {
            LOG_MSG("ODBC_CONFIG_DSN");

            std::string dsn = config.GetDsn();

            configuration loaded(config);

            ReadDsnConfiguration(dsn.c_str(), loaded, &diag);

            if (!DisplayConnectionWindow(hwndParent, loaded))
                return FALSE;

            if (!RegisterDsn(loaded, driver))
                return FALSE;

            if (loaded.GetDsn() != dsn && !UnregisterDsn(dsn.c_str()))
                return FALSE;

            break;
        }

        case ODBC_REMOVE_DSN:
        {
            LOG_MSG("ODBC_REMOVE_DSN");

            if (!UnregisterDsn(config.GetDsn().c_str()))
                return FALSE;

            break;
        }

        default:
            return FALSE;
    }

    return TRUE;
}
