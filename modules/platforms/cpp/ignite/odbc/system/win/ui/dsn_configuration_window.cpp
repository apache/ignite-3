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

#include <Shlwapi.h>
#include <windowsx.h>

#include "../../../log.h"
#include "../../../ssl_mode.h"

#include "../../../config/config_tools.h"
#include "../../../diagnostic/diagnosable_adapter.h"
#include "ignite/odbc/system/ui/dsn_configuration_window.h"

namespace ignite
{
    namespace odbc
    {
        namespace system
        {
            namespace ui
            {
                DsnConfigurationWindow::DsnConfigurationWindow(Window* parent, config::configuration& config):
                    CustomWindow(parent, "IgniteConfigureDsn", "Configure Apache Ignite DSN"),
                    width(360),
                    height(620),
                    connectionSettingsGroupBox(),
                    sslSettingsGroupBox(),
                    authSettingsGroupBox(),
                    additionalSettingsGroupBox(),
                    nameLabel(),
                    nameEdit(),
                    addressLabel(),
                    addressEdit(),
                    schemaLabel(),
                    schemaEdit(),
                    pageSizeLabel(),
                    pageSizeEdit(),
                    distributedJoinsCheckBox(),
                    enforceJoinOrderCheckBox(),
                    replicatedOnlyCheckBox(),
                    collocatedCheckBox(),
                    protocolVersionLabel(),
                    protocolVersionComboBox(),
                    userLabel(),
                    userEdit(),
                    passwordLabel(),
                    passwordEdit(),
                    engineModeComboBox(),
                    nestedTxModeComboBox(),
                    okButton(),
                    cancelButton(),
                    config(config),
                    accepted(false)
                {
                    // No-op.
                }

                DsnConfigurationWindow::~DsnConfigurationWindow()
                {
                    // No-op.
                }

                void DsnConfigurationWindow::Create()
                {
                    // Finding out parent position.
                    RECT parentRect;
                    GetWindowRect(parent->GetHandle(), &parentRect);

                    // Positioning window to the center of parent window.
                    const int posX = parentRect.left + (parentRect.right - parentRect.left - width) / 2;
                    const int posY = parentRect.top + (parentRect.bottom - parentRect.top - height) / 2;

                    RECT desiredRect = {posX, posY, posX + width, posY + height};
                    AdjustWindowRect(&desiredRect, WS_BORDER | WS_CAPTION | WS_SYSMENU | WS_THICKFRAME, FALSE);

                    Window::Create(WS_OVERLAPPED | WS_SYSMENU, desiredRect.left, desiredRect.top,
                        desiredRect.right - desiredRect.left, desiredRect.bottom - desiredRect.top, 0);

                    if (!handle)
                    {
                        std::stringstream buf;

                        buf << "Can not create window, error code: " << GetLastError();

                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, buf.str().c_str());
                    }
                }

                void DsnConfigurationWindow::OnCreate()
                {
                    int groupPosY = MARGIN;
                    int groupSizeY = width - 2 * MARGIN;

                    groupPosY += INTERVAL + CreateConnectionSettingsGroup(MARGIN, groupPosY, groupSizeY);
                    groupPosY += INTERVAL + CreateAuthSettingsGroup(MARGIN, groupPosY, groupSizeY);
                    groupPosY += INTERVAL + CreateSslSettingsGroup(MARGIN, groupPosY, groupSizeY);
                    groupPosY += INTERVAL + CreateAdditionalSettingsGroup(MARGIN, groupPosY, groupSizeY);

                    int cancelPosX = width - MARGIN - BUTTON_WIDTH;
                    int okPosX = cancelPosX - INTERVAL - BUTTON_WIDTH;

                    okButton = CreateButton(okPosX, groupPosY, BUTTON_WIDTH, BUTTON_HEIGHT, "Ok", ChildId::OK_BUTTON);
                    cancelButton = CreateButton(cancelPosX, groupPosY, BUTTON_WIDTH, BUTTON_HEIGHT,
                        "Cancel", ChildId::CANCEL_BUTTON);
                }

                int DsnConfigurationWindow::CreateConnectionSettingsGroup(int posX, int posY, int sizeX)
                {
                    enum { LABEL_WIDTH = 100 };

                    int labelPosX = posX + INTERVAL;

                    int editSizeX = sizeX - LABEL_WIDTH - 3 * INTERVAL;
                    int editPosX = labelPosX + LABEL_WIDTH + INTERVAL;

                    int rowPos = posY + 2 * INTERVAL;

                    const char* val = config.GetDsn().c_str();
                    nameLabel = CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                        "Data Source Name:", ChildId::NAME_LABEL);
                    nameEdit = CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT, val, ChildId::NAME_EDIT);

                    rowPos += INTERVAL + ROW_HEIGHT;

                    std::string addr = config::addresses_to_string(config.get_addresses());

                    val = addr.c_str();
                    addressLabel = CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                        "Address:", ChildId::ADDRESS_LABEL);
                    addressEdit = CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT, val, ChildId::ADDRESS_EDIT);

                    rowPos += INTERVAL + ROW_HEIGHT;

                    val = config.GetSchema().c_str();
                    schemaLabel = CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                        "Schema name:", ChildId::SCHEMA_LABEL);
                    schemaEdit = CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT, val, ChildId::SCHEMA_EDIT);

                    rowPos += INTERVAL + ROW_HEIGHT;

                    protocolVersionLabel = CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                        "Protocol version:", ChildId::PROTOCOL_VERSION_LABEL);
                    protocolVersionComboBox = CreateComboBox(editPosX, rowPos, editSizeX, ROW_HEIGHT,
                        "Protocol version", ChildId::PROTOCOL_VERSION_COMBO_BOX);

                    int id = 0;

                    const protocol_version::version_set& supported = protocol_version::get_supported();

                    protocol_version version = config.GetProtocolVersion();

                    if (!version.is_supported())
                        version = protocol_version::get_current();

                    for (protocol_version::version_set::const_iterator it = supported.begin(); it != supported.end(); ++it)
                    {
                        protocolVersionComboBox->AddString(it->to_string());

                        if (*it == version)
                            protocolVersionComboBox->SetSelection(id);

                        ++id;
                    }

                    rowPos += INTERVAL + ROW_HEIGHT;

                    connectionSettingsGroupBox = CreateGroupBox(posX, posY, sizeX, rowPos - posY,
                        "Connection settings", ChildId::CONNECTION_SETTINGS_GROUP_BOX);

                    return rowPos - posY;
                }

                int DsnConfigurationWindow::CreateAuthSettingsGroup(int posX, int posY, int sizeX)
                {
                    enum { LABEL_WIDTH = 120 };

                    int labelPosX = posX + INTERVAL;

                    int editSizeX = sizeX - LABEL_WIDTH - 3 * INTERVAL;
                    int editPosX = labelPosX + LABEL_WIDTH + INTERVAL;

                    int rowPos = posY + 2 * INTERVAL;

                    const char* val = config.GetUser().c_str();

                    userLabel = CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT, "User :", ChildId::USER_LABEL);
                    userEdit = CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT, val, ChildId::USER_EDIT);

                    rowPos += INTERVAL + ROW_HEIGHT;

                    val = config.GetPassword().c_str();
                    passwordLabel = CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                        "Password:", ChildId::PASSWORD_LABEL);
                    passwordEdit = CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT,
                        val, ChildId::USER_EDIT, ES_PASSWORD);

                    rowPos += INTERVAL + ROW_HEIGHT;

                    authSettingsGroupBox = CreateGroupBox(posX, posY, sizeX, rowPos - posY,
                        "Authentication settings", ChildId::AUTH_SETTINGS_GROUP_BOX);

                    return rowPos - posY;
                }

                int DsnConfigurationWindow::CreateSslSettingsGroup(int posX, int posY, int sizeX)
                {
                    using ssl::ssl_mode;

                    enum { LABEL_WIDTH = 120 };

                    int labelPosX = posX + INTERVAL;

                    int editSizeX = sizeX - LABEL_WIDTH - 3 * INTERVAL;
                    int editPosX = labelPosX + LABEL_WIDTH + INTERVAL;

                    int rowPos = posY + 2 * INTERVAL;

                    ssl_mode sslMode = config.GetSslMode();
                    std::string sslModeStr = ssl_mode_to_string(sslMode);

                    const char* val = sslModeStr.c_str();

                    sslModeLabel = CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                        "SSL Mode:", ChildId::SSL_MODE_LABEL);
                    sslModeComboBox = CreateComboBox(editPosX, rowPos, editSizeX, ROW_HEIGHT,
                        "", ChildId::SSL_MODE_COMBO_BOX);

                    sslModeComboBox->AddString("disable");
                    sslModeComboBox->AddString("require");

                    sslModeComboBox->SetSelection(sslMode);

                    rowPos += INTERVAL + ROW_HEIGHT;

                    val = config.GetSslKeyFile().c_str();
                    sslKeyFileLabel = CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                        "SSL Private Key:", ChildId::SSL_KEY_FILE_LABEL);
                    sslKeyFileEdit = CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT,
                        val, ChildId::SSL_KEY_FILE_EDIT);

                    SHAutoComplete(sslKeyFileEdit->GetHandle(), SHACF_DEFAULT);

                    rowPos += INTERVAL + ROW_HEIGHT;

                    val = config.GetSslCertFile().c_str();
                    sslCertFileLabel = CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                        "SSL Certificate:", ChildId::SSL_CERT_FILE_LABEL);
                    sslCertFileEdit = CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT,
                        val, ChildId::SSL_CERT_FILE_EDIT);

                    SHAutoComplete(sslCertFileEdit->GetHandle(), SHACF_DEFAULT);

                    rowPos += INTERVAL + ROW_HEIGHT;

                    val = config.GetSslCaFile().c_str();
                    sslCaFileLabel = CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                        "SSL Certificate Authority:", ChildId::SSL_CA_FILE_LABEL);
                    sslCaFileEdit = CreateEdit(editPosX, rowPos, editSizeX, ROW_HEIGHT,
                        val, ChildId::SSL_CA_FILE_EDIT);

                    SHAutoComplete(sslCaFileEdit->GetHandle(), SHACF_DEFAULT);

                    rowPos += INTERVAL + ROW_HEIGHT;

                    sslSettingsGroupBox = CreateGroupBox(posX, posY, sizeX, rowPos - posY,
                        "SSL settings", ChildId::SSL_SETTINGS_GROUP_BOX);

                    sslKeyFileEdit->SetEnabled(sslMode != ssl_mode::DISABLE);
                    sslCertFileEdit->SetEnabled(sslMode != ssl_mode::DISABLE);
                    sslCaFileEdit->SetEnabled(sslMode != ssl_mode::DISABLE);

                    return rowPos - posY;
                }

                int DsnConfigurationWindow::CreateAdditionalSettingsGroup(int posX, int posY, int sizeX)
                {
                    enum { LABEL_WIDTH = 130 };

                    int labelPosX = posX + INTERVAL;

                    int editSizeX = sizeX - LABEL_WIDTH - 3 * INTERVAL;
                    int editPosX = labelPosX + LABEL_WIDTH + INTERVAL;

                    int checkBoxSize = (sizeX - 3 * INTERVAL) / 2;

                    protocol_version version = config.GetProtocolVersion();

                    if (!version.is_supported())
                        version = protocol_version::get_current();

                    int rowPos = posY + 2 * INTERVAL;

                    std::string tmp = lexical_cast<std::string>(config.get_page_size());
                    const char* val = tmp.c_str();
                    pageSizeLabel = CreateLabel(labelPosX, rowPos, LABEL_WIDTH,
                        ROW_HEIGHT, "Page size:", ChildId::PAGE_SIZE_LABEL);

                    pageSizeEdit = CreateEdit(editPosX, rowPos, editSizeX,
                        ROW_HEIGHT, val, ChildId::PAGE_SIZE_EDIT, ES_NUMBER);

                    rowPos += INTERVAL + ROW_HEIGHT;

                    engineModeLabel = CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                                                  "SQL query engine:", ChildId::ENGINE_MODE_LABEL);
                    engineModeComboBox = CreateComboBox(editPosX, rowPos, editSizeX, ROW_HEIGHT,
                                                        "", ChildId::ENGINE_MODE_COMBO_BOX);
                    {
                        int id = 0;

                        const EngineMode::ModeSet &supported = EngineMode::GetValidValues();

                        for (EngineMode::ModeSet::const_iterator it = supported.begin(); it != supported.end(); ++it) {
                            engineModeComboBox->AddString(EngineMode::to_string(*it));

                            if (*it == config.GetEngineMode())
                                engineModeComboBox->SetSelection(id);

                            ++id;
                        }
                    }

                    engineModeComboBox->SetEnabled(version >= protocol_version::VERSION_2_13_0);

                    rowPos += INTERVAL + ROW_HEIGHT;

                    nestedTxModeLabel = CreateLabel(labelPosX, rowPos, LABEL_WIDTH, ROW_HEIGHT,
                        "Nested Transaction Mode:", ChildId::NESTED_TX_MODE_LABEL);
                    nestedTxModeComboBox = CreateComboBox(editPosX, rowPos, editSizeX, ROW_HEIGHT,
                        "", ChildId::NESTED_TX_MODE_COMBO_BOX);

                    {
                        int id = 0;

                        const nested_tx_mode::ModeSet &supported = nested_tx_mode::GetValidValues();

                        for (nested_tx_mode::ModeSet::const_iterator it = supported.begin();
                             it != supported.end(); ++it) {
                            nestedTxModeComboBox->AddString(nested_tx_mode::to_string(*it));

                            if (*it == config.GetNestedTxMode())
                                nestedTxModeComboBox->SetSelection(id);

                            ++id;
                        }
                    }

                    nestedTxModeComboBox->SetEnabled(version >= protocol_version::VERSION_2_5_0);

                    rowPos += INTERVAL + ROW_HEIGHT;

                    distributedJoinsCheckBox = CreateCheckBox(labelPosX, rowPos, checkBoxSize, ROW_HEIGHT,
                        "Distributed Joins", ChildId::DISTRIBUTED_JOINS_CHECK_BOX, config.IsDistributedJoins());

                    enforceJoinOrderCheckBox = CreateCheckBox(labelPosX + checkBoxSize + INTERVAL,
                        rowPos, checkBoxSize, ROW_HEIGHT, "Enforce Join Order",
                        ChildId::ENFORCE_JOIN_ORDER_CHECK_BOX, config.IsEnforceJoinOrder());

                    rowPos += ROW_HEIGHT;

                    replicatedOnlyCheckBox = CreateCheckBox(labelPosX, rowPos, checkBoxSize, ROW_HEIGHT,
                        "Replicated Only", ChildId::REPLICATED_ONLY_CHECK_BOX, config.IsReplicatedOnly());

                    collocatedCheckBox = CreateCheckBox(labelPosX + checkBoxSize + INTERVAL, rowPos, checkBoxSize,
                        ROW_HEIGHT, "Collocated", ChildId::COLLOCATED_CHECK_BOX, config.IsCollocated());

                    rowPos += ROW_HEIGHT;

                    lazyCheckBox = CreateCheckBox(labelPosX, rowPos, checkBoxSize, ROW_HEIGHT,
                        "Lazy", ChildId::LAZY_CHECK_BOX, config.IsLazy());

                    lazyCheckBox->SetEnabled(version >= protocol_version::VERSION_2_1_5);

                    skipReducerOnUpdateCheckBox = CreateCheckBox(labelPosX + checkBoxSize + INTERVAL, rowPos,
                        checkBoxSize, ROW_HEIGHT, "Skip reducer on update", ChildId::SKIP_REDUCER_ON_UPDATE_CHECK_BOX,
                        config.IsSkipReducerOnUpdate());

                    skipReducerOnUpdateCheckBox->SetEnabled(version >= protocol_version::VERSION_2_3_0);

                    rowPos += ROW_HEIGHT + INTERVAL;

                    additionalSettingsGroupBox = CreateGroupBox(posX, posY, sizeX, rowPos - posY,
                        "Additional settings", ChildId::ADDITIONAL_SETTINGS_GROUP_BOX);

                    return rowPos - posY;
                }

                bool DsnConfigurationWindow::OnMessage(UINT msg, WPARAM wParam, LPARAM lParam)
                {
                    switch (msg)
                    {
                        case WM_COMMAND:
                        {
                            switch (LOWORD(wParam))
                            {
                                case ChildId::OK_BUTTON:
                                {
                                    try
                                    {
                                        RetrieveParameters(config);

                                        accepted = true;

                                        PostMessage(GetHandle(), WM_CLOSE, 0, 0);
                                    }
                                    catch (IgniteError& err)
                                    {
                                        MessageBox(NULL, err.GetText(), "Error!", MB_ICONEXCLAMATION | MB_OK);
                                    }

                                    break;
                                }

                                case IDCANCEL:
                                case ChildId::CANCEL_BUTTON:
                                {
                                    PostMessage(GetHandle(), WM_CLOSE, 0, 0);

                                    break;
                                }

                                case ChildId::DISTRIBUTED_JOINS_CHECK_BOX:
                                {
                                    distributedJoinsCheckBox->SetChecked(!distributedJoinsCheckBox->IsChecked());

                                    break;
                                }

                                case ChildId::ENFORCE_JOIN_ORDER_CHECK_BOX:
                                {
                                    enforceJoinOrderCheckBox->SetChecked(!enforceJoinOrderCheckBox->IsChecked());

                                    break;
                                }

                                case ChildId::REPLICATED_ONLY_CHECK_BOX:
                                {
                                    replicatedOnlyCheckBox->SetChecked(!replicatedOnlyCheckBox->IsChecked());

                                    break;
                                }

                                case ChildId::COLLOCATED_CHECK_BOX:
                                {
                                    collocatedCheckBox->SetChecked(!collocatedCheckBox->IsChecked());

                                    break;
                                }

                                case ChildId::LAZY_CHECK_BOX:
                                {
                                    lazyCheckBox->SetChecked(!lazyCheckBox->IsChecked());

                                    break;
                                }

                                case ChildId::SKIP_REDUCER_ON_UPDATE_CHECK_BOX:
                                {
                                    skipReducerOnUpdateCheckBox->SetChecked(!skipReducerOnUpdateCheckBox->IsChecked());

                                    break;
                                }

                                case ChildId::PROTOCOL_VERSION_COMBO_BOX:
                                {
                                    std::string versionStr;
                                    protocolVersionComboBox->GetText(versionStr);

                                    protocol_version version = protocol_version::from_string(versionStr);
                                    lazyCheckBox->SetEnabled(version >= protocol_version::VERSION_2_1_5);
                                    skipReducerOnUpdateCheckBox->SetEnabled(version >= protocol_version::VERSION_2_3_0);
                                    nestedTxModeComboBox->SetEnabled(version >= protocol_version::VERSION_2_5_0);
                                    engineModeComboBox->SetEnabled(version >= protocol_version::VERSION_2_13_0);

                                    break;
                                }

                                case ChildId::SSL_MODE_COMBO_BOX:
                                {
                                    using ssl::ssl_mode;

                                    std::string sslModeStr;
                                    sslModeComboBox->GetText(sslModeStr);

                                    ssl_mode sslMode = ssl_mode_from_string(sslModeStr, ssl_mode::DISABLE);

                                    sslKeyFileEdit->SetEnabled(sslMode != ssl_mode::DISABLE);
                                    sslCertFileEdit->SetEnabled(sslMode != ssl_mode::DISABLE);
                                    sslCaFileEdit->SetEnabled(sslMode != ssl_mode::DISABLE);

                                    break;
                                }

                                default:
                                    return false;
                            }

                            break;
                        }

                        case WM_DESTROY:
                        {
                            PostQuitMessage(accepted ? Result::OK : Result::CANCEL);

                            break;
                        }

                        default:
                            return false;
                    }

                    return true;
                }

                void DsnConfigurationWindow::RetrieveParameters(config::configuration& cfg) const
                {
                    RetrieveConnectionParameters(cfg);
                    RetrieveAuthParameters(cfg);
                    RetrieveSslParameters(cfg);
                    RetrieveAdditionalParameters(cfg);
                }

                void DsnConfigurationWindow::RetrieveConnectionParameters(config::configuration& cfg) const
                {
                    std::string dsnStr;
                    std::string addressStr;
                    std::string schemaStr;
                    std::string versionStr;

                    nameEdit->GetText(dsnStr);
                    addressEdit->GetText(addressStr);
                    schemaEdit->GetText(schemaStr);
                    protocolVersionComboBox->GetText(versionStr);

                    strip_surrounding_whitespaces(addressStr);
                    strip_surrounding_whitespaces(dsnStr);
                    // Stripping of whitespaces off the schema skipped intentionally

                    LOG_MSG("Retrieving arguments:");
                    LOG_MSG("DSN:                " << dsnStr);
                    LOG_MSG("Address:            " << addressStr);
                    LOG_MSG("Schema:             " << schemaStr);
                    LOG_MSG("Protocol version:   " << versionStr);

                    if (dsnStr.empty())
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "DSN name can not be empty.");

                    diagnostic_record_storage diag;

                    std::vector<EndPoint> addresses;

                    config::parse_address(addressStr, addresses, &diag);

                    if (diag.get_status_records_number() > 0)
                    {
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC,
                            diag.get_status_record(1).get_message_text().c_str());
                    }

                    protocol_version version = protocol_version::from_string(versionStr);

                    if (!version.is_supported())
                        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, "Protocol version is not supported.");

                    cfg.SetDsn(dsnStr);
                    cfg.set_addresses(addresses);
                    cfg.SetSchema(schemaStr);
                    cfg.SetProtocolVersion(version);
                }

                void DsnConfigurationWindow::RetrieveAuthParameters(config::configuration& cfg) const
                {
                    std::string user;
                    std::string password;

                    userEdit->GetText(user);
                    passwordEdit->GetText(password);

                    cfg.SetUser(user);
                    cfg.SetPassword(password);
                }

                void DsnConfigurationWindow::RetrieveSslParameters(config::configuration& cfg) const
                {
                    std::string sslModeStr;
                    std::string sslKeyStr;
                    std::string sslCertStr;
                    std::string sslCaStr;

                    sslModeComboBox->GetText(sslModeStr);
                    sslKeyFileEdit->GetText(sslKeyStr);
                    sslCertFileEdit->GetText(sslCertStr);
                    sslCaFileEdit->GetText(sslCaStr);

                    LOG_MSG("Retrieving arguments:");
                    LOG_MSG("SSL Mode:           " << sslModeStr);
                    LOG_MSG("SSL Key:            " << sslKeyStr);
                    LOG_MSG("SSL Certificate:    " << sslCertStr);
                    LOG_MSG("SSL CA:             " << sslCaStr);

                    ssl::ssl_mode sslMode = ssl::ssl_mode_from_string(sslModeStr, ssl::ssl_mode::DISABLE);

                    cfg.SetSslMode(sslMode);
                    cfg.SetSslKeyFile(sslKeyStr);
                    cfg.SetSslCertFile(sslCertStr);
                    cfg.SetSslCaFile(sslCaStr);
                }

                void DsnConfigurationWindow::RetrieveAdditionalParameters(config::configuration& cfg) const
                {
                    std::string pageSizeStr;

                    pageSizeEdit->GetText(pageSizeStr);

                    int32_t page_size = lexical_cast<int32_t>(pageSizeStr);

                    if (page_size <= 0)
                        page_size = config.get_page_size();

                    std::string nestedTxModeStr;

                    nestedTxModeComboBox->GetText(nestedTxModeStr);

                    nested_tx_mode nestedTxMode = nested_tx_mode::from_string(nestedTxModeStr,
                                                                               config.GetNestedTxMode());

                    std::string engineModeStr;

                    engineModeComboBox->GetText(engineModeStr);

                    EngineMode::Type engineMode = EngineMode::from_string(engineModeStr, config.GetEngineMode());

                    bool distributedJoins = distributedJoinsCheckBox->IsChecked();
                    bool enforceJoinOrder = enforceJoinOrderCheckBox->IsChecked();
                    bool replicatedOnly = replicatedOnlyCheckBox->IsChecked();
                    bool collocated = collocatedCheckBox->IsChecked();
                    bool lazy = lazyCheckBox->IsChecked();
                    bool skipReducerOnUpdate = skipReducerOnUpdateCheckBox->IsChecked();

                    LOG_MSG("Retrieving arguments:");
                    LOG_MSG("Page size:              " << page_size);
                    LOG_MSG("SQL Engine Mode:        " << EngineMode::to_string(engineMode));
                    LOG_MSG("Nested TX Mode:         " << nested_tx_mode::to_string(nestedTxMode));
                    LOG_MSG("Distributed Joins:      " << (distributedJoins ? "true" : "false"));
                    LOG_MSG("Enforce Join Order:     " << (enforceJoinOrder ? "true" : "false"));
                    LOG_MSG("Replicated only:        " << (replicatedOnly ? "true" : "false"));
                    LOG_MSG("Collocated:             " << (collocated ? "true" : "false"));
                    LOG_MSG("Lazy:                   " << (lazy ? "true" : "false"));
                    LOG_MSG("Skip reducer on update: " << (skipReducerOnUpdate ? "true" : "false"));

                    cfg.set_page_size(page_size);
                    cfg.SetEngineMode(engineMode);
                    cfg.SetNestedTxMode(nestedTxMode);
                    cfg.SetDistributedJoins(distributedJoins);
                    cfg.SetEnforceJoinOrder(enforceJoinOrder);
                    cfg.SetReplicatedOnly(replicatedOnly);
                    cfg.SetCollocated(collocated);
                    cfg.SetLazy(lazy);
                    cfg.SetSkipReducerOnUpdate(skipReducerOnUpdate);
                }
            }
        }
    }
}
