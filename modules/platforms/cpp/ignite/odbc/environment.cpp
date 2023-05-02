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

#include <cstdlib>

#include "connection.h"
#include "environment.h"
#include "ignite/odbc/system/odbc_constants.h"

namespace ignite
{

connection* environment::create_connection()
{
    connection* connection;

    IGNITE_ODBC_API_CALL(internal_create_connection(connection));

    return connection;
}

void environment::deregister_connection(connection* conn)
{
    m_connections.erase(conn);
}

sql_result environment::internal_create_connection(connection*& connection)
{
    connection = new connection(this);

    if (!connection)
    {
        add_status_record(sql_state::SHY001_MEMORY_ALLOCATION, "Not enough memory.");

        return sql_result::AI_ERROR;
    }

    m_connections.insert(connection);

    return sql_result::AI_SUCCESS;
}

void environment::transaction_commit()
{
    IGNITE_ODBC_API_CALL(internal_transaction_commit());
}

sql_result environment::internal_transaction_commit()
{
    sql_result res = sql_result::AI_SUCCESS;

    for (auto conn : m_connections)
    {
        conn->transaction_commit();

        diagnostic_record_storage& diag = conn->get_diagnostic_records();

        if (diag.get_status_records_number() > 0)
        {
            add_status_record(diag.get_status_record(1));
            res = sql_result::AI_SUCCESS_WITH_INFO;
        }
    }

    return res;
}

void environment::transaction_rollback()
{
    IGNITE_ODBC_API_CALL(internal_transaction_rollback());
}

sql_result environment::internal_transaction_rollback()
{
    sql_result res = sql_result::AI_SUCCESS;

    for (auto conn : m_connections)
    {
        conn->transaction_rollback();

        diagnostic_record_storage& diag = conn->get_diagnostic_records();

        if (diag.get_status_records_number() > 0)
        {
            add_status_record(diag.get_status_record(1));
            res = sql_result::AI_SUCCESS_WITH_INFO;
        }
    }

    return res;
}

void environment::set_attribute(int32_t attr, void* value, int32_t len)
{
    IGNITE_ODBC_API_CALL(internal_set_attribute(attr, value, len));
}

sql_result environment::internal_set_attribute(int32_t attr, void* value, int32_t len)
{
    UNUSED_VALUE(len);

    environment_attribute attribute = environment_attribute_to_internal(attr);

    switch (attribute)
    {
        case environment_attribute::ODBC_VERSION:
        {
            auto version = static_cast<int32_t>(reinterpret_cast<intptr_t>(value));

            if (version != m_odbc_version)
            {
                add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED,
                    "ODBC version is not supported.");

                return sql_result::AI_SUCCESS_WITH_INFO;
            }

            return sql_result::AI_SUCCESS;
        }

        case environment_attribute::OUTPUT_NTS:
        {
            auto nts = static_cast<int32_t>(reinterpret_cast<intptr_t>(value));

            if (nts != m_odbc_nts)
            {
                add_status_record(sql_state::S01S02_OPTION_VALUE_CHANGED,
                    "Only null-termination of strings is supported.");

                return sql_result::AI_SUCCESS_WITH_INFO;
            }

            return sql_result::AI_SUCCESS;
        }

        case environment_attribute::UNKNOWN:
        default:
            break;
    }

    add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
        "Attribute is not supported.");

    return sql_result::AI_ERROR;
}

void environment::get_attribute(int32_t attr, application_data_buffer& buffer)
{
    IGNITE_ODBC_API_CALL(internal_get_attribute(attr, buffer));
}

sql_result environment::internal_get_attribute(int32_t attr, application_data_buffer& buffer)
{
    environment_attribute attribute = environment_attribute_to_internal(attr);

    switch (attribute)
    {
        case environment_attribute::ODBC_VERSION:
        {
            buffer.put_int32(m_odbc_version);

            return sql_result::AI_SUCCESS;
        }

        case environment_attribute::OUTPUT_NTS:
        {
            buffer.put_int32(m_odbc_nts);

            return sql_result::AI_SUCCESS;
        }

        case environment_attribute::UNKNOWN:
        default:
            break;
    }

    add_status_record(sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED,
        "Attribute is not supported.");

    return sql_result::AI_ERROR;
}

} // namespace ignite
