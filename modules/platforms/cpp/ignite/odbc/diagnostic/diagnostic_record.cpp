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

#include "ignite/odbc/diagnostic/diagnostic_record.h"

#include <set>
#include <string>

namespace {
/** SQLSTATEs defined by Open Group and ISO call-level interface. */
const std::string ORIGIN_ISO_9075 = "ISO 9075";

/** ODBC-specific SQLSTATEs (all those whose SQLSTATE class is "IM"). */
const std::string ORIGIN_ODBC_3_0 = "ODBC 3.0";

/** SQL state unknown constant. */
const std::string STATE_UNKNOWN;

/** SQL state 01004 constant. */
const std::string STATE_01004 = "01004";

/** SQL state 01S00 constant. */
const std::string STATE_01S00 = "01S00";

/** SQL state 01S01 constant. */
const std::string STATE_01S01 = "01S01";

/** SQL state 01S02 constant. */
const std::string STATE_01S02 = "01S02";

/** SQL state 01S07 constant. */
const std::string STATE_01S07 = "01S07";

/** SQL state 07009 constant. */
const std::string STATE_07009 = "07009";

/** SQL state 07006 constant. */
const std::string STATE_07006 = "07006";

/** SQL state 08001 constant. */
const std::string STATE_08001 = "08001";

/** SQL state 08002 constant. */
const std::string STATE_08002 = "08002";

/** SQL state 08003 constant. */
const std::string STATE_08003 = "08003";

/** SQL state 08004 constant. */
const std::string STATE_08004 = "08004";

/** SQL state 08S01 constant. */
const std::string STATE_08S01 = "08S01";

/** SQL state 22002 constant. */
const std::string STATE_22002 = "22002";

/** SQL state 22026 constant. */
const std::string STATE_22026 = "22026";

/** SQL state 23000 constant. */
const std::string STATE_23000 = "23000";

/** SQL state 24000 constant. */
const std::string STATE_24000 = "24000";

/** SQL state 25000 constant. */
const std::string STATE_25000 = "25000";

/** SQL state 3F000 constant. */
const std::string STATE_3F000 = "3F000";

/** SQL state 40001 constant. */
const std::string STATE_40001 = "40001";

/** SQL state 42000 constant. */
const std::string STATE_42000 = "42000";

/** SQL state 42S01 constant. */
const std::string STATE_42S01 = "42S01";

/** SQL state 42S02 constant. */
const std::string STATE_42S02 = "42S02";

/** SQL state 42S11 constant. */
const std::string STATE_42S11 = "42S11";

/** SQL state 42S12 constant. */
const std::string STATE_42S12 = "42S12";

/** SQL state 42S21 constant. */
const std::string STATE_42S21 = "42S21";

/** SQL state 42S22 constant. */
const std::string STATE_42S22 = "42S22";

/** SQL state HY000 constant. */
const std::string STATE_HY000 = "HY000";

/** SQL state HY001 constant. */
const std::string STATE_HY001 = "HY001";

/** SQL state HY003 constant. */
const std::string STATE_HY003 = "HY003";

/** SQL state HY004 constant. */
const std::string STATE_HY004 = "HY004";

/** SQL state HY009 constant. */
const std::string STATE_HY009 = "HY009";

/** SQL state HY010 constant. */
const std::string STATE_HY010 = "HY010";

/** SQL state HY090 constant. */
const std::string STATE_HY090 = "HY090";

/** SQL state HY092 constant. */
const std::string STATE_HY092 = "HY092";

/** SQL state HY097 constant. */
const std::string STATE_HY097 = "HY097";

/** SQL state HY105 constant. */
const std::string STATE_HY105 = "HY105";

/** SQL state HY106 constant. */
const std::string STATE_HY106 = "HY106";

/** SQL state HYC00 constant. */
const std::string STATE_HYC00 = "HYC00";

/** SQL state HYT00 constant. */
const std::string STATE_HYT00 = "HYT00";

/** SQL state HYT01 constant. */
const std::string STATE_HYT01 = "HYT01";

/** SQL state IM001 constant. */
const std::string STATE_IM001 = "IM001";
} // namespace

namespace ignite {

const std::string &diagnostic_record::get_class_origin() const {
    const std::string &state = get_sql_state();

    if (state[0] == 'I' && state[1] == 'M')
        return ORIGIN_ODBC_3_0;

    return ORIGIN_ISO_9075;
}

const std::string &diagnostic_record::get_subclass_origin() const {
    static std::set<std::string> odbcSubclasses;

    if (odbcSubclasses.empty()) {
        // This is a fixed list taken from ODBC doc.
        // Please do not add/remove values here.
        odbcSubclasses.insert("01S00");
        odbcSubclasses.insert("01S01");
        odbcSubclasses.insert("01S02");
        odbcSubclasses.insert("01S06");
        odbcSubclasses.insert("01S07");
        odbcSubclasses.insert("07S01");
        odbcSubclasses.insert("08S01");
        odbcSubclasses.insert("21S01");
        odbcSubclasses.insert("21S02");
        odbcSubclasses.insert("25S01");
        odbcSubclasses.insert("25S02");
        odbcSubclasses.insert("25S03");
        odbcSubclasses.insert("42S01");
        odbcSubclasses.insert("42S02");
        odbcSubclasses.insert("42S11");
        odbcSubclasses.insert("42S12");
        odbcSubclasses.insert("42S21");
        odbcSubclasses.insert("42S22");
        odbcSubclasses.insert("HY095");
        odbcSubclasses.insert("HY097");
        odbcSubclasses.insert("HY098");
        odbcSubclasses.insert("HY099");
        odbcSubclasses.insert("HY100");
        odbcSubclasses.insert("HY101");
        odbcSubclasses.insert("HY105");
        odbcSubclasses.insert("HY107");
        odbcSubclasses.insert("HY109");
        odbcSubclasses.insert("HY110");
        odbcSubclasses.insert("HY111");
        odbcSubclasses.insert("HYT00");
        odbcSubclasses.insert("HYT01");
        odbcSubclasses.insert("IM001");
        odbcSubclasses.insert("IM002");
        odbcSubclasses.insert("IM003");
        odbcSubclasses.insert("IM004");
        odbcSubclasses.insert("IM005");
        odbcSubclasses.insert("IM006");
        odbcSubclasses.insert("IM007");
        odbcSubclasses.insert("IM008");
        odbcSubclasses.insert("IM010");
        odbcSubclasses.insert("IM011");
        odbcSubclasses.insert("IM012");
    }

    const std::string &state = get_sql_state();

    if (odbcSubclasses.find(state) != odbcSubclasses.end())
        return ORIGIN_ODBC_3_0;

    return ORIGIN_ISO_9075;
}

const std::string &diagnostic_record::get_message_text() const {
    return m_message;
}

const std::string &diagnostic_record::get_connection_name() const {
    return m_connection_name;
}

const std::string &diagnostic_record::get_server_name() const {
    return m_server_name;
}

const std::string &diagnostic_record::get_sql_state() const {
    switch (m_sql_state) {
        case sql_state::S01004_DATA_TRUNCATED:
            return STATE_01004;

        case sql_state::S01S00_INVALID_CONNECTION_STRING_ATTRIBUTE:
            return STATE_01S00;

        case sql_state::S01S01_ERROR_IN_ROW:
            return STATE_01S01;

        case sql_state::S01S02_OPTION_VALUE_CHANGED:
            return STATE_01S02;

        case sql_state::S01S07_FRACTIONAL_TRUNCATION:
            return STATE_01S07;

        case sql_state::S07006_RESTRICTION_VIOLATION:
            return STATE_07006;

        case sql_state::S22002_INDICATOR_NEEDED:
            return STATE_22002;

        case sql_state::S22026_DATA_LENGTH_MISMATCH:
            return STATE_22026;

        case sql_state::S23000_INTEGRITY_CONSTRAINT_VIOLATION:
            return STATE_23000;

        case sql_state::S24000_INVALID_CURSOR_STATE:
            return STATE_24000;

        case sql_state::S25000_INVALID_TRANSACTION_STATE:
            return STATE_25000;

        case sql_state::S3F000_INVALID_SCHEMA_NAME:
            return STATE_3F000;

        case sql_state::S40001_SERIALIZATION_FAILURE:
            return STATE_40001;

        case sql_state::S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION:
            return STATE_42000;

        case sql_state::S42S01_TABLE_OR_VIEW_ALREADY_EXISTS:
            return STATE_42S01;

        case sql_state::S42S02_TABLE_OR_VIEW_NOT_FOUND:
            return STATE_42S02;

        case sql_state::S42S11_INDEX_ALREADY_EXISTS:
            return STATE_42S11;

        case sql_state::S42S12_INDEX_NOT_FOUND:
            return STATE_42S12;

        case sql_state::S42S21_COLUMN_ALREADY_EXISTS:
            return STATE_42S21;

        case sql_state::S42S22_COLUMN_NOT_FOUND:
            return STATE_42S22;

        case sql_state::S07009_INVALID_DESCRIPTOR_INDEX:
            return STATE_07009;

        case sql_state::S08001_CANNOT_CONNECT:
            return STATE_08001;

        case sql_state::S08002_ALREADY_CONNECTED:
            return STATE_08002;

        case sql_state::S08003_NOT_CONNECTED:
            return STATE_08003;

        case sql_state::S08004_CONNECTION_REJECTED:
            return STATE_08004;

        case sql_state::S08S01_LINK_FAILURE:
            return STATE_08S01;

        case sql_state::SHY000_GENERAL_ERROR:
            return STATE_HY000;

        case sql_state::SHY001_MEMORY_ALLOCATION:
            return STATE_HY001;

        case sql_state::SHY003_INVALID_APPLICATION_BUFFER_TYPE:
            return STATE_HY003;

        case sql_state::SHY009_INVALID_USE_OF_NULL_POINTER:
            return STATE_HY009;

        case sql_state::SHY010_SEQUENCE_ERROR:
            return STATE_HY010;

        case sql_state::SHY090_INVALID_STRING_OR_BUFFER_LENGTH:
            return STATE_HY090;

        case sql_state::SHY092_OPTION_TYPE_OUT_OF_RANGE:
            return STATE_HY092;

        case sql_state::SHY097_COLUMN_TYPE_OUT_OF_RANGE:
            return STATE_HY097;

        case sql_state::SHY105_INVALID_PARAMETER_TYPE:
            return STATE_HY105;

        case sql_state::SHY106_FETCH_TYPE_OUT_OF_RANGE:
            return STATE_HY106;

        case sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED:
            return STATE_HYC00;

        case sql_state::SHYT00_TIMEOUT_EXPIRED:
            return STATE_HYT00;

        case sql_state::SHYT01_CONNECTION_TIMEOUT:
            return STATE_HYT01;

        case sql_state::SIM001_FUNCTION_NOT_SUPPORTED:
            return STATE_IM001;

        default:
            break;
    }

    return STATE_UNKNOWN;
}

int32_t diagnostic_record::get_row_number() const {
    return m_row_num;
}

int32_t diagnostic_record::get_column_number() const {
    return m_column_num;
}

bool diagnostic_record::is_retrieved() const {
    return m_retrieved;
}

void diagnostic_record::mark_retrieved() {
    m_retrieved = true;
}

} // namespace ignite
