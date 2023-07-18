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

#include "common_types.h"
#include "ignite/odbc/system/odbc_constants.h"

namespace ignite {

SQLRETURN sql_result_to_return_code(sql_result result) {
    switch (result) {
        case sql_result::AI_SUCCESS:
            return SQL_SUCCESS;

        case sql_result::AI_SUCCESS_WITH_INFO:
            return SQL_SUCCESS_WITH_INFO;

        case sql_result::AI_NO_DATA:
            return SQL_NO_DATA;

        case sql_result::AI_NEED_DATA:
            return SQL_NEED_DATA;

        case sql_result::AI_ERROR:
        default:
            return SQL_ERROR;
    }
}

diagnostic_field diagnostic_field_to_internal(int16_t field) {
    switch (field) {
        case SQL_DIAG_CURSOR_ROW_COUNT:
            return diagnostic_field::HEADER_CURSOR_ROW_COUNT;

        case SQL_DIAG_DYNAMIC_FUNCTION:
            return diagnostic_field::HEADER_DYNAMIC_FUNCTION;

        case SQL_DIAG_DYNAMIC_FUNCTION_CODE:
            return diagnostic_field::HEADER_DYNAMIC_FUNCTION_CODE;

        case SQL_DIAG_NUMBER:
            return diagnostic_field::HEADER_NUMBER;

        case SQL_DIAG_RETURNCODE:
            return diagnostic_field::HEADER_RETURN_CODE;

        case SQL_DIAG_ROW_COUNT:
            return diagnostic_field::HEADER_ROW_COUNT;

        case SQL_DIAG_CLASS_ORIGIN:
            return diagnostic_field::STATUS_CLASS_ORIGIN;

        case SQL_DIAG_COLUMN_NUMBER:
            return diagnostic_field::STATUS_COLUMN_NUMBER;

        case SQL_DIAG_CONNECTION_NAME:
            return diagnostic_field::STATUS_CONNECTION_NAME;

        case SQL_DIAG_MESSAGE_TEXT:
            return diagnostic_field::STATUS_MESSAGE_TEXT;

        case SQL_DIAG_NATIVE:
            return diagnostic_field::STATUS_NATIVE;

        case SQL_DIAG_ROW_NUMBER:
            return diagnostic_field::STATUS_ROW_NUMBER;

        case SQL_DIAG_SERVER_NAME:
            return diagnostic_field::STATUS_SERVER_NAME;

        case SQL_DIAG_SQLSTATE:
            return diagnostic_field::STATUS_SQLSTATE;

        case SQL_DIAG_SUBCLASS_ORIGIN:
            return diagnostic_field::STATUS_SUBCLASS_ORIGIN;

        default:
            break;
    }

    return diagnostic_field::UNKNOWN;
}

environment_attribute environment_attribute_to_internal(int32_t attr) {
    switch (attr) {
        case SQL_ATTR_ODBC_VERSION:
            return environment_attribute::ODBC_VERSION;

        case SQL_ATTR_OUTPUT_NTS:
            return environment_attribute::OUTPUT_NTS;

        default:
            break;
    }

    return environment_attribute::UNKNOWN;
}

sql_state response_status_to_sql_state(int32_t status) {
    switch (response_status(status)) {
        case response_status::PARSING_FAILURE:
        case response_status::KEY_UPDATE:
        case response_status::UNEXPECTED_OPERATION:
            return sql_state::S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION;

        case response_status::UNSUPPORTED_OPERATION:
            return sql_state::SHYC00_OPTIONAL_FEATURE_NOT_IMPLEMENTED;

        case response_status::UNEXPECTED_ELEMENT_TYPE:
            return sql_state::SHY004_INVALID_SQL_DATA_TYPE;

        case response_status::DUPLICATE_KEY:
        case response_status::NULL_KEY:
        case response_status::NULL_VALUE:
            return sql_state::S23000_INTEGRITY_CONSTRAINT_VIOLATION;

        case response_status::TABLE_NOT_FOUND:
            return sql_state::S42S02_TABLE_OR_VIEW_NOT_FOUND;

        case response_status::INDEX_ALREADY_EXISTS:
            return sql_state::S42S11_INDEX_ALREADY_EXISTS;

        case response_status::INDEX_NOT_FOUND:
            return sql_state::S42S12_INDEX_NOT_FOUND;

        case response_status::TABLE_ALREADY_EXISTS:
            return sql_state::S42S01_TABLE_OR_VIEW_ALREADY_EXISTS;

        case response_status::COLUMN_NOT_FOUND:
            return sql_state::S42S22_COLUMN_NOT_FOUND;

        case response_status::COLUMN_ALREADY_EXISTS:
            return sql_state::S42S21_COLUMN_ALREADY_EXISTS;

        case response_status::TRANSACTION_COMPLETED:
            return sql_state::S25000_INVALID_TRANSACTION_STATE;

        case response_status::TRANSACTION_SERIALIZATION_ERROR:
            return sql_state::S40001_SERIALIZATION_FAILURE;

        case response_status::CACHE_NOT_FOUND:
        case response_status::NULL_TABLE_DESCRIPTOR:
        case response_status::CONVERSION_FAILED:
        case response_status::CONCURRENT_UPDATE:
        case response_status::ENTRY_PROCESSING:
        case response_status::TABLE_DROP_FAILED:
        case response_status::STMT_TYPE_MISMATCH:
        case response_status::UNKNOWN_ERROR:
        default:
            return sql_state::SHY000_GENERAL_ERROR;
    }
}

} // namespace ignite
