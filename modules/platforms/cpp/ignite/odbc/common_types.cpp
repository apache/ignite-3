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

sql_state error_code_to_sql_state(error::code code) {
    switch (code) {
        // Common group. Group code: 1
        case error::code::CURSOR_ALREADY_CLOSED:
        case error::code::RESOURCE_CLOSING:
            return sql_state::S24000_INVALID_CURSOR_STATE;
        case error::code::NODE_STOPPING:
        case error::code::COMPONENT_NOT_STARTED:
        case error::code::ILLEGAL_ARGUMENT:
        case error::code::USER_OBJECT_SERIALIZATION:
        case error::code::SSL_CONFIGURATION:
        case error::code::NODE_LEFT:
        case error::code::INTERNAL:
        case error::code::NULLABLE_VALUE:
            return sql_state::SHY000_GENERAL_ERROR;

        // Table group. Group code: 2
        case error::code::TABLE_ALREADY_EXISTS:
            return sql_state::S42S01_TABLE_OR_VIEW_ALREADY_EXISTS;
        case error::code::TABLE_NOT_FOUND:
            return sql_state::S42S02_TABLE_OR_VIEW_NOT_FOUND;
        case error::code::COLUMN_ALREADY_EXISTS:
            return sql_state::S42S21_COLUMN_ALREADY_EXISTS;
        case error::code::COLUMN_NOT_FOUND:
            return sql_state::S42S22_COLUMN_NOT_FOUND;
        case error::code::TABLE_STOPPING:
        case error::code::TABLE_DEFINITION:
        case error::code::SCHEMA_VERSION_MISMATCH:
        case error::code::UNSUPPORTED_PARTITION_TYPE:
            return sql_state::SHY000_GENERAL_ERROR;

        // Client group. Group code: 3
        case error::code::CONNECTION:
        case error::code::PROTOCOL:
        case error::code::PROTOCOL_COMPATIBILITY:
            return sql_state::S08001_CANNOT_CONNECT;
        case error::code::TABLE_ID_NOT_FOUND:
            return sql_state::S42S02_TABLE_OR_VIEW_NOT_FOUND;
        case error::code::AUTHENTICATION:
        case error::code::AUTHORIZATION:
        case error::code::CONFIGURATION:
        case error::code::CLUSTER_ID_MISMATCH:
        case error::code::CLIENT_SSL_CONFIGURATION:
        case error::code::HANDSHAKE_HEADER:
            return sql_state::S08004_CONNECTION_REJECTED;

        // Sql group. Group code: 4
        case error::code::SCHEMA_NOT_FOUND:
            return sql_state::S3F000_INVALID_SCHEMA_NAME;
        case error::code::EXECUTION_CANCELLED:
            return sql_state::SHY008_OPERATION_CANCELED;
        case error::code::TX_CONTROL_INSIDE_EXTERNAL_TX:
            return sql_state::S25000_INVALID_TRANSACTION_STATE;
        case error::code::CONSTRAINT_VIOLATION:
            return sql_state::S23000_INTEGRITY_CONSTRAINT_VIOLATION;
        case error::code::STMT_PARSE:
            return sql_state::S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION;
        case error::code::STMT_VALIDATION:
            return sql_state::S42000_SYNTAX_ERROR_OR_ACCESS_VIOLATION;
        case error::code::QUERY_NO_RESULT_SET:
        case error::code::RUNTIME:
        case error::code::MAPPING:
            return sql_state::SHY000_GENERAL_ERROR;

        // MetaStorage group. Group code: 5
        case error::code::OP_EXECUTION:
        case error::code::OP_EXECUTION_TIMEOUT:
            return sql_state::SHYT00_TIMEOUT_EXPIRED;
        case error::code::STARTING_STORAGE:
        case error::code::RESTORING_STORAGE:
        case error::code::COMPACTION:
            return sql_state::SHY000_GENERAL_ERROR;

        // Index group. Group code: 6
        case error::code::INDEX_NOT_FOUND:
            return sql_state::S42S12_INDEX_NOT_FOUND;
        case error::code::INDEX_ALREADY_EXISTS:
            return sql_state::S42S11_INDEX_ALREADY_EXISTS;
        case error::code::INVALID_INDEX_DEFINITION:
            return sql_state::SHY000_GENERAL_ERROR;

        // Transactions group. Group code: 7
        case error::code::TX_STATE_STORAGE:
        case error::code::TX_STATE_STORAGE_STOPPED:
        case error::code::TX_UNEXPECTED_STATE:
        case error::code::ACQUIRE_LOCK:
        case error::code::ACQUIRE_LOCK_TIMEOUT:
        case error::code::TX_COMMIT:
        case error::code::TX_ROLLBACK:
        case error::code::TX_FAILED_READ_WRITE_OPERATION:
        case error::code::TX_REPLICA_UNAVAILABLE:
        case error::code::TX_STATE_STORAGE_REBALANCE:
        case error::code::TX_READ_ONLY_TOO_OLD:
        case error::code::TX_INCOMPATIBLE_SCHEMA:
        case error::code::TX_PRIMARY_REPLICA_EXPIRED:
        case error::code::TX_ALREADY_FINISHED:
        case error::code::TX_STALE_OPERATION:
            return sql_state::S25000_INVALID_TRANSACTION_STATE;

        // Replicator group. Group code: 8
        case error::code::CURSOR_CLOSE:
            return sql_state::S24000_INVALID_CURSOR_STATE;
        case error::code::REPLICA_TIMEOUT:
            return sql_state::SHYT00_TIMEOUT_EXPIRED;
        case error::code::REPLICA_COMMON:
        case error::code::REPLICA_IS_ALREADY_STARTED:
        case error::code::REPLICA_UNSUPPORTED_REQUEST:
        case error::code::REPLICA_UNAVAILABLE:
        case error::code::REPLICA_MISS:
        case error::code::REPLICA_STOPPING:
        case error::code::REPLICATION_SAFE_TIME_REORDERING:
            return sql_state::SHY000_GENERAL_ERROR;

        // Storage group. Group code: 9
        case error::code::GENERIC:
        case error::code::DIRECTORY_CREATION:
        case error::code::ALREADY_CLOSED:
        case error::code::STORAGE_REBALANCE:
        case error::code::ALREADY_DESTROYED:
        case error::code::INDEX_NOT_BUILT:
            return sql_state::SHY000_GENERAL_ERROR;

        // DistributionZones group. Group code: 10
        case error::code::ZONE_NOT_FOUND:
            return sql_state::SHY000_GENERAL_ERROR;

        // Network group. Group code: 11
        case error::code::UNRESOLVABLE_CONSISTENT_ID:
        case error::code::PORT_IN_USE:
        case error::code::RECIPIENT_LEFT:
        case error::code::ADDRESS_UNRESOLVED:
            return sql_state::S08001_CANNOT_CONNECT;

        // NodeConfiguration group. Group code: 12
        case error::code::CONFIG_READ:
        case error::code::CONFIG_FILE_CREATE:
        case error::code::CONFIG_WRITE:
        case error::code::CONFIG_PARSE:
            return sql_state::SHY000_GENERAL_ERROR;

        // CodeDeployment group. Group code: 13
        case error::code::UNIT_NOT_FOUND:
        case error::code::UNIT_ALREADY_EXISTS:
        case error::code::UNIT_CONTENT_READ:
        case error::code::UNIT_UNAVAILABLE:
            return sql_state::SHY000_GENERAL_ERROR;

        // GarbageCollector group. Group code: 14
        case error::code::CLOSED:
            return sql_state::SHY000_GENERAL_ERROR;

        // Authentication group. Group code: 15
        case error::code::UNSUPPORTED_AUTHENTICATION_TYPE:
        case error::code::INVALID_CREDENTIALS:
        case error::code::BASIC_PROVIDER:
            return sql_state::S08004_CONNECTION_REJECTED;

        // Compute group. Group code: 16
        case error::code::CLASS_PATH:
        case error::code::CLASS_LOADER:
        case error::code::CLASS_INITIALIZATION:
        case error::code::QUEUE_OVERFLOW:
        case error::code::COMPUTE_JOB_STATUS_TRANSITION:
        case error::code::CANCELLING:
        case error::code::RESULT_NOT_FOUND:
        case error::code::FAIL_TO_GET_JOB_STATE:
        case error::code::COMPUTE_JOB_FAILED:
        case error::code::CHANGE_JOB_PRIORITY_NO_JOB:
        case error::code::PRIMARY_REPLICA_RESOLVE:
        case error::code::CHANGE_JOB_PRIORITY_JOB_EXECUTING:
        case error::code::CHANGE_JOB_PRIORITY:
        case error::code::NODE_NOT_FOUND:
            return sql_state::SHY000_GENERAL_ERROR;

        // Catalog group. Group code: 17
        case error::code::VALIDATION:
            return sql_state::SHY000_GENERAL_ERROR;

        // PlacementDriver group. Group code: 18
        case error::code::PRIMARY_REPLICA_AWAIT_TIMEOUT:
            return sql_state::SHYT00_TIMEOUT_EXPIRED;
        case error::code::PRIMARY_REPLICA_AWAIT:
            return sql_state::SHY000_GENERAL_ERROR;

        // CriticalWorkers group. Group code: 19
        case error::code::SYSTEM_WORKER_BLOCKED:
        case error::code::SYSTEM_CRITICAL_OPERATION_TIMEOUT:
            return sql_state::SHY000_GENERAL_ERROR;

        // DisasterRecovery group. Group code: 20
        case error::code::NODES_NOT_FOUND:
        case error::code::ILLEGAL_PARTITION_ID:
        case error::code::PARTITION_STATE:
        case error::code::CLUSTER_NOT_IDLE:
            return sql_state::SHY000_GENERAL_ERROR;

        // Embedded group. Group code: 21
        case error::code::CLUSTER_NOT_INITIALIZED:
        case error::code::CLUSTER_INIT_FAILED:
        case error::code::NODE_NOT_STARTED:
        case error::code::NODE_START:
            return sql_state::SHY000_GENERAL_ERROR;

        // Marshalling group. Group code: асV 22
        case error::code::COMMON:
        case error::code::UNSUPPORTED_OBJECT_TYPE:
        case error::code::TYPE_CHECK_MARSHALLING:
            return sql_state::SHY000_GENERAL_ERROR;
    }

    return sql_state::SHY000_GENERAL_ERROR;
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
