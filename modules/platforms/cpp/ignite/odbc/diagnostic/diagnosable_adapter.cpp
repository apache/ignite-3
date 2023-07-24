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

#include "ignite/odbc/diagnostic/diagnosable_adapter.h"
#include "ignite/odbc/log.h"
#include "ignite/odbc/odbc_error.h"

namespace ignite {

void diagnosable_adapter::add_status_record(
    sql_state sql_state, const std::string &message, int32_t row_num, int32_t column_num) {
    LOG_MSG("Adding new record: " << message << ", row_num: " << row_num << ", column_num: " << column_num);
    m_diagnostic_records.add_status_record(diagnostic_record(sql_state, message, "", "", row_num, column_num));
}

void diagnosable_adapter::add_status_record(sql_state sql_state, const std::string &message) {
    add_status_record(sql_state, message, 0, 0);
}

void diagnosable_adapter::add_status_record(const std::string &message) {
    add_status_record(sql_state::SHY000_GENERAL_ERROR, message);
}

void diagnosable_adapter::add_status_record(const odbc_error &err) {
    add_status_record(err.get_state(), err.get_error_message(), 0, 0);
}

void diagnosable_adapter::add_status_record(const diagnostic_record &rec) {
    LOG_MSG("Adding new record: " << rec.get_sql_state() << " " << rec.get_message_text());
    m_diagnostic_records.add_status_record(rec);
}

} // namespace ignite
