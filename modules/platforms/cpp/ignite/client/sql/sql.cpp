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

#include "ignite/client/sql/sql.h"
#include "ignite/client/detail/argument_check_utils.h"
#include "ignite/client/detail/sql/sql_impl.h"

namespace ignite {

void sql::execute_async(transaction *tx, const sql_statement &statement, std::vector<primitive> args,
    ignite_callback<result_set> callback) {
    m_impl->execute_async(tx, statement, std::move(args), std::move(callback));
}

void sql::execute_script_async(
    const sql_statement &statement, std::vector<primitive> args, ignite_callback<void> callback) {
    m_impl->execute_script_async(statement, std::move(args), std::move(callback));
}

} // namespace ignite
