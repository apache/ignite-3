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

#include "ignite/odbc/meta/table_meta.h"

namespace ignite {

void table_meta::read(protocol::reader &reader) {
    auto status = reader.read_int32();
    assert(status == 0);

    auto err_msg = reader.read_string_nullable();
    assert(!err_msg);

    schema_name = reader.read_string();
    table_name = reader.read_string();
    table_type = reader.read_string();
}

table_meta_vector read_table_meta_vector(protocol::reader &reader) {
    auto meta_num = reader.read_int32();

    table_meta_vector meta;
    meta.reserve(static_cast<std::size_t>(meta_num));

    for (std::int32_t i = 0; i < meta_num; ++i) {
        meta.emplace_back();
        meta.back().read(reader);
    }

    return meta;
}

} // namespace ignite
