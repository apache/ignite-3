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

#pragma once

#include "ignite/client/detail/table/schema.h"
#include "ignite/client/table/ignite_tuple.h"
#include "ignite/client/transaction/transaction.h"
#include "ignite/common/primitive.h"

#include "ignite/protocol/writer.h"
#include "ignite/tuple/binary_tuple_builder.h"
#include "ignite/tuple/binary_tuple_parser.h"

namespace ignite::detail {
/**
 * Tuple concatenation function.
 *
 * @param left Left hand value.
 * @param right Right hand value.
 * @return Resulting tuple.
 */
[[nodiscard]] ignite_tuple concat(const ignite_tuple &left, const ignite_tuple &right);

/**
 * Write tuple using table schema and writer.
 *
 * @param writer Writer.
 * @param sch Schema.
 * @param tuple Tuple.
 * @param key_only Should only key fields be written or not.
 */
void write_tuple(protocol::writer &writer, const schema &sch, const ignite_tuple &tuple, bool key_only);

/**
 * Write tuples using table schema and writer.
 *
 * @param writer Writer.
 * @param sch Schema.
 * @param tuples Tuples.
 * @param key_only Should only key fields be written or not.
 */
void write_tuples(protocol::writer &writer, const schema &sch, const std::vector<ignite_tuple> &tuples, bool key_only);

} // namespace ignite::detail
