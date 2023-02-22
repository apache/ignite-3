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

#include "ignite/client/primitive.h"
#include "ignite/client/table/ignite_tuple.h"
#include "ignite/client/transaction/transaction.h"
#include "ignite/schema/binary_tuple_builder.h"
#include "ignite/schema/binary_tuple_parser.h"
#include "ignite/schema/ignite_type.h"

namespace ignite::detail {

/**
 * Claim space for a value with type header in tuple builder.
 *
 * @param builder Tuple builder.
 * @param value Value.
 */
void claim_primitive_with_type(binary_tuple_builder &builder, const primitive &value);

/**
 * Append a value with type header in tuple builder.
 *
 * @param builder Tuple builder.
 * @param value Value.
 */
void append_primitive_with_type(binary_tuple_builder &builder, const primitive &value);

/**
 * Read column value from binary tuple.
 *
 * @param parser Binary tuple parser.
 * @param typ Column type.
 * @param scale Column scale.
 * @return Column value.
 */
[[nodiscard]] primitive read_next_column(binary_tuple_parser &parser, ignite_type typ, std::int32_t scale);

/**
 * Read column value from binary tuple.
 *
 * @param parser Binary tuple parser.
 * @param typ Column type.
 * @param scale Column scale.
 * @return Column value.
 */
[[nodiscard]] primitive read_next_column(binary_tuple_parser &parser, column_type typ, std::int32_t scale);

/**
 * Tuple concatenation function.
 *
 * @param left Left hand value.
 * @param right Right hand value.
 * @return Resulting tuple.
 */
[[nodiscard]] ignite_tuple concat(const ignite_tuple& left, const ignite_tuple& right);

} // namespace ignite::detail
