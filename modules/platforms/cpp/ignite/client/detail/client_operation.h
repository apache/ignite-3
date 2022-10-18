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

namespace ignite::detail {

/**
 * Client operation code.
 */
enum class client_operation {
    /** Get all tables. */
    TABLES_GET = 3,

    /** Get table. */
    TABLE_GET = 4,

    /** Upsert tuple. */
    TUPLE_UPSERT = 10,

    /** Get tuple. */
    TUPLE_GET = 12,
};

/**
 * Message type.
 */
enum class message_type {
    /** Response. */
    RESPONSE = 0,

    /** Notification. */
    NOTIFICATION = 1,
};

} // namespace ignite::detail
