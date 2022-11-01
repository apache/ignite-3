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

    /** Get schemas. */
    SCHEMAS_GET = 5,

    /** Upsert tuple. */
    TUPLE_UPSERT = 10,

    /** Get tuple. */
    TUPLE_GET = 12,

    /** Upsert all tuples. */
    TUPLE_UPSERT_ALL = 13,

    /** Get all tuples. */
    TUPLE_GET_ALL = 15,

    /** Get and upsert tuple. */
    TUPLE_GET_AND_UPSERT = 16,

    /** Insert tuple. */
    TUPLE_INSERT = 18,

    /** Insert all tuples. */
    TUPLE_INSERT_ALL = 20,

    /** Replace tuple. */
    TUPLE_REPLACE = 22,

    /** Replace exact tuple. */
    TUPLE_REPLACE_EXACT = 24,

    /** Get and replace tuple. */
    TUPLE_GET_AND_REPLACE = 26,

    /** Delete tuple. */
    TUPLE_DELETE = 28,

    /** Delete all tuples. */
    TUPLE_DELETE_ALL = 29,

    /** Delete exact tuple. */
    TUPLE_DELETE_EXACT = 30,

    /** Delete all exact tuples. */
    TUPLE_DELETE_ALL_EXACT = 31,

    /** Get and delete tuple. */
    TUPLE_GET_AND_DELETE = 32,
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
