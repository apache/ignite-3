/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.client;

/**
 * Client operation codes.
 */
public class ClientOp {
    public static final int TABLE_CREATE = 1;

    public static final int TABLE_DROP = 2;

    public static final int TABLES_GET = 3;

    public static final int TABLE_GET = 4;

    public static final int SCHEMAS_GET = 5;

    public static final int TUPLE_UPSERT = 10;

    public static final int TUPLE_UPSERT_SCHEMALESS = 11;

    public static final int TUPLE_GET = 12;
}
