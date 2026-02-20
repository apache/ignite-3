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

package org.apache.ignite.internal.client.proto;

/**
 * Error data extensions. When the server returns an error response, it may contain additional data in a map. Keys are defined here.
 */
public class ErrorExtensions {
    public static final String EXPECTED_SCHEMA_VERSION = "expected-schema-ver";

    public static final String SQL_UPDATE_COUNTERS = "sql-update-counters";

    public static final String DELAYED_ACK = "delayed-ack";

    public static final String TX_KILL = "tx-kill";
}
