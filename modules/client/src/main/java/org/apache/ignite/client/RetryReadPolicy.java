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

package org.apache.ignite.client;

/**
 * Retry policy that returns true for all read-only operations that do not modify data.
 */
public class RetryReadPolicy extends RetryLimitPolicy {
    /** {@inheritDoc} */
    @Override
    public boolean shouldRetry(RetryPolicyContext context) {
        if (!super.shouldRetry(context)) {
            return false;
        }

        switch (context.operation()) {
            case TABLES_GET:
            case TUPLE_CONTAINS_KEY:
            case TUPLE_GET_ALL:
            case TUPLE_GET:
            case TABLE_GET:
            case CHANNEL_CONNECT:
                return true;

            case TUPLE_UPSERT:
            case COMPUTE_EXECUTE:
            case COMPUTE_EXECUTE_MAPREDUCE:
            case COMPUTE_GET_STATUS:
            case COMPUTE_CANCEL:
            case COMPUTE_CHANGE_PRIORITY:
            case TUPLE_GET_AND_DELETE:
            case TUPLE_DELETE_ALL_EXACT:
            case TUPLE_DELETE_EXACT:
            case TUPLE_DELETE_ALL:
            case TUPLE_DELETE:
            case TUPLE_GET_AND_REPLACE:
            case TUPLE_REPLACE_EXACT:
            case TUPLE_REPLACE:
            case TUPLE_INSERT_ALL:
            case TUPLE_INSERT:
            case TUPLE_GET_AND_UPSERT:
            case TUPLE_UPSERT_ALL:
            case SQL_EXECUTE:
            case SQL_EXECUTE_BATCH:
            case SQL_CURSOR_NEXT_PAGE:
            case SQL_EXECUTE_SCRIPT:
            case STREAMER_BATCH_SEND:
            case PRIMARY_REPLICAS_GET:
            case STREAMER_WITH_RECEIVER_BATCH_SEND:
                return false;

            default:
                throw new UnsupportedOperationException("Unsupported operation type: " + context.operation());
        }
    }
}
