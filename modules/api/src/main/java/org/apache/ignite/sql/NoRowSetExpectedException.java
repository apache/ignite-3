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

package org.apache.ignite.sql;

import static org.apache.ignite.lang.ErrorGroups.Sql.QUERY_NO_RESULT_SET_ERR;

/**
 * Exception is thrown when a query doesn't intend to return any rows (e.g. a DML or a DDL query).
 */
public class NoRowSetExpectedException extends SqlException {
    /**
     * Creates an exception.
     */
    public NoRowSetExpectedException() {
        super(QUERY_NO_RESULT_SET_ERR, "Query has no result set");
    }
}
