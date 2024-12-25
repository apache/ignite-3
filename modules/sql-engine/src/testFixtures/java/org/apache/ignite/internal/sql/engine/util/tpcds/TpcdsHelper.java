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

package org.apache.ignite.internal.sql.engine.util.tpcds;

import static org.apache.ignite.internal.sql.engine.util.tpch.TpchHelper.loadFromResource;

/**
 * Provides utility methods to work with queries defined by the TPC-DS benchmark.
 */
public final class TpcdsHelper {

    private TpcdsHelper() {

    }

    /**
     * Loads a TPC-DS query given a query identifier. Query identifier can be in the following forms:
     * <ul>
     *     <li>query id - a number, e.g. {@code 14}. Queries are stored in files named {@code query{id}.sql}.</li>
     * </ul>
     *
     * @param queryId The identifier of a query.
     * @return  An SQL query.
     */
    public static String getQuery(String queryId) {
        int numericId = Integer.parseInt(queryId);
        var queryFile = String.format("tpcds/query%s.sql", numericId);
        return loadFromResource(queryFile);
    }
}
