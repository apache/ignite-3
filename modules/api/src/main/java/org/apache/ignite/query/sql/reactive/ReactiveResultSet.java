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

package org.apache.ignite.query.sql.reactive;

import java.sql.ResultSetMetaData;
import java.util.concurrent.Flow;
import org.apache.ignite.query.sql.QueryType;
import org.apache.ignite.query.sql.SqlRow;

/**
 * Reactive result set provides methods to subscribe to the query results in reactive way.
 *
 * Note: It implies to be used with the reactive framework such as ProjectReactor or R2DBC.
 */
public interface ReactiveResultSet extends Flow.Publisher<SqlRow> {

    /**
     * Return publisher for the ResultSet's metadata.
     *
     * @return Metadata publisher.
     */
    Flow.Publisher<ResultSetMetaData> metadata();

    /**
     * @return Number of affected rows.
     */
    Flow.Publisher<Integer> updateCount();

    /**
     * @return Query type.
     * @see QueryType
     */
    Flow.Publisher<QueryType> queryType();
}
