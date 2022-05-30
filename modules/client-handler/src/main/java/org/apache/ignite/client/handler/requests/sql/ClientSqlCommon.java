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

package org.apache.ignite.client.handler.requests.sql;

import java.util.List;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.SqlRow;
import org.apache.ignite.sql.async.AsyncResultSet;

/**
 * Common SQL request handling logic.
 */
class ClientSqlCommon {
    static void packCurrentPage(ClientMessagePacker out, AsyncResultSet asyncResultSet) {
        List<ColumnMetadata> cols = asyncResultSet.metadata().columns();

        out.packArrayHeader(asyncResultSet.currentPageSize());

        for (SqlRow row : asyncResultSet.currentPage()) {
            for (int i = 0; i < cols.size(); i++) {
                // TODO: IGNITE-17052 pack only the value according to the known type.
                out.packObjectWithType(row.value(i));
            }
        }

        if (!asyncResultSet.hasMorePages()) {
            asyncResultSet.closeAsync();
        }
    }
}
