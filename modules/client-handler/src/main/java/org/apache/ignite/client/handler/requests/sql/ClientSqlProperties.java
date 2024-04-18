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

package org.apache.ignite.client.handler.requests.sql;

import javax.annotation.Nullable;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.sql.api.IgniteSqlImpl;
import org.apache.ignite.internal.sql.engine.QueryProperty;
import org.apache.ignite.internal.sql.engine.property.SqlProperties;
import org.apache.ignite.internal.sql.engine.property.SqlPropertiesHelper;

class ClientSqlProperties {
    private final @Nullable String schema;

    private final int pageSize;

    private final long queryTimeout;

    private final long idleTimeout;

    ClientSqlProperties(ClientMessageUnpacker in) {
        schema = in.tryUnpackNil() ? null : in.unpackString();
        pageSize = in.tryUnpackNil() ? IgniteSqlImpl.DEFAULT_PAGE_SIZE : in.unpackInt();
        queryTimeout = in.tryUnpackNil() ? 0 : in.unpackLong();
        idleTimeout = in.tryUnpackNil() ? 0 : in.unpackLong();

        // Skip properties - not used by SQL engine.
        in.unpackInt(); // Number of properties.
        in.readBinaryUnsafe(); // Binary tuple with properties
    }

    public @Nullable String schema() {
        return schema;
    }

    public int pageSize() {
        return pageSize;
    }

    public long queryTimeout() {
        return queryTimeout;
    }

    public long idleTimeout() {
        return idleTimeout;
    }

    SqlProperties toSqlProps() {
        SqlProperties.Builder builder = SqlPropertiesHelper.newBuilder()
                .set(QueryProperty.QUERY_TIMEOUT, queryTimeout);

        if (schema != null) {
            builder.set(QueryProperty.DEFAULT_SCHEMA, schema);
        }

        return builder.build();
    }
}
