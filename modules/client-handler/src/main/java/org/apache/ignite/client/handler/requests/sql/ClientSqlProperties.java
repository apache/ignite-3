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

import java.time.ZoneId;
import java.util.Set;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;
import org.apache.ignite.internal.client.sql.QueryModifier;
import org.apache.ignite.internal.sql.SqlCommon;
import org.apache.ignite.internal.sql.engine.SqlProperties;
import org.apache.ignite.lang.util.IgniteNameUtils;
import org.jetbrains.annotations.Nullable;

class ClientSqlProperties {
    private final @Nullable String schema;

    private final int pageSize;

    private final long queryTimeout;

    private final long idleTimeout;

    private final @Nullable String timeZoneId;

    private final Set<QueryModifier> queryModifiers;

    ClientSqlProperties(ClientMessageUnpacker in, boolean unpackQueryModifiers) {
        schema = in.tryUnpackNil() ? null : IgniteNameUtils.parseIdentifier(in.unpackString());
        pageSize = in.tryUnpackNil() ? SqlCommon.DEFAULT_PAGE_SIZE : in.unpackInt();
        queryTimeout = in.tryUnpackNil() ? 0 : in.unpackLong();
        idleTimeout = in.tryUnpackNil() ? 0 : in.unpackLong();
        timeZoneId = in.tryUnpackNil() ? null : in.unpackString();

        // Skip properties - not used by SQL engine.
        in.unpackInt(); // Number of properties.
        in.readBinaryUnsafe(); // Binary tuple with properties

        queryModifiers = unpackQueryModifiers
                ? QueryModifier.unpack(in.unpackByte())
                : QueryModifier.SINGLE_STMT_MODIFIERS;
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
        SqlProperties sqlProperties = new SqlProperties()
                .queryTimeout(queryTimeout)
                .allowedQueryTypes(ClientSqlCommon.convertQueryModifierToQueryType(queryModifiers))
                .allowMultiStatement(false);

        if (schema != null) {
            sqlProperties.defaultSchema(schema);
        }

        if (timeZoneId != null) {
            sqlProperties.timeZoneId(ZoneId.of(timeZoneId));
        }

        return sqlProperties;
    }
}
