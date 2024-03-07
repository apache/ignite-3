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

package org.apache.ignite.internal.catalog.sql;

import static org.apache.ignite.internal.catalog.sql.QueryUtils.isGreaterThanZero;

import java.sql.Time;
import java.sql.Timestamp;
import java.util.Date;
import java.util.UUID;
import org.apache.ignite.catalog.ColumnType;

class ColumnTypeImpl<T> extends QueryPart {

    private final ColumnType<T> wrapped;

    public static <T> ColumnTypeImpl<T> wrap(ColumnType<T> type) {
        return new ColumnTypeImpl<>(type);
    }

    private ColumnTypeImpl(ColumnType<T> type) {
        this.wrapped = type;
    }

    @Override
    protected void accept(QueryContext ctx) {
        ctx.sql(wrapped.typeName());

        if (isGreaterThanZero(wrapped.length())) {
            ctx.sql("(").sql(wrapped.length()).sql(")");
        } else if (isGreaterThanZero(wrapped.precision())) {
            ctx.sql("(").sql(wrapped.precision());
            if (isGreaterThanZero(wrapped.scale())) {
                ctx.sql(", ").sql(wrapped.scale());
            }
            ctx.sql(")");
        }

        if (wrapped.nullable() != null && !wrapped.nullable()) {
            ctx.sql(" NOT NULL");
        }

        if (wrapped.defaultValue() != null) {
            if (isNeedsQuotes(wrapped)) {
                ctx.sql(" DEFAULT '").sql(wrapped.defaultValue().toString()).sql("'");
            } else {
                ctx.sql(" DEFAULT ").sql(wrapped.defaultValue().toString());
            }
        } else if (wrapped.defaultExpression() != null) {
            ctx.sql(" DEFAULT ").sql(wrapped.defaultExpression());
        }
    }

    private static boolean isNeedsQuotes(ColumnType<?> type) {
        Class<?> typeClass = type.type();
        return String.class.equals(typeClass)
                || Date.class.equals(typeClass)
                || Time.class.equals(typeClass)
                || Timestamp.class.equals(typeClass)
                || byte[].class.equals(typeClass)
                || UUID.class.equals(typeClass);
    }
}
