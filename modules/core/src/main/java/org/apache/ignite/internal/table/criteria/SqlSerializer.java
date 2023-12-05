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

package org.apache.ignite.internal.table.criteria;

import static org.apache.ignite.internal.util.StringUtils.nullOrBlank;

import java.util.LinkedList;
import java.util.List;
import org.apache.ignite.table.criteria.Argument;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.CriteriaVisitor;
import org.apache.ignite.table.criteria.StaticText;
import org.jetbrains.annotations.Nullable;

/**
 * Serializes {@link Criteria} into into SQL.
 */
public class SqlSerializer implements CriteriaVisitor<Void> {
    @SuppressWarnings("StringBufferField")
    private final StringBuilder builder = new StringBuilder(128);
    private final List<Object> arguments = new LinkedList<>();

    /**
     * Get query arguments.
     *
     * @return Query arguments.
     */
    public Object[] getArguments() {
        return arguments.toArray(new Object[0]);
    }

    /** {@inheritDoc} */
    @Override
    public <T> void visit(Argument<T> argument, @Nullable Void context) {
        append("?");

        arguments.add(argument.getValue());
    }

    /** {@inheritDoc} */
    @Override
    public <T> void visit(StaticText text, @Nullable Void context) {
        append(text.getText());
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return builder.toString();
    }

    private SqlSerializer append(String str) {
        builder.append(str);

        return this;
    }

    /**
     * Builder.
     */
    public static class Builder  {
        @Nullable
        private String tableName;

        @Nullable
        private Criteria where;

        /**
         * Sets the table name (the number of entries that will be sent to the cluster in one network call).
         *
         * @param tableName Table name.
         * @return This builder instance.
         */
        public SqlSerializer.Builder tableName(String tableName) {
            this.tableName = tableName;

            return this;
        }

        /**
         * Add the given where expressions.
         *
         * @param where where condition.
         */
        public SqlSerializer.Builder where(@Nullable Criteria where) {
            this.where = where;

            return this;
        }

        /**
         * Builds the options.
         *
         * @return Criteria query options.
         */
        public SqlSerializer build() {
            if (nullOrBlank(tableName)) {
                throw new IllegalArgumentException("Table name can't be null or blank");
            }

            var ser = new SqlSerializer()
                    .append("SELECT * FROM ").append(tableName).append(" ");

            if (where != null) {
                ser.append("WHERE ");

                where.accept(ser, null);
            }

            return ser;
        }
    }
}
