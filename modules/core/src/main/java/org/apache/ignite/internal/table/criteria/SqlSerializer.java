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
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.table.criteria.Column;
import org.apache.ignite.table.criteria.Criteria;
import org.apache.ignite.table.criteria.CriteriaVisitor;
import org.apache.ignite.table.criteria.Expression;
import org.apache.ignite.table.criteria.Operator;
import org.apache.ignite.table.criteria.Parameter;
import org.jetbrains.annotations.Nullable;

/**
 * Serializes {@link Criteria} into into SQL.
 */
public class SqlSerializer implements CriteriaVisitor<Void> {
    private static final Map<Operator, String> ELEMENT_TEMPLATES = Map.of(
            Operator.EQ, "{0} = {1}",
            Operator.IS_NULL, "{0} IS NULL",
            Operator.IS_NOT_NULL, "{0} IS NOT NULL",
            Operator.GOE, "{0} >= {1}",
            Operator.GT, "{0} > {1}",
            Operator.LOE, "{0} <= {1}",
            Operator.LT, "{0} < {1}",
            Operator.NOT, "NOT {0}"
    );

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
    public <T> void visit(Parameter<T> argument, @Nullable Void context) {
        append("?");

        arguments.add(argument.getValue());
    }

    /** {@inheritDoc} */
    @Override
    public <T> void visit(Column column, @Nullable Void context) {
        append(column.getName());
    }

    /** {@inheritDoc} */
    @Override
    public <T> void visit(Expression expression, @Nullable Void context) {
        var operator = expression.getOperator();
        var elements = expression.getElements();

        String template;

        if (operator == Operator.AND || operator == Operator.OR) {
            var delimiter = operator == Operator.AND ? ") AND (" : ") OR (";

            template = IntStream.range(0, elements.length)
                    .mapToObj(i -> String.format("{%d}", i))
                    .collect(Collectors.joining(delimiter, "(", ")"));
        } else if (operator == Operator.IN || operator == Operator.NOT_IN) {
            var prefix = operator == Operator.IN ? "{0} IN (" : "{0} NOT (";

            template = IntStream.range(1, elements.length)
                    .mapToObj(i -> String.format("{%d}", i))
                    .collect(Collectors.joining(", ", prefix, ")"));
        } else {
            template = ELEMENT_TEMPLATES.get(operator);
        }

        int end = 0;
        var matcher = Pattern.compile("\\{(\\d+)\\}").matcher(template);

        while (matcher.find()) {
            if (matcher.start() > end) {
                append(template.substring(end, matcher.start()));
            }

            int index = Integer.parseInt(matcher.group(1));
            elements[index].accept(this, context);

            end = matcher.end();
        }

        if (end < template.length()) {
            append(template.substring(end));
        }
    }

    /** {@inheritDoc} */
    @Override
    public <T> void visit(Criteria criteria, @Nullable Void context) {
        criteria.accept(this, context);
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
        private Set<String> columnNames;

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
         * Sets the valid table column names to prevent SQL injection.
         *
         * @param columnNames Acceptable columns names.
         * @return This builder instance.
         */
        public SqlSerializer.Builder columns(Set<String> columnNames) {
            this.columnNames = columnNames;

            return this;
        }

        /**
         * Set the given criteria.
         *
         * @param where where condition.
         */
        public SqlSerializer.Builder where(@Nullable Criteria where) {
            this.where = where;

            return this;
        }

        /**
         * Builds the SQL query.
         *
         * @return SQL query text and arguments.
         */
        public SqlSerializer build() {
            if (nullOrBlank(tableName)) {
                throw new IllegalArgumentException("Table name can't be null or blank");
            }

            var ser = new SqlSerializer()
                    .append("SELECT * ")
                    .append("FROM ").append(tableName);

            if (where != null) {
                if (CollectionUtils.nullOrEmpty(columnNames)) {
                    throw new IllegalArgumentException("The columns of the table must be specified to prevent SQL injection");
                }

                ColumnValidator.INSTANCE.visit(where, columnNames);

                ser.append(" WHERE ");
                ser.visit(where, null);
            }

            return ser;
        }
    }
}
