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
import static org.apache.ignite.lang.util.IgniteNameUtils.canonicalOrSimpleName;
import static org.apache.ignite.lang.util.IgniteNameUtils.quote;
import static org.apache.ignite.lang.util.IgniteNameUtils.quoteIfNeeded;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
 *
 * <p>Note: Doesn't required any context to traverse an criteria tree, {@code null} can be used as initial context.
 */
public class SqlSerializer implements CriteriaVisitor<Void> {
    private static final Map<Operator, String> ELEMENT_TEMPLATES = Map.of(
            Operator.EQ, "{0} = {1}",
            Operator.NOT_EQ, "{0} <> {1}",
            Operator.IS_NULL, "{0} IS NULL",
            Operator.IS_NOT_NULL, "{0} IS NOT NULL",
            Operator.GOE, "{0} >= {1}",
            Operator.GT, "{0} > {1}",
            Operator.LOE, "{0} <= {1}",
            Operator.LT, "{0} < {1}",
            Operator.NOT, "NOT ({0})"
    );

    private final Pattern pattern = Pattern.compile("\\{(\\d+)\\}");

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
        append(quoteIfNeeded(column.getName()));
    }

    /** {@inheritDoc} */
    @Override
    public <T> void visit(Expression expression, @Nullable Void context) {
        Operator operator = expression.getOperator();
        Criteria[] elements = expression.getElements();

        if (operator == Operator.AND || operator == Operator.OR) {
            append(operator == Operator.AND ? ") AND (" : ") OR (", "(", ")", elements, context);
        } else if (operator == Operator.IN || operator == Operator.NOT_IN) {
            elements[0].accept(this, context);
            append(operator == Operator.IN ? " IN " : " NOT IN ");

            Criteria[] tail = Arrays.copyOfRange(elements, 1, elements.length);
            append(", ", "(", ")", tail, context);
        } else {
            String template = ELEMENT_TEMPLATES.get(operator);

            int end = 0;
            Matcher matcher = pattern.matcher(template);

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

    private void append(String delimiter, String prefix, String suffix, Criteria[] elements, @Nullable Void context) {
        if (elements.length > 1) {
            append(prefix);
        }

        for (int i = 0; i < elements.length; i++) {
            elements[i].accept(this, context);

            if (i < elements.length - 1) {
                append(delimiter);
            }
        }

        if (elements.length > 1) {
            append(suffix);
        }
    }

    /**
     * Builder.
     */
    public static class Builder  {
        @Nullable
        private String tableName;

        @Nullable
        private Collection<String> columnNames;

        @Nullable
        private String indexName;

        @Nullable
        private Criteria where;

        /**
         * Sets the table name. Must be unquoted name or name is cast to upper case.
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
         * @param columnNames Acceptable columns names. Must be unquoted name or name is cast to upper case.
         * @return This builder instance.
         */
        public SqlSerializer.Builder columns(Collection<String> columnNames) {
            this.columnNames = columnNames;

            return this;
        }

        /**
         * Set the given criteria.
         *
         * @param indexName The predicate to filter entries or {@code null} to return all entries from the underlying table.
         */
        public SqlSerializer.Builder indexName(@Nullable String indexName) {
            this.indexName = indexName;

            return this;
        }

        /**
         * Set the given criteria.
         *
         * @param where The predicate to filter entries or {@code null} to return all entries from the underlying table.
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

            SqlSerializer ser = new SqlSerializer()
                    .append("SELECT");

            if (!nullOrBlank(indexName)) {
                if (!canonicalOrSimpleName(indexName)) {
                    throw new IllegalArgumentException("Index name must be alphanumeric with underscore and start with letter. Was: "
                            + indexName);
                }

                ser.append(" /*+ FORCE_INDEX(").append(normalizeIndexName(indexName)).append(") */");
            }

            ser.append(" * FROM ").append(quoteIfNeeded(tableName));

            if (where != null) {
                if (CollectionUtils.nullOrEmpty(columnNames)) {
                    throw new IllegalArgumentException("The columns of the table must be specified to validate input");
                }

                ColumnValidator.INSTANCE.visit(where, columnNames);

                ser.append(" WHERE ");
                ser.visit(where, null);
            }

            return ser;
        }

        private static String normalizeIndexName(String name) {
            return quote(name.toUpperCase(Locale.ROOT));
        }
    }
}
