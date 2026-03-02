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

class QueryContext {
    private static final int INDENT = 2;

    private final StringBuilder sql;

    private int indent;

    private boolean newline = false;

    QueryContext() {
        sql = new StringBuilder();
    }

    /**
     * Visit a QueryPart in the current context.
     * This method is called by certain QueryPart implementations to recursively visit nested parts.
     *
     * @param part The component.
     * @return current context.
     * @see AbstractCatalogQuery
     */
    QueryContext visit(QueryPart part) {
        part.accept(this);
        return this;
    }

    String getSql() {
        return sql.toString();
    }

    QueryContext sql(char c) {
        applyNewLine();
        sql.append(c);
        resetSeparatorFlags();
        return this;
    }

    QueryContext sql(String s) {
        applyNewLine();
        sql.append(s);
        resetSeparatorFlags();
        return this;
    }

    QueryContext sql(int i) {
        applyNewLine();
        sql.append(i);
        resetSeparatorFlags();
        return this;
    }

    QueryContext sql(double d) {
        applyNewLine();
        sql.append(d);
        resetSeparatorFlags();
        return this;
    }

    QueryContext formatSeparator() {
        newline = true;
        return this;
    }

    boolean isQuoteNames() {
        return false;
    }

    QueryContext sqlIndentStart(String s) {
        return sql(s).sqlIndentStart();
    }

    private QueryContext sqlIndentStart() {
        indent += INDENT;
        return this;
    }

    @SuppressWarnings("PMD.UnusedPrivateMethod") // Part of incomplete indentation feature
    private String indentation() {
        return indent == 0 ? "" : " ".repeat(indent);
    }

    private void applyNewLine() {
        if (newline) {
            sql.append(System.lineSeparator());
        }
    }

    private void resetSeparatorFlags() {
        newline = false;
    }
}
