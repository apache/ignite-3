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

package org.apache.ignite.internal.sql.engine.sql;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.List;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlFormatOptions;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test suite to verify parsing of the {@code ALTER TABLE ... ALTER COLUMN} DDL commands.
 */
public class SqlAlterColumnDdlParserTest extends AbstractDdlParserTest {
    private static final String TABLE_NAME = "TEST_TABLE";
    private static final String COLUMN_NAME = "TEST_COLUMN";
    private static final String QUERY_PREFIX = IgniteStringFormatter.format("ALTER TABLE {} ALTER COLUMN {} ", TABLE_NAME, COLUMN_NAME);

    /**
     * Verifies parsing of {@code ALTER TABLE ... ALTER COLUMN ... SET/DROP NOT NULL} statement.
     *
     * <p>The parser is expected to produce a node of {@link IgniteSqlAlterColumn} class with the specified table name and the
     * column name.
     * For the {@code SET NOT NULL} statement, {@link IgniteSqlAlterColumn#notNull()} must return {@code true}.
     * For the {@code DROP NOT NULL} statement, {@link IgniteSqlAlterColumn#notNull()} must return {@code false}.
     */
    @Test
    public void testNotNull() {
        assertThat(parseAlterColumn("SET NOT NULL").notNull(), is(true));
        assertThat(parseAlterColumn("DROP NOT NULL").notNull(), is(false));
    }

    /**
     * Verifies parsing of {@code ALTER TABLE ... ALTER COLUMN ... SET/DROP DEFAULT} statement.
     *
     * <p>The parser is expected to produce a node of {@link IgniteSqlAlterColumn} class with the specified table name and the
     * column name.
     * For {@code SET DEFAULT 'EXPRESSION'}, {@link IgniteSqlAlterColumn#expression()} must return expected default expression.
     * For {@code DROP DEFAULT}, {@link IgniteSqlAlterColumn#expression()} must return {@code null}.
     */
    @Test
    public void testDefault() {
        checkDefaultIsNull(parseAlterColumn("DROP DEFAULT").expression());
        checkDefaultIsNull(parseAlterColumn("SET DEFAULT NULL", "DROP DEFAULT").expression());

        SqlNode dflt = parseAlterColumn("SET DEFAULT 10").expression();
        assertThat(dflt, instanceOf(SqlLiteral.class));
        assertThat(((SqlLiteral) dflt).getValueAs(Integer.class), equalTo(10));

        dflt = parseAlterColumn("SET DEFAULT FUNC").expression();
        assertThat(dflt, instanceOf(SqlIdentifier.class));
        assertThat(((SqlIdentifier) dflt).getSimple(), equalTo("FUNC"));
    }

    /**
     * Verifies parsing of {@code ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE} statement.
     *
     * <p>The parser is expected to produce a node of {@link IgniteSqlAlterColumn} class with the specified {@link
     * IgniteSqlAlterColumn#name() table name}, {@link IgniteSqlAlterColumn#columnName() column name}, column {@link
     * IgniteSqlAlterColumn#dataType() data type} and an optional {@link IgniteSqlAlterColumn#expression() default expression}.
     */
    @Test
    public void testSetDataType() {
        validateDataType("SET DATA TYPE INTEGER", "INTEGER", null, null);
        validateDataType("SET DATA TYPE INTEGER NOT NULL", "INTEGER", true, null);
        validateDataType("SET DATA TYPE INTEGER NULL", "INTEGER", false, null);
        validateDataType("SET DATA TYPE INTEGER DEFAULT -1", "INTEGER", null, -1L);
        validateDataType("SET DATA TYPE INTEGER DEFAULT FUNC", "INTEGER", null, "FUNC");
        validateDataType("SET DATA TYPE INTEGER DEFAULT NULL", "INTEGER", null, null);
        validateDataType("SET DATA TYPE INTEGER NOT NULL DEFAULT -1", "INTEGER", true, -1);
        validateDataType("SET DATA TYPE INTEGER NULL DEFAULT NULL", "INTEGER", false, null);
    }

    private void validateDataType(String querySuffix, @Nullable String typeName, @Nullable Boolean notNull, @Nullable Object expDefault) {
        IgniteSqlAlterColumn alterColumn = parseAlterColumn(querySuffix);

        assertNotNull(alterColumn.dataType());
        assertThat(alterColumn.dataType().getTypeName().getSimple(), equalTo(typeName));
        assertThat(alterColumn.notNull(), equalTo(notNull));

        if (expDefault == null) {
            if (alterColumn.expression() != null) {
                checkDefaultIsNull(alterColumn.expression());
            }
        } else if (alterColumn.expression() instanceof SqlIdentifier) {
            assertThat(((SqlIdentifier) alterColumn.expression()).getSimple(), equalTo(expDefault));
        } else {
            assertThat(alterColumn.expression(), instanceOf(SqlLiteral.class));
            assertThat(((SqlLiteral) alterColumn.expression()).getValueAs(expDefault.getClass()), equalTo(expDefault));
        }
    }

    private void checkDefaultIsNull(@Nullable SqlNode dflt) {
        assertNotNull(dflt);
        assertThat(dflt, instanceOf(SqlLiteral.class));
        assertThat(((SqlLiteral) dflt).getTypeName(), equalTo(SqlTypeName.NULL));
    }

    private IgniteSqlAlterColumn parseAlterColumn(String querySuffix) {
        return parseAlterColumn(querySuffix, null);
    }

    private IgniteSqlAlterColumn parseAlterColumn(String querySuffix, @Nullable String unparseQuerySuffix) {
        String query = QUERY_PREFIX + querySuffix;

        SqlNode node = parse(query);
        assertThat(node, instanceOf(IgniteSqlAlterColumn.class));

        IgniteSqlAlterColumn alterColumn = (IgniteSqlAlterColumn) node;

        assertThat(alterColumn.name().names, is(List.of(TABLE_NAME)));
        assertThat(alterColumn.columnName().getSimple(), equalTo(COLUMN_NAME));

        // Validate unparsed expression.
        assertThat(unparse(alterColumn), equalTo(unparseQuerySuffix == null ? query : QUERY_PREFIX + unparseQuerySuffix));

        return alterColumn;
    }

    private String unparse(SqlNode node) {
        SqlPrettyWriter writer = new SqlPrettyWriter();
        SqlFormatOptions opts = new SqlFormatOptions();

        opts.setQuoteAllIdentifiers(false);
        writer.setFormatOptions(opts);

        node.unparse(writer, 0, 0);

        return writer.toString();
    }
}
