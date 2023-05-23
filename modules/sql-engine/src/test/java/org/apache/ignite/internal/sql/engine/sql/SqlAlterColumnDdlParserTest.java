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
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.pretty.SqlFormatOptions;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.lang.IgniteStringFormatter;
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
     * <p>The parser is expected to produce a node of {@link IgniteSqlAlterColumnNotNull} class with the specified table name and the
     * column name.
     * For the {@code SET NOT NULL} statement, {@link IgniteSqlAlterColumnNotNull#notNull()} must return {@code true}.
     * For the {@code DROP NOT NULL} statement, {@link IgniteSqlAlterColumnNotNull#notNull()} must return {@code false}.
     */
    @Test
    public void testNotNull() {
        Class<IgniteSqlAlterColumnNotNull> expCls = IgniteSqlAlterColumnNotNull.class;

        assertThat(parseSingleAction("SET NOT NULL", expCls).notNull(), is(true));
        assertThat(parseSingleAction("DROP NOT NULL", expCls).notNull(), is(false));
    }

    /**
     * Verifies parsing of {@code ALTER TABLE ... ALTER COLUMN ... SET/DROP DEFAULT} statement.
     *
     * <p>The parser is expected to produce a node of {@link IgniteSqlAlterColumnDefault} class with the specified table name and the
     * column name.
     * For {@code SET DEFAULT 'EXPRESSION'}, {@link IgniteSqlAlterColumnDefault#expression()} must return expected default expression.
     * For {@code DROP DEFAULT}, {@link IgniteSqlAlterColumnDefault#expression()} must return {@code null}.
     */
    @Test
    public void testDefault() {
        Class<IgniteSqlAlterColumnDefault> expCls = IgniteSqlAlterColumnDefault.class;

        assertNull(parseSingleAction("DROP DEFAULT", expCls).expression());

        SqlNode dflt = parseSingleAction("SET DEFAULT 10", expCls).expression();
        assertThat(dflt, instanceOf(SqlLiteral.class));
        assertThat(((SqlLiteral) dflt).getValueAs(Integer.class), equalTo(10));
    }

    /**
     * Verifies parsing of {@code ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE} statement.
     *
     * <p>The parser is expected to produce a node of {@link IgniteSqlAlterColumnType} class with the specified {@link
     * IgniteSqlAlterColumn#name() table name}, {@link IgniteSqlAlterColumn#columnName() column name} and column {@link
     * IgniteSqlAlterColumnType#dataType() data type}.
     */
    @Test
    @SuppressWarnings("ThrowableNotThrown")
    public void testSetDataType() {
        Class<IgniteSqlAlterColumnType> expCls = IgniteSqlAlterColumnType.class;
        String query = "SET DATA TYPE LONG";

        IgniteSqlAlterColumnType alterColumn = parseSingleAction(query, expCls);

        assertNotNull(alterColumn.dataType());
        assertThat(alterColumn.dataType().getTypeName().getSimple(), equalTo("LONG"));

        IgniteTestUtils.assertThrowsWithCause(() -> parseSingleAction(query + " NOT NULL", expCls), SqlParseException.class, "Encountered");
        IgniteTestUtils.assertThrowsWithCause(() -> parseSingleAction(query + " DEFAULT 1", expCls), SqlParseException.class, "Encountered");
    }

    /**
     * Verifies parsing of multiple comma-separated alter column actions.
     *
     * <p>The parser must support the following syntax:
     * {@code ALTER TABLE &lt;table_name&gt; ALTER COLUMN &lt;col_name&gt; &lt;action1, action2, ...&gt;}.
     */
    @Test
    public void testCommaSeparatedActions() {
        String query = "SET DATA TYPE FLOAT, SET DATA TYPE DOUBLE, SET NOT NULL, DROP NOT NULL, SET DEFAULT 1, DROP DEFAULT";

        IgniteSqlAlterColumn alterColumn = parseAlterColumn(query);

        assertThat(alterColumn.actions().size(), equalTo(6));

        SqlNode action = alterColumn.actions().get(0);
        assertThat(action, instanceOf(IgniteSqlAlterColumnType.class));
        assertThat(((IgniteSqlAlterColumnType)action).dataType().getTypeName().getSimple(), equalTo("FLOAT"));

        action = alterColumn.actions().get(1);
        assertThat(action, instanceOf(IgniteSqlAlterColumnType.class));
        assertThat(((IgniteSqlAlterColumnType)action).dataType().getTypeName().getSimple(), equalTo("DOUBLE"));

        action = alterColumn.actions().get(2);
        assertThat(action, instanceOf(IgniteSqlAlterColumnNotNull.class));
        assertThat(((IgniteSqlAlterColumnNotNull)action).notNull(), is(true));

        action = alterColumn.actions().get(3);
        assertThat(action, instanceOf(IgniteSqlAlterColumnNotNull.class));
        assertThat(((IgniteSqlAlterColumnNotNull)action).notNull(), is(false));

        action = alterColumn.actions().get(4);
        assertThat(action, instanceOf(IgniteSqlAlterColumnDefault.class));
        assertThat(((SqlLiteral)((IgniteSqlAlterColumnDefault)action).expression()).getValueAs(Float.class), equalTo(1.0f));

        action = alterColumn.actions().get(5);
        assertThat(action, instanceOf(IgniteSqlAlterColumnDefault.class));
        assertNull(((IgniteSqlAlterColumnDefault) action).expression());
    }

    private <T extends IgniteSqlAlterColumnAction> T parseSingleAction(String querySuffix, Class<T> cls) {
        IgniteSqlAlterColumn alterColumn = parseAlterColumn(querySuffix);

        assertNotNull(alterColumn.actions());
        assertThat(alterColumn.actions().size(), equalTo(1));

        SqlNode action = alterColumn.actions().iterator().next();
        assertThat(action, instanceOf(cls));

        return (T) action;
    }

    private IgniteSqlAlterColumn parseAlterColumn(String querySuffix) {
        String query = QUERY_PREFIX + querySuffix;

        SqlNode node = parse(query);
        assertThat(node, instanceOf(IgniteSqlAlterColumn.class));

        IgniteSqlAlterColumn alterColumn = (IgniteSqlAlterColumn) node;

        assertThat(alterColumn.name().names, is(List.of(TABLE_NAME)));
        assertThat(alterColumn.columnName().getSimple(), equalTo(COLUMN_NAME));
        assertNotNull(alterColumn.actions());
        assertThat(alterColumn.actions().size(), greaterThan(0));

        // Validate unparsed expression.
        assertThat(unparse(alterColumn), equalTo(query));

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
