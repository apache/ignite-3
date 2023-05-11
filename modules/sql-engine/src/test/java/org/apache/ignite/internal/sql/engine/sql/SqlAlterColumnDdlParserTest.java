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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.List;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlFormatOptions;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.ignite.lang.IgniteStringFormatter;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test suite to verify parsing of the ALTER TABLE ... ALTER COLUMN DDL commands.
 */
public class SqlAlterColumnDdlParserTest extends AbstractDdlParserTest {
    private static final String TABLE_NAME = "TEST_TABLE";
    private static final String COLUMN_NAME = "TEST_COLUMN";
    private static final String QUERY_PREFIX = IgniteStringFormatter.format("ALTER TABLE {} ALTER COLUMN {} ", TABLE_NAME, COLUMN_NAME);

    @Test
    public void testNotNull() {
        validateNotNull("SET NOT NULL", true);
        validateNotNull("DROP NOT NULL", false);
    }

    @Test
    public void testDefault() {
        validateDefault("SET DEFAULT 10", "10");
        validateDefault("DROP DEFAULT", null);
    }

    @Test
    public void testSetDataType() {
        validateDataType("SET DATA TYPE LONG", false, null, "LONG");
        validateDataType("SET DATA TYPE LONG DEFAULT -1", false, "-1", "LONG");
        validateDataType("SET DATA TYPE INTEGER NOT NULL", true, null, "INTEGER");
        validateDataType("SET DATA TYPE INTEGER NOT NULL DEFAULT -1", true, "-1", "INTEGER");
    }

    private void validateDataType(String querySuffix, boolean notNull, @Nullable String dflt, @Nullable String typeName) {
        IgniteSqlAlterTableAlterColumn alterColumn = parseAlterColumn(querySuffix);

        assertNotNull(alterColumn.dataType());

        assertThat(alterColumn.dataType().getTypeName().getSimple(), equalTo(typeName));
        assertThat(alterColumn.dataType().getNullable(), is(!notNull));

        if (dflt != null) {
            assertThat(alterColumn.strategy(), equalTo(ColumnStrategy.DEFAULT));
            assertThat(alterColumn.defaultExpression(), instanceOf(SqlLiteral.class));
            assertThat(((SqlLiteral) alterColumn.defaultExpression()).toValue(), equalTo(dflt));
        } else if (notNull) {
            assertThat(alterColumn.strategy(), equalTo(ColumnStrategy.NOT_NULLABLE));
        } else {
            assertThat(alterColumn.strategy(), equalTo(ColumnStrategy.NULLABLE));
        }
    }

    private void validateNotNull(String querySuffix, boolean notNull) {
        IgniteSqlAlterTableAlterColumn alterColumn = parseAlterColumn(querySuffix);

        assertNull(alterColumn.dataType());
        assertNull(alterColumn.defaultExpression());
        assertNotNull(alterColumn.strategy());

        switch (alterColumn.strategy()) {
            case NULLABLE:
                assertThat(notNull, is(false));
                break;

            case NOT_NULLABLE:
                assertThat(notNull, is(true));
                break;

            default:
                fail("Unexpected strategy: "+ alterColumn.strategy());
        }
    }

    private void validateDefault(String querySuffix, @Nullable String defaultValue) {
        IgniteSqlAlterTableAlterColumn alterColumn = parseAlterColumn(querySuffix);

        assertNull(alterColumn.dataType());
        assertThat(alterColumn.strategy(), equalTo(ColumnStrategy.DEFAULT));

        if (defaultValue == null) {
            assertNull(alterColumn.defaultExpression());
        } else {
            assertThat(alterColumn.defaultExpression(), instanceOf(SqlLiteral.class));
            assertThat(((SqlLiteral) alterColumn.defaultExpression()).toValue(), equalTo(defaultValue));
        }
    }

    private IgniteSqlAlterTableAlterColumn parseAlterColumn(String querySuffix) {
        String query = QUERY_PREFIX + querySuffix;

        SqlNode node = parse(query);
        assertThat(node, instanceOf(IgniteSqlAlterTableAlterColumn.class));

        IgniteSqlAlterTableAlterColumn alterColumn = (IgniteSqlAlterTableAlterColumn) node;

        assertThat(alterColumn.name().names, is(List.of(TABLE_NAME)));
        assertThat(alterColumn.columnName().getSimple(), equalTo(COLUMN_NAME));

        // Validate unparsed expression.
        assertThat(unparse(node), equalTo(query));

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
