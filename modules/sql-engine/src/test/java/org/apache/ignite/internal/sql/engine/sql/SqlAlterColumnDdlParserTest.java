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

import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.assertThrowsSqlException;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.ignite.lang.ErrorGroups.Sql;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test suite to verify parsing of the {@code ALTER TABLE ... ALTER COLUMN} DDL commands.
 */
public class SqlAlterColumnDdlParserTest extends AbstractParserTest {

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
        IgniteSqlAlterColumn alterColumn = parseAlterColumn("ALTER TABLE t ALTER COLUMN a SET NOT NULL");
        assertThat(alterColumn.notNull(), is(true));
        expectUnparsed(alterColumn, "ALTER TABLE \"T\" ALTER COLUMN \"A\" SET NOT NULL");

        alterColumn = parseAlterColumn("ALTER TABLE t ALTER COLUMN a DROP NOT NULL");
        assertThat(alterColumn.notNull(), is(false));
        expectUnparsed(alterColumn, "ALTER TABLE \"T\" ALTER COLUMN \"A\" DROP NOT NULL");
    }

    /**
     * Verifies parsing of {@code ALTER TABLE ... ALTER COLUMN ... SET/DROP DEFAULT} statement.
     *
     * <p>The parser is expected to produce a node of {@link IgniteSqlAlterColumn} class with the specified table name and the
     * column name.
     * <ul>
     *     <li>Command {@code DROP DEFAULT} must be equivalent to {@code SET DEFAULT NULL}, and {@link IgniteSqlAlterColumn#expression()}
     *         in this case must contain SQL literal with type NULL.</li>
     *     <li>For {@code SET DEFAULT &lt;LITERAL&gt;} {@link IgniteSqlAlterColumn#expression()} must contain expected SQL literal.</li>
     *     <li>For {@code SET DEFAULT &lt;ID&gt;} parser should throw an exception.</li>
     * </ul>
     */
    @Test
    public void testDefault() {
        IgniteSqlAlterColumn alterColumn = parseAlterColumn("ALTER TABLE t ALTER COLUMN a DROP DEFAULT");
        checkDefaultIsNull(alterColumn.expression());
        expectUnparsed(alterColumn, "ALTER TABLE \"T\" ALTER COLUMN \"A\" DROP DEFAULT");

        alterColumn = parseAlterColumn("ALTER TABLE t ALTER COLUMN a SET DEFAULT NULL");
        checkDefaultIsNull(alterColumn.expression());
        expectUnparsed(alterColumn, "ALTER TABLE \"T\" ALTER COLUMN \"A\" SET DEFAULT NULL");

        alterColumn = parseAlterColumn("ALTER TABLE t ALTER COLUMN a SET DEFAULT 10");
        SqlNode dflt = alterColumn.expression();
        assertThat(dflt, instanceOf(SqlLiteral.class));
        assertThat(((SqlLiteral) dflt).getValueAs(Integer.class), equalTo(10));
        expectUnparsed(alterColumn, "ALTER TABLE \"T\" ALTER COLUMN \"A\" SET DEFAULT 10");

        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \"FUNC\"",
                () -> parse("ALTER TABLE t ALTER COLUMN a SET DEFAULT FUNC"));
    }

    /**
     * Verifies parsing of {@code ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE} statement.
     *
     * <p>Parser must support the following syntax {@code SET DATA TYPE &lt;new_type&gt; [NOT NULL | NULL] [DEFAULT &lt;default value&gt;]}.
     *
     * <p>Parser is expected to produce a node of {@link IgniteSqlAlterColumn} class with the specified {@link
     * IgniteSqlAlterColumn#name() table name}, {@link IgniteSqlAlterColumn#columnName() column name}, column {@link
     * IgniteSqlAlterColumn#dataType() data type}, an optional {@link IgniteSqlAlterColumn#expression() default expression}, and an optional
     * {@link IgniteSqlAlterColumn#notNull() notNull flag}.
     */
    @Test
    public void testSetDataType() {
        IgniteSqlAlterColumn alterColumn = parseAlterColumn("ALTER TABLE t ALTER COLUMN c SET DATA TYPE INTEGER");
        expectDataType(alterColumn, "INTEGER", null, null);
        expectUnparsed(alterColumn, "ALTER TABLE \"T\" ALTER COLUMN \"C\" SET DATA TYPE INTEGER");

        alterColumn = parseAlterColumn("ALTER TABLE t ALTER COLUMN c SET DATA TYPE INTEGER NOT NULL");
        expectDataType(alterColumn, "INTEGER", true, null);
        expectUnparsed(alterColumn, "ALTER TABLE \"T\" ALTER COLUMN \"C\" SET DATA TYPE INTEGER NOT NULL");

        alterColumn = parseAlterColumn("ALTER TABLE t ALTER COLUMN c SET DATA TYPE INTEGER NULL");
        expectDataType(alterColumn, "INTEGER", false, null);
        expectUnparsed(alterColumn, "ALTER TABLE \"T\" ALTER COLUMN \"C\" SET DATA TYPE INTEGER NULL");

        alterColumn = parseAlterColumn("ALTER TABLE t ALTER COLUMN c SET DATA TYPE INTEGER DEFAULT -1");
        expectDataType(alterColumn, "INTEGER", null, -1);
        expectUnparsed(alterColumn, "ALTER TABLE \"T\" ALTER COLUMN \"C\" SET DATA TYPE INTEGER DEFAULT -1");

        alterColumn = parseAlterColumn("ALTER TABLE t ALTER COLUMN c SET DATA TYPE INTEGER NOT NULL DEFAULT -1");
        expectDataType(alterColumn, "INTEGER", true, -1);
        expectUnparsed(alterColumn, "ALTER TABLE \"T\" ALTER COLUMN \"C\" SET DATA TYPE INTEGER NOT NULL DEFAULT -1");

        alterColumn = parseAlterColumn("ALTER TABLE t ALTER COLUMN c SET DATA TYPE INTEGER DEFAULT NULL");
        expectDataType(alterColumn, "INTEGER", null, null);
        expectUnparsed(alterColumn, "ALTER TABLE \"T\" ALTER COLUMN \"C\" SET DATA TYPE INTEGER DEFAULT NULL");

        alterColumn = parseAlterColumn("ALTER TABLE t ALTER COLUMN c SET DATA TYPE INTEGER NULL DEFAULT NULL");
        expectDataType(alterColumn, "INTEGER", false, null);
        expectUnparsed(alterColumn, "ALTER TABLE \"T\" ALTER COLUMN \"C\" SET DATA TYPE INTEGER NULL DEFAULT NULL");

        assertThrowsSqlException(
                Sql.STMT_PARSE_ERR,
                "Failed to parse query: Encountered \"FUNC\"",
                () -> parse("ALTER TABLE t ALTER COLUMN a SET DATA TYPE INTEGER DEFAULT FUNC"));
    }

    private void expectDataType(IgniteSqlAlterColumn alterColumn,
            @Nullable String typeName, @Nullable Boolean notNull, @Nullable Object expDefault) {

        assertNotNull(alterColumn.dataType());
        assertThat(alterColumn.dataType().getTypeName().getSimple(), equalTo(typeName));
        assertThat(alterColumn.notNull(), equalTo(notNull));

        if (expDefault == null) {
            if (alterColumn.expression() != null) {
                checkDefaultIsNull(alterColumn.expression());
            }
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
        return (IgniteSqlAlterColumn) parse(querySuffix);
    }
}
