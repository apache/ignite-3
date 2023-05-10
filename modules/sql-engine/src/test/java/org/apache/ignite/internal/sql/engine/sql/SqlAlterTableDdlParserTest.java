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

import java.util.List;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.ddl.SqlColumnDeclaration;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;

/**
 * Test suite to verify parsing of the ALTER TABLE DDL commands.
 */
public class SqlAlterTableDdlParserTest extends AbstractDdlParserTest {
    /**
     * Parsing of ALTER TABLE ALTER COLUMN statement.
     */
    @Test
    public void alterColumn() {
        checkAlterColumn("ALTER TABLE my_table ALTER COLUMN my_column SET DEFAULT 10;",
                "MY_TABLE", "MY_COLUMN", null, "10", null, true);

        checkAlterColumn("ALTER TABLE my_table ALTER COLUMN my_column DROP DEFAULT;",
                "MY_TABLE", "MY_COLUMN", null, null, null, false);

        checkAlterColumn("ALTER TABLE my_table ALTER COLUMN my_column SET NOT NULL;",
                "MY_TABLE", "MY_COLUMN", false, null, null, null);

        checkAlterColumn("ALTER TABLE my_table ALTER COLUMN my_column DROP NOT NULL;",
                "MY_TABLE", "MY_COLUMN", true, null, null, null);

        checkAlterColumn("ALTER TABLE my_table ALTER COLUMN my_column DATA TYPE LONG;",
                "MY_TABLE", "MY_COLUMN", true, null, "LONG", null);

        checkAlterColumn("ALTER TABLE my_table ALTER COLUMN my_column DATA TYPE LONG NOT NULL;",
                "MY_TABLE", "MY_COLUMN", false, null, "LONG", null);

        checkAlterColumn("ALTER TABLE my_table ALTER COLUMN my_column DATA TYPE LONG NOT NULL DEFAULT 5;",
                "MY_TABLE", "MY_COLUMN", false, "5", "LONG", null);
    }

    private void checkAlterColumn(
            String query,
            String tableName,
            String colName,
            @Nullable Boolean nullable,
            @Nullable String dflt,
            @Nullable String typeName,
            @Nullable Boolean changeDefault
    ) {
        SqlNode node = parse(query);
        assertThat(node, instanceOf(IgniteSqlAlterTableAlterColumn.class));

        IgniteSqlAlterTableAlterColumn alterColumn = (IgniteSqlAlterTableAlterColumn) node;

        assertThat(alterColumn.name().names, is(List.of(tableName)));
        assertNotNull(alterColumn.column());
        assertThat(alterColumn.column(), instanceOf(SqlColumnDeclaration.class));

        SqlColumnDeclaration col = (SqlColumnDeclaration) alterColumn.column();
        assertThat(col.name.getSimple(), equalTo(colName));

        if (dflt != null) {
            assertThat(col.strategy, equalTo(ColumnStrategy.DEFAULT));
            assertThat(col.expression, instanceOf(SqlLiteral.class));
            assertThat(((SqlLiteral) col.expression).toValue(), equalTo(dflt));

            if (typeName == null) {
                assertThat(alterColumn.defaultValue(), equalTo(changeDefault));
            } else {
                assertNull(alterColumn.defaultValue());
                assertNull(alterColumn.notNull());
            }
        } else {
            assertNull(col.expression);
        }

        if (typeName != null) {
            assertNotNull(col.dataType);

            assertThat(col.dataType.getTypeName().getSimple(), equalTo(typeName));
            assertNull(alterColumn.notNull());
            assertNull(alterColumn.defaultValue());
        } else {
            assertNull(col.dataType);
        }

        if (nullable != null) {
            if (typeName != null) {
                assertThat(col.dataType.getNullable(), equalTo(nullable));
                assertNull(alterColumn.notNull());
                assertNull(alterColumn.defaultValue());
            } else {
                assertThat(alterColumn.notNull(), equalTo(!nullable));
                assertNull(alterColumn.defaultValue());
            }
        }
    }
}
