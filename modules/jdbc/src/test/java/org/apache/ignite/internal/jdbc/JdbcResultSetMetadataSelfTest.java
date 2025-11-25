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

package org.apache.ignite.internal.jdbc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ColumnMetadataImpl.ColumnOriginImpl;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSet;
import org.apache.ignite.sql.ResultSetMetadata;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link JdbcResultSetMetadata}.
 */
public class JdbcResultSetMetadataSelfTest extends JdbcResultSetMetadataBaseSelfTest {

    @Test
    @Override
    public void unwrapAndIsWrapperFor() throws SQLException {
        ResultSetMetaData md = createMeta(List.of(COLUMN));
        {
            assertTrue(md.isWrapperFor(ResultSetMetaData.class));
            assertDoesNotThrow(() -> md.unwrap(ResultSetMetaData.class));
        }

        {
            assertTrue(md.isWrapperFor(JdbcResultSetMetadata.class));
            assertDoesNotThrow(() -> md.unwrap(JdbcResultSetMetadata.class));
        }

        {
            assertFalse(md.isWrapperFor(ResultSet.class));
            SQLException err = assertThrows(SQLException.class, () -> md.unwrap(ResultSet.class));
            assertThat(err.getMessage(), containsString("Result set meta data is not a wrapper for " + ResultSet.class.getName()));
        }
    }

    @Override
    protected ResultSetMetaData createMeta(List<ColumnDefinition> columns) {
        List<ColumnMetadata> columnsMeta = new ArrayList<>();

        for (ColumnDefinition s : columns) {
            ColumnOriginImpl origin;
            if (s.schema != null) {
                origin = new ColumnOriginImpl(s.schema, s.table, s.column);
            } else {
                origin = null;
            }

            columnsMeta.add(new ColumnMetadataImpl(s.label, s.type, s.precision, s.scale, s.nullable, origin));
        }
        ResultSetMetadata apiMeta = new ResultSetMetadataImpl(columnsMeta);
        return new JdbcResultSetMetadata(apiMeta);
    }
}
