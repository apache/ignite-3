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

package org.apache.ignite.internal.jdbc2;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.jdbc.ColumnDefinition;
import org.apache.ignite.internal.jdbc.JdbcResultSetMetadataBaseSelfTest;
import org.apache.ignite.internal.sql.ColumnMetadataImpl;
import org.apache.ignite.internal.sql.ColumnMetadataImpl.ColumnOriginImpl;
import org.apache.ignite.internal.sql.ResultSetMetadataImpl;
import org.apache.ignite.sql.ColumnMetadata;
import org.apache.ignite.sql.ResultSetMetadata;

/**
 * Tests for {@link JdbcResultSetMetadata}.
 */
public class JdbcResultSetMetadata2SelfTest extends JdbcResultSetMetadataBaseSelfTest {
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
