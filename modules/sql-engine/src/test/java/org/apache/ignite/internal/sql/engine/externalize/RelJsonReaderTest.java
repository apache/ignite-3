/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.sql.engine.externalize;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.UUID;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Statistic;
import org.apache.ignite.internal.sql.engine.rel.AbstractIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteIndexScan;
import org.apache.ignite.internal.sql.engine.rel.IgniteTableScan;
import org.apache.ignite.internal.sql.engine.schema.IgniteIndex;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.SqlSchemaManager;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/**
 * Tests to verify {@link RelJsonReader} behaviour.
 */
public class RelJsonReaderTest {
    /**
     * Test verifies that during deserialization table being resolved by its ID.
     */
    @Test
    void fromJsonTblScan() {
        UUID tableId = UUID.randomUUID();

        IgniteTable igniteTableMock = mock(IgniteTable.class);
        when(igniteTableMock.getStatistic()).thenReturn(new Statistic() {});
        when(igniteTableMock.getRowType(any())).thenReturn(mock(RelDataType.class));

        SqlSchemaManager schemaMock = mock(SqlSchemaManager.class);
        when(schemaMock.tableById(tableId)).thenReturn(igniteTableMock);

        String json = ""
                + "{\n"
                + "  \"rels\" : [ {\n"
                + "    \"id\" : \"0\",\n"
                + "    \"relOp\" : \"IgniteTableScan\",\n"
                + "    \"table\" : [\"PUBLIC\", \"TEST\"],\n"
                + "    \"tableId\" : \"" + tableId + "\",\n"
                + "    \"inputs\" : [ ]\n"
                + "  } ]\n"
                + "}";

        RelNode node = RelJsonReader.fromJson(schemaMock, json);

        assertThat(node, isA(IgniteTableScan.class));
        assertThat(node.getTable(), notNullValue());
        assertThat(node.getTable().unwrap(IgniteTable.class), is(igniteTableMock));
        Mockito.verify(schemaMock).tableById(tableId);
    }

    @Test
    void fromJsonIdxScan() {
        UUID indexId = UUID.randomUUID();
        UUID tableId = UUID.randomUUID();

        IgniteTable igniteTableMock = mock(IgniteTable.class);
        IgniteIndex igniteIdxMock = mock(IgniteIndex.class);
        when(igniteTableMock.getStatistic()).thenReturn(new Statistic() {});
        when(igniteTableMock.getRowType(any())).thenReturn(mock(RelDataType.class));

        SqlSchemaManager schemaMock = mock(SqlSchemaManager.class);

        when(schemaMock.tableById(tableId)).thenReturn(igniteTableMock);
        when(schemaMock.indexById(indexId)).thenReturn(igniteIdxMock);

        String json = ""
                + "{\n"
                + "  \"rels\" : [ {\n"
                + "    \"id\" : \"0\",\n"
                + "    \"relOp\" : \"IgniteIndexScan\",\n"
                + "    \"index\" : \"idx0\",\n"
                + "    \"table\" : [\"PUBLIC\", \"TEST\"],\n"
                + "    \"indexId\" : \"" + indexId + "\",\n"
                + "    \"tableId\" : \"" + tableId + "\",\n"
                + "    \"inputs\" : [ ]\n"
                + "  } ]\n"
                + "}";

        RelNode node = RelJsonReader.fromJson(schemaMock, json);

        assertThat(node, isA(IgniteIndexScan.class));
        assertThat(node.getTable(), notNullValue());
        assertThat(node.getTable().unwrap(IgniteTable.class), is(igniteTableMock));

        AbstractIndexScan scan = (AbstractIndexScan) node;
        assertThat(scan.getIndex(), notNullValue());
        assertThat(scan.getIndex(), is(igniteIdxMock));

        Mockito.verify(schemaMock).tableById(tableId);
        Mockito.verify(schemaMock).indexById(indexId);
    }
}