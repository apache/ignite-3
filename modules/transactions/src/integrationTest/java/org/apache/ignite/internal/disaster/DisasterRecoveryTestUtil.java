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

package org.apache.ignite.internal.disaster;

import static java.time.Duration.ofSeconds;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.awaitility.Awaitility.await;

import java.util.Set;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.schema.row.RowAssembler;
import org.apache.ignite.internal.table.InternalTable;
import org.awaitility.core.ConditionTimeoutException;

/**
 * Utility class for disaster recovery tests.
 */
class DisasterRecoveryTestUtil {
    static void assertValueOnSpecificNodes(
            String tableName,
            Set<IgniteImpl> nodes,
            int id,
            int val,
            SchemaDescriptor schema
    ) {
        for (IgniteImpl node : nodes) {
            assertValueOnSpecificNode(tableName, node, id, val, schema);
        }
    }

    static void assertValueOnSpecificNode(String tableName, IgniteImpl node, int id, int val, SchemaDescriptor schema) {
        InternalTable internalTable = unwrapTableViewInternal(node.tables().table(tableName)).internalTable();

        Row keyValueRow0 = createKeyValueRow(schema, id, val);
        Row keyRow0 = createKeyRow(schema, id);

        try {
            await()
                    .atMost(ofSeconds(20))
                    .until(() -> {
                        BinaryRow actual = internalTable.get(keyRow0, node.clock().now(), node.node())
                                .join();

                        return compareRows(actual, keyValueRow0);
                    });
        } catch (ConditionTimeoutException e) {
            throw new AssertionError(
                    String.format("Row comparison failed within the timeout. [node={}, id={}, val={}]", node.name(), id, val),
                    e
            );
        }
    }

    static Row createKeyValueRow(SchemaDescriptor schema, int id, int value) {
        RowAssembler rowBuilder = new RowAssembler(schema, -1);

        rowBuilder.appendInt(id);
        rowBuilder.appendInt(value);

        return Row.wrapBinaryRow(schema, rowBuilder.build());
    }

    private static boolean compareRows(BinaryRow row1, BinaryRow row2) {
        return row1.schemaVersion() == row2.schemaVersion() && row1.tupleSlice().equals(row2.tupleSlice());
    }

    private static Row createKeyRow(SchemaDescriptor schema, int id) {
        RowAssembler rowBuilder = new RowAssembler(schema.version(), schema.keyColumns(), -1);

        rowBuilder.appendInt(id);

        return Row.wrapKeyOnlyBinaryRow(schema, rowBuilder.build());
    }
}
