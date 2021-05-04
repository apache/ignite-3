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

package org.apache.ignite.internal.table.distributed.raft;

import java.util.Iterator;
import java.util.UUID;
import org.apache.ignite.internal.schema.ByteBufferRow;
import org.apache.ignite.internal.schema.Column;
import org.apache.ignite.internal.schema.NativeTypes;
import org.apache.ignite.internal.schema.Row;
import org.apache.ignite.internal.schema.RowAssembler;
import org.apache.ignite.internal.schema.SchemaDescriptor;
import org.apache.ignite.internal.table.distributed.command.GetCommand;
import org.apache.ignite.internal.table.distributed.command.InsertCommand;
import org.apache.ignite.internal.table.distributed.command.response.SingleRowResponse;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * There are a tests for a table command listener.
 */
public class PartitionCommandListenerTest {
    /** Key count. */
    public static final int KEY_COUNT = 100;

    /** Schema. */
    public static SchemaDescriptor SCHEMA = new SchemaDescriptor(UUID.randomUUID(),
        1,
        new Column[] {new Column("key", NativeTypes.INTEGER, false)},
        new Column[] {new Column("value", NativeTypes.INTEGER, false)}
    );

    /** Table command listener. */
    private static PartitionCommandListener commandListener;

    /**
     * Inisializes a table listener before tests.
     */
    @BeforeAll
    public static void before() {
        commandListener = new PartitionCommandListener();
    }

    @Test
    public void testInsertCommands() {
        readValues(false);

        insertValuse(false);
        insertValuse(true);

        readValues(true);
    }

    private void readValues(boolean existed) {
        commandListener.onRead(new Iterator<CommandClosure<ReadCommand>>() {
            /** Iteration. */
            private int i = 0;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return i < KEY_COUNT;
            }

            /** {@inheritDoc} */
            @Override public CommandClosure<ReadCommand> next() {
                CommandClosure<ReadCommand> clo = mock(CommandClosure.class);

                when(clo.command()).thenReturn(new GetCommand(getTestKey(i)));

                doAnswer(invocation -> {
                    assert false : "Exception happened: " + invocation.getArgument(0);

                    return null;
                }).when(clo).failure(any());

                doAnswer(invocation -> {
                    SingleRowResponse resp = invocation.getArgument(0);

                    if (existed) {
                        assertNotNull(resp.getValue());

                        assertTrue(resp.getValue().hasValue());

                        Row row = new Row(SCHEMA, resp.getValue());

                        assertEquals(i - 1, row.intValue(0));
                        assertEquals(i - 1, row.intValue(1));
                    }
                    else
                        assertNull(resp.getValue());

                    return null;
                }).when(clo).success(any(SingleRowResponse.class));

                i++;

                return clo;
            }
        });
    }

    private void insertValuse(boolean existed) {
        commandListener.onWrite(new Iterator<CommandClosure<WriteCommand>>() {
            /** Iteration. */
            private int i = 0;

            /** {@inheritDoc} */
            @Override public boolean hasNext() {
                return i < KEY_COUNT;
            }

            /** {@inheritDoc} */
            @Override public CommandClosure<WriteCommand> next() {
                CommandClosure<WriteCommand> clo = mock(CommandClosure.class);

                when(clo.command()).thenReturn(new InsertCommand(getTestRow(i, i)));

                doAnswer(invocation -> {
                    assert false : "Exception happened: " + invocation.getArgument(0);

                    return null;
                }).when(clo).failure(any());

                doNothing().when(clo).success(!existed);

                i++;

                return clo;
            }
        });
    }

    @Test
    public void testReadCommands() {

    }

    /**
     * Prepares a test row which contains only key field.
     *
     * @return Row.
     */
    @NotNull private Row getTestKey(int key) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 4096, 0, 0);

        rowBuilder.appendInt(key);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.build()));
    }

    /**
     * Prepares a test row which contains key and value fields.
     *
     * @return Row.
     */
    @NotNull private Row getTestRow(int key, int val) {
        RowAssembler rowBuilder = new RowAssembler(SCHEMA, 4096, 0, 0);

        rowBuilder.appendInt(key);
        rowBuilder.appendInt(val);

        return new Row(SCHEMA, new ByteBufferRow(rowBuilder.build()));
    }
}
