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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.calcite.rel.core.TableModify;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.UpdateableTable;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.util.Pair;
import org.jetbrains.annotations.Nullable;

/**
 * ModifyNode.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class ModifyNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT> {
    private final TableModify.Operation modifyOp;

    private final UpdateableTable table;

    private final @Nullable List<String> updateColumns;

    private final int @Nullable [] mapping;

    private List<RowT> rows = new ArrayList<>(MODIFY_BATCH_SIZE);

    private long updatedRows;

    private int waiting;

    private int requested;

    private boolean inLoop;

    private State state = State.UPDATING;

    /**
     * Constructor.
     *
     * @param ctx An execution context.
     * @param table A table to update.
     * @param op A type of the update operation.
     * @param updateColumn Enumeration of columns to update if applicable.
     */
    public ModifyNode(
            ExecutionContext<RowT> ctx,
            UpdateableTable table,
            TableModify.Operation op,
            @Nullable List<String> updateColumn
    ) {
        super(ctx);

        this.table = table;
        this.modifyOp = op;
        this.updateColumns = updateColumn;

        this.mapping = mapping(table.descriptor(), updateColumn);
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        if (!inLoop) {
            tryEnd();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;
        assert state == State.UPDATING;

        checkState();

        waiting--;

        rows.add(row);

        flushTuples(false);

        if (waiting == 0) {
            source().request(waiting = MODIFY_BATCH_SIZE);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting = -1;
        state = State.UPDATED;

        tryEnd();
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        if (idx != 0) {
            throw new IndexOutOfBoundsException();
        }

        return this;
    }

    private void tryEnd() throws Exception {
        assert downstream() != null;

        if (state == State.UPDATING && waiting == 0) {
            source().request(waiting = MODIFY_BATCH_SIZE);
        }

        if (state == State.UPDATED && requested > 0) {
            flushTuples(true);

            state = State.END;

            inLoop = true;
            try {
                requested--;
                downstream().push(context().rowHandler().factory(long.class).create(updatedRows));
            } finally {
                inLoop = false;
            }
        }

        if (state == State.END && requested > 0) {
            requested = 0;
            downstream().end();
        }
    }

    private void flushTuples(boolean force) {
        if (nullOrEmpty(rows) || (!force && rows.size() < MODIFY_BATCH_SIZE)) {
            return;
        }

        List<RowT> rows = this.rows;
        this.rows = new ArrayList<>(MODIFY_BATCH_SIZE);

        switch (modifyOp) {
            case INSERT:
                table.insertAll(context(), rows).join();

                break;
            case UPDATE:
                inlineUpdates(0, rows);

                table.upsertAll(context(), rows).join();

                break;
            case MERGE:
                Pair<List<RowT>, List<RowT>> split = splitMerge(rows);

                List<CompletableFuture<?>> mergeParts = new ArrayList<>(2);

                if (split.getFirst() != null) {
                    mergeParts.add(table.insertAll(context(), split.getFirst()));
                }

                if (split.getSecond() != null) {
                    mergeParts.add(table.upsertAll(context(), split.getSecond()));
                }

                CompletableFuture.allOf(
                        mergeParts.toArray(CompletableFuture[]::new)
                ).join();

                break;
            case DELETE:
                table.deleteAll(context(), rows).join();

                break;
            default:
                throw new UnsupportedOperationException(modifyOp.name());
        }

        updatedRows += rows.size();
    }

    /** See {@link #mapping(TableDescriptor, List)}. */
    private void inlineUpdates(int offset, List<RowT> rows) {
        if (mapping == null) {
            return;
        }

        assert updateColumns != null;

        RowHandler<RowT> handler = context().rowHandler();

        int rowSize = handler.columnCount(rows.get(0));
        int updateColumnOffset = hasUpsertSemantic(rowSize) ? rowSize - (mapping.length + updateColumns.size()) : 0;

        for (RowT row : rows) {
            for (int i = 0; i < mapping.length; i++) {
                if (offset == 0 && i == mapping[i]) {
                    continue;
                }

                handler.set(i, row, handler.get(mapping[i] + updateColumnOffset, row));
            }
        }
    }

    /**
     * Splits the rows of merge operation onto rows that should be inserted and rows that should be updated.
     *
     * @param rows Rows to split.
     * @return Pair where first element is list of rows to insert (or null if there is no such rows), and second
     *     element is list of rows to update (or null if there is no such rows).
     */
    private Pair<List<RowT>, List<RowT>> splitMerge(List<RowT> rows) {
        if (nullOrEmpty(updateColumns)) {
            // WHEN NOT MATCHED clause only
            return new Pair<>(rows, null);
        }

        assert mapping != null;

        List<RowT> rowsToInsert = null;
        List<RowT> rowsToUpdate = null;

        RowHandler<RowT> handler = context().rowHandler();
        int rowSize = handler.columnCount(rows.get(0));

        int updateColumnOffset = rowSize - (mapping.length + updateColumns.size());

        if (!hasUpsertSemantic(rowSize)) {
            // WHEN MATCHED clause only
            rowsToUpdate = rows;
        } else {
            rowsToInsert = new ArrayList<>();
            rowsToUpdate = new ArrayList<>();

            for (RowT row : rows) {
                // this check doesn't seem correct because NULL could be a legit value for column,
                // but this is how it was implemented before, so I just file an issue to deal with this later
                // TODO: https://issues.apache.org/jira/browse/IGNITE-18883
                if (handler.get(updateColumnOffset, row) == null) {
                    rowsToInsert.add(row);
                } else {
                    rowsToUpdate.add(row);
                }
            }

            if (nullOrEmpty(rowsToInsert)) {
                rowsToInsert = null;
            }

            if (nullOrEmpty(rowsToUpdate)) {
                rowsToUpdate = null;
            }
        }

        if (rowsToUpdate != null) {
            inlineUpdates(updateColumnOffset, rowsToUpdate);
        }

        return new Pair<>(rowsToInsert, rowsToUpdate);
    }

    /**
     * Returns {@code true} if this ModifyNode is MERGE operator that has both WHEN MATCHED and WHEN NOT MATCHED
     * clauses, thus has na UPSERT semantic.
     *
     * <p>The rows passed to the node have one of the possible format: <ol>
     *     <li>[insert row type] -- the node is INSERT or MERGE with WHEN NOT MATCHED clause only</li>
     *     <li>[delete row type] -- the node is DELETE</li>
     *     <li>[full row type] + [columns to update] -- the node is UPDATE or MERGE with WHEN MATCHED clause only</li>
     *     <li>[insert row type] + [full row type] + [columns to update] -- the node is MERGE with both handlers, has UPSERT semantic</li>
     * </ol>
     *
     * @param rowSize The size of the row passed to the node.
     * @return {@code true} if the node is MERGE with UPSERT semantic.
     */
    private boolean hasUpsertSemantic(int rowSize) {
        return mapping != null && updateColumns != null && rowSize > mapping.length + updateColumns.size();
    }

    /**
     * Creates a mapping to inline updates into the row.
     *
     * <p>The row passed to the modify node contains columns specified by
     * {@link TableDescriptor#selectForUpdateRowType(IgniteTypeFactory)} followed by {@link #updateColumns}. Here is an example:
     *
     * <pre>
     *     CREATE TABLE t (a INT, b INT, c INT);
     *     INSERT INTO t VALUES (2, 2, 2);
     *
     *     UPDATE t SET b = b + 10, c = c * 10;
     *     -- If selectForUpdateRowType specifies all the table columns,
     *     -- then the following row should be passed to ModifyNode:
     *     -- [2, 2, 2, 12, 20], where first three values is the original values
     *     -- of columns A, B, and C respectively, 12 is the computed value for column B,
     *     -- and 20 is the computed value for column C
     * </pre>
     *
     * <p>For example above we should get mapping [0, 3, 4]: <ul>
     *     <li>The column at index 0 is A. It's not updated, thus it mapped onto itself: 0 --> 0</li>
     *     <li>The column at index 1 is B. B should be updated, and new value for B is at the index 3: 1 --> 3</li>
     *     <li>The column at index 2 is C. C should be updated, and new value for C is at the index 4: 2 --> 4</li>
     * </ul>
     *
     * @param descriptor A descriptor of the target table.
     * @param updateColumns Enumeration of columns to update.
     * @return A mapping to inline the updates into the row.
     */
    private static int @Nullable [] mapping(TableDescriptor descriptor, @Nullable List<String> updateColumns) {
        if (updateColumns == null) {
            return null;
        }

        int columnCount = descriptor.columnsCount();

        int[] mapping = new int[columnCount];

        Object2IntMap<String> updateColumnIndexes = new Object2IntOpenHashMap<>(updateColumns.size());

        for (int i = 0; i < updateColumns.size(); i++) {
            updateColumnIndexes.put(updateColumns.get(i), i + columnCount);
        }

        for (int i = 0; i < columnCount; i++) {
            ColumnDescriptor columnDescriptor = descriptor.columnDescriptor(i);

            mapping[i] = updateColumnIndexes.getOrDefault(columnDescriptor.name(), i);
        }

        return mapping;
    }

    private enum State {
        UPDATING,

        UPDATED,

        END
    }

}
