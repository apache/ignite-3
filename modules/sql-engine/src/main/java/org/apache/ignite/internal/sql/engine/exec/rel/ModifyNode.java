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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import java.util.stream.StreamSupport;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.exec.UpdatableTable;
import org.apache.ignite.internal.sql.engine.exec.mapping.ColocationGroup;
import org.apache.ignite.internal.sql.engine.exec.row.RowSchema;
import org.apache.ignite.internal.sql.engine.schema.ColumnDescriptor;
import org.apache.ignite.internal.sql.engine.schema.IgniteTable;
import org.apache.ignite.internal.sql.engine.schema.TableDescriptor;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.type.NativeTypes;
import org.apache.ignite.internal.util.Pair;
import org.jetbrains.annotations.Nullable;

/**
 * The node that can perform data modification operation on tables implementing {@link UpdatableTable} interface.
 *
 * <p>This node covers all currently supported DML operations: INSERT, UPDATE, DELETE, and MERGE.
 *
 * <p>Depending on the type of operation, rows passed to this node can have one of the following formats: <ol>
 *     <li>[insert row type] if the operation is INSERT or MERGE with WHEN NOT MATCHED clause only</li>
 *     <li>[delete row type] if the operation is DELETE</li>
 *     <li>[full row type] + [columns to update] if the operation is UPDATE or MERGE with WHEN MATCHED clause only</li>
 *     <li>[insert row type] + [full row type] + [columns to update] if the operation is MERGE with both WHEN MATCHED
 *         and WHEN NOT MATCHED clauses</li>
 * </ol>
 * , where <ul>
 *     <li>[insert row type] is the same as [full row type]</li>
 *     <li>[delete row type] is the type of the row defined by {@link IgniteTable#rowTypeForDelete(IgniteTypeFactory)}</li>
 *     <li>[columns to update] is the projection of [full row type] having only columns enumerated in
 *         {@link #updateColumns} (with respect to the order of the enumeration)</li>
 * </ul>
 *
 * <p>Depending on the type of operation, different preparatory steps must be taken: <ul>
 *     <li>Before any update, new value must be inlined into the old row: rows for update contains actual row
 *         read from a table followed by new values with respect to {@link #updateColumns}</li>
 *     <li>For MERGE operation, the rows need to be split on two group: rows to insert and rows to update</li>
 *     <li>In case of MERGE with both WHEN MATCHED and WHEN NOT MATCHED clauses, rows to update apart inlining
 *         are need to be shifted on size_of([insert row type]). And here is why: for complex MERGE the rows passed
 *         to node started with the size_of([insert row type]) representing the new row for NOT MATCHED handler
 *         (this part is never empty); then next size_of([full row type]) columns represent the old row
 *         (this part is empty for NOT MATCHED handler, and is not empty for MATCHED handler),
 *         followed by size_of([columns to update]) new values.</li>
 * </ul>
 */
public class ModifyNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT> {

    private static final RowSchema MODIFY_RESULT = RowSchema.builder()
            .addField(NativeTypes.INT64)
            .build();

    private final TableModify.Operation modifyOp;

    private final UpdatableTable table;

    private final @Nullable List<String> updateColumns;

    private final int @Nullable [] mapping;

    private final long sourceId;

    private final int[] insertRowMapping;

    private List<RowT> rows = new ArrayList<>(MODIFY_BATCH_SIZE);

    private long updatedRows;

    private int waiting;

    private int requested;

    private boolean inFlightUpdate;

    /**
     * Constructor.
     *
     * @param ctx An execution context.
     * @param table A table to update.
     * @param op A type of the update operation.
     * @param updateColumns Enumeration of columns to update if applicable.
     */
    public ModifyNode(
            ExecutionContext<RowT> ctx,
            UpdatableTable table,
            long sourceId,
            TableModify.Operation op,
            @Nullable List<String> updateColumns
    ) {
        super(ctx);

        this.table = table;
        this.sourceId = sourceId;
        this.modifyOp = op;
        this.updateColumns = updateColumns;

        this.mapping = mapping(table.descriptor(), updateColumns);

        this.insertRowMapping = StreamSupport.stream(table.descriptor().spliterator(), false)
                        .filter(Predicate.not(ColumnDescriptor::virtual))
                        .mapToInt(ColumnDescriptor::logicalIndex)
                        .toArray();
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources()) && sources().size() == 1;
        assert rowsCnt > 0 && requested == 0;

        checkState();

        requested = rowsCnt;

        requestNextBatchIfNeeded();
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting--;

        rows.add(row);

        assert rows.size() <= MODIFY_BATCH_SIZE;

        if (needToFlush()) {
            flushTuples();
        }

        requestNextBatchIfNeeded();
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        checkState();

        waiting = -1;

        if (needToFlush()) {
            flushTuples();
        } else {
            // special case: if there is nothing to flush, and no in-flight batch,
            // then the source most probably was empty, and we just need to pass
            // through this signal to downstream
            tryEnd();
        }
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

    private void requestNextBatchIfNeeded() throws Exception {
        if (waiting == 0 && rows.isEmpty()) {
            source().request(waiting = MODIFY_BATCH_SIZE);
        }
    }

    private void tryEnd() throws Exception {
        assert downstream() != null;

        if (waiting == -1 && requested > 0 && !inFlightUpdate && rows.isEmpty()) {
            downstream().push(context().rowHandler().factory(MODIFY_RESULT).create(updatedRows));

            requested = 0;
            downstream().end();
        }
    }

    private void flushTuples() {
        assert !nullOrEmpty(rows);

        inFlightUpdate = true;

        List<RowT> rows = this.rows;
        this.rows = new ArrayList<>(MODIFY_BATCH_SIZE);

        CompletableFuture<?> modifyResult;
        ColocationGroup colocationGroup = context().group(sourceId);
        assert colocationGroup != null : "No colocation group for sourceId#" + sourceId;

        switch (modifyOp) {
            case INSERT:
                modifyResult = table.insertAll(context(), rows, colocationGroup);

                break;
            case UPDATE:
                inlineUpdates(0, rows);

                modifyResult = table.upsertAll(context(), rows, colocationGroup);

                break;
            case MERGE:
                // we split the rows because upsert will silently update the row if it's exists,
                // but constraint violation error must to be raised if conflict row is inserted during
                // WHEN NOT MATCHED handling
                Pair<@Nullable List<RowT>, @Nullable List<RowT>> split = splitMerge(rows);

                List<CompletableFuture<?>> mergeParts = new ArrayList<>(2);

                if (split.getFirst() != null) {
                    mergeParts.add(table.insertAll(context(), split.getFirst(), colocationGroup));
                }

                if (split.getSecond() != null) {
                    mergeParts.add(table.upsertAll(context(), split.getSecond(), colocationGroup));
                }

                modifyResult = CompletableFuture.allOf(
                        mergeParts.toArray(CompletableFuture[]::new)
                );

                break;
            case DELETE:
                modifyResult = table.deleteAll(context(), rows, colocationGroup);

                break;
            default:
                throw new UnsupportedOperationException(modifyOp.name());
        }

        modifyResult.whenComplete((r, e) -> context().execute(() -> {
            if (e != null) {
                onError(e);

                return;
            }

            inFlightUpdate = false;

            updatedRows += rows.size();

            if (needToFlush()) {
                flushTuples();
            }

            requestNextBatchIfNeeded();

            tryEnd();
        }, this::onError));
    }

    private boolean needToFlush() {
        return !inFlightUpdate
                && (rows.size() >= MODIFY_BATCH_SIZE || (!rows.isEmpty() && waiting == -1));
    }

    /** See {@link #mapping(TableDescriptor, List)}. */
    private void inlineUpdates(int offset, List<RowT> rows) {
        if (mapping == null) {
            // the node inserts or deletes rows only
            return;
        }

        assert updateColumns != null;

        RowHandler<RowT> handler = context().rowHandler();

        int[] targetMapping = applyOffset(mapping, offset);

        rows.replaceAll(row -> handler.map(row, targetMapping));
    }

    /** Adds the provided offset to each value in the mapping. */
    private static int[] applyOffset(int[] srcMapping, int offset) {
        if (offset == 0) {
            return srcMapping;
        }

        int[] targetMapping = new int[srcMapping.length];

        for (int i = 0; i < targetMapping.length; i++) {
            targetMapping[i] = srcMapping[i] + offset;
        }

        return targetMapping;
    }

    /**
     * Splits the rows of merge operation onto rows that should be inserted and rows that should be updated.
     *
     * @param rows Rows to split.
     * @return Pair where first element is list of rows to insert (or null if there is no such rows), and second
     *     element is list of rows to update (or null if there is no such rows).
     */
    private Pair<@Nullable List<RowT>, @Nullable List<RowT>> splitMerge(List<RowT> rows) {
        RowHandler<RowT> handler = context().rowHandler();

        if (nullOrEmpty(updateColumns)) {
            return new Pair<>(rows, null);
        }

        assert mapping != null;

        List<RowT> rowsToInsert = null;
        List<RowT> rowsToUpdate = null;

        int rowSize = handler.columnCount(rows.get(0));

        // we already handled WHEN NOT MATCHED clause only, thus the possible formats of rows
        // are [full row type] + [columns to update] and [insert row type] + [full row type] + [columns to update].
        // Size of the mapping always matches the size of full row, so in any case we will get
        // an offset of the first column of the update part
        // see javadoc of ModifyNode for details on possible formats of rows passed to the node
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

        if (rowsToInsert != null) {
            rowsToInsert.replaceAll(row -> handler.map(row, insertRowMapping));
        }

        if (rowsToUpdate != null) {
            inlineUpdates(updateColumnOffset, rowsToUpdate);
        }

        return new Pair<>(rowsToInsert, rowsToUpdate);
    }

    /**
     * Returns {@code true} if this ModifyNode is MERGE operator that has both WHEN MATCHED and WHEN NOT MATCHED
     * clauses, thus has an UPSERT semantic.
     *
     * <p>See {@link ModifyNode} for details on possible formats of rows passed to the node.
     *
     * @param rowSize The size of the row passed to the node.
     * @return {@code true} if the node is MERGE with UPSERT semantic.
     * @see ModifyNode
     */
    private boolean hasUpsertSemantic(int rowSize) {
        return mapping != null && updateColumns != null && rowSize > mapping.length + updateColumns.size();
    }

    /**
     * Creates a mapping to inline updates into the row.
     *
     * <p>The row passed to the modify node contains columns specified by
     * {@link IgniteTable#getRowType(RelDataTypeFactory)} followed by {@link #updateColumns}. Here is an example:
     *
     * <pre>
     *     CREATE TABLE t (a INT, b INT, c INT);
     *     INSERT INTO t VALUES (2, 2, 2);
     *
     *     UPDATE t SET b = b + 10, c = c * 10;
     *     -- If getRowType specifies all the table columns,
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

        int columnCount = descriptor.storedColumns();

        int[] mapping = new int[columnCount];

        for (int i = 0; i < columnCount; i++) {
            mapping[i] = i;
        }

        for (int i = 0; i < updateColumns.size(); i++) {
            String columnName = updateColumns.get(i);
            ColumnDescriptor columnDescriptor = descriptor.columnDescriptor(columnName);

            assert !columnDescriptor.virtual() : "Virtual column can't be updated";

            mapping[columnDescriptor.logicalIndex()] = columnCount + i;
        }

        return mapping;
    }
}
