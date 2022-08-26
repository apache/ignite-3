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

package org.apache.ignite.internal.sql.engine.exec.rel;

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.RowHandler;
import org.apache.ignite.internal.sql.engine.schema.InternalIgniteTable;
import org.apache.ignite.internal.sql.engine.schema.ModifyRow;
import org.apache.ignite.internal.sql.engine.type.IgniteTypeFactory;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.tx.InternalTransaction;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteStringFormatter;

/**
 * ModifyNode.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class ModifyNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT>, Downstream<RowT> {
    private final InternalIgniteTable table;

    private final TableModify.Operation modifyOp;

    private final List<String> cols;

    private final InternalTable tableView;

    private List<ModifyRow> rows = new ArrayList<>(MODIFY_BATCH_SIZE);

    private long updatedRows;

    private int waiting;

    private int requested;

    private boolean inLoop;

    private State state = State.UPDATING;

    private InternalTransaction tx;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param ctx  Execution context.
     * @param rowType Rel data type.
     * @param table Table object.
     * @param op Operation/
     * @param cols Update column list.
     */
    public ModifyNode(
            ExecutionContext<RowT> ctx,
            RelDataType rowType,
            InternalIgniteTable table,
            TableModify.Operation op,
            List<String> cols
    ) {
        super(ctx, rowType);

        this.table = table;
        this.modifyOp = op;
        this.cols = cols;

        tx = ctx.transaction();

        tableView = table.table();
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

        switch (modifyOp) {
            case DELETE:
            case UPDATE:
            case INSERT:
            case MERGE:
                rows.add(table.toModifyRow(context(), row, modifyOp, cols));

                flushTuples(false);

                break;
            default:
                throw new UnsupportedOperationException(modifyOp.name());
        }

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

    /** Returns mapping of modifications per modification action. */
    private Map<ModifyRow.Operation, Collection<BinaryRowEx>> getOperationsPerAction(List<ModifyRow> rows) {
        Map<ModifyRow.Operation, Collection<BinaryRowEx>> store = new EnumMap<>(ModifyRow.Operation.class);

        for (ModifyRow tuple : rows) {
            store.computeIfAbsent(tuple.getOp(), k -> new ArrayList<>()).add(tuple.getRow());
        }

        return store;
    }

    private void flushTuples(boolean force) {
        if (nullOrEmpty(rows) || !force && rows.size() < MODIFY_BATCH_SIZE) {
            return;
        }

        List<ModifyRow> rows = this.rows;
        this.rows = new ArrayList<>(MODIFY_BATCH_SIZE);

        Map<ModifyRow.Operation, Collection<BinaryRowEx>> operations = getOperationsPerAction(rows);

        for (Map.Entry<ModifyRow.Operation, Collection<BinaryRowEx>> op : operations.entrySet()) {
            switch (op.getKey()) {
                case INSERT_ROW:
                    Collection<BinaryRow> conflictKeys = tableView.insertAll(op.getValue(), tx).join();

                    if (!conflictKeys.isEmpty()) {
                        IgniteTypeFactory typeFactory = context().getTypeFactory();
                        RowHandler.RowFactory<RowT> rowFactory = context().rowHandler().factory(
                                context().getTypeFactory(),
                                table.descriptor().insertRowType(typeFactory)
                        );

                        List<String> conflictKeys0 = conflictKeys.stream()
                                .map(binRow -> table.toRow(context(), binRow, rowFactory, null))
                                .map(context().rowHandler()::toString)
                                .collect(Collectors.toList());

                        throw conflictKeysException(conflictKeys0);
                    }

                    break;
                case UPDATE_ROW:
                    tableView.upsertAll(op.getValue(), tx).join();

                    break;
                case DELETE_ROW:
                    tableView.deleteAll(op.getValue(), tx).join();

                    break;
                default:
                    throw new UnsupportedOperationException(op.getKey().name());
            }
        }

        updatedRows += rows.size();
    }

    /** Transforms keys list to appropriate exception. */
    private IgniteInternalException conflictKeysException(List<String> conflictKeys) {
        if (modifyOp == TableModify.Operation.INSERT) {
            return new IgniteInternalException("Failed to INSERT some keys because they are already in cache. "
                    + "[rows=" + conflictKeys + ']');
        } else {
            return new IgniteInternalException(
                    IgniteStringFormatter.format("Failed to MERGE some keys due to keys conflict or concurrent updates, "
                            + "clashed input rows: {}", conflictKeys));
        }
    }

    private enum State {
        UPDATING,

        UPDATED,

        END
    }
}
