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

import java.util.List;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.QueryCancelledException;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.exp.func.IterableTableFunction;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunction;
import org.apache.ignite.internal.sql.engine.exec.exp.func.TableFunctionInstance;
import org.apache.ignite.internal.sql.engine.util.Commons;
import org.apache.ignite.sql.SqlException;

/**
 * Scan node.
 */
public class ScanNode<RowT> extends AbstractNode<RowT> implements SingleNode<RowT> {
    private final TableFunction<RowT> func;

    private TableFunctionInstance<RowT> inst;

    private int requested;

    private boolean inLoop;

    /**
     * Constructor for a scan that returns rows from the given iterable.
     *
     * @param ctx Execution context.
     * @param src Source iterable.
     */
    public ScanNode(ExecutionContext<RowT> ctx, Iterable<RowT> src) {
        this(ctx, new IterableTableFunction<>(src));
    }

    /**
     * Constructor for a scan that returns rows produced by the given table function.
     *
     * @param ctx Execution context.
     * @param src Table function.
     */
    public ScanNode(ExecutionContext<RowT> ctx, TableFunction<RowT> src) {
        super(ctx);

        this.func = src;
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert rowsCnt > 0 && requested == 0 : "rowsCnt=" + rowsCnt + ", requested=" + requested;

        onRequestReceived();

        requested = rowsCnt;

        if (!inLoop) {
            this.execute(this::push);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void closeInternal() {
        super.closeInternal();

        Commons.closeQuiet(inst);
        inst = null;
        Commons.closeQuiet(func);
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        Commons.closeQuiet(inst);
        inst = null;
        requested = 0;
    }

    /** {@inheritDoc} */
    @Override
    public void register(List<Node<RowT>> sources) {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        throw new UnsupportedOperationException();
    }

    private void push() throws Exception {
        inLoop = true;
        try {
            if (inst == null) {
                inst = func.createInstance(context());
            }

            int processed = 0;
            while (requested > 0 && inst.hasNext()) {
                requested--;
                downstream().push(inst.next());

                if (++processed == inBufSize && requested > 0) {
                    // allow others to do their job
                    this.execute(this::push);

                    return;
                }
            }
        } catch (QueryCancelledException | SqlException e) {
            throw e;
        } finally {
            inLoop = false;
        }

        if (requested > 0 && !hasNext()) {
            Commons.closeQuiet(inst);
            inst = null;

            requested = 0;

            downstream().end();
        }
    }

    @Override
    protected void dumpDebugInfo0(IgniteStringBuilder buf) {
        buf.app("class=").app(getClass().getSimpleName())
                .app(", requested=").app(requested);
    }

    private boolean hasNext() {
        try {
            return inst.hasNext();
        } catch (QueryCancelledException | SqlException e) {
            throw e;
        }
    }
}
