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

import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;

/**
 * UnionAllNode.
 * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
 */
public class UnionAllNode<RowT> extends AbstractNode<RowT> implements Downstream<RowT> {
    private int curSrc;

    private int waiting;

    /**
     * Constructor.
     * TODO Documentation https://issues.apache.org/jira/browse/IGNITE-15859
     *
     * @param ctx Execution context.
     */
    public UnionAllNode(ExecutionContext<RowT> ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override
    protected Downstream<RowT> requestDownstream(int idx) {
        assert sources() != null;
        assert idx >= 0 && idx < sources().size();

        return this;
    }

    /** {@inheritDoc} */
    @Override
    public void request(int rowsCnt) throws Exception {
        assert !nullOrEmpty(sources());
        assert rowsCnt > 0 && waiting == 0;

        source().request(waiting = rowsCnt);
    }

    /** {@inheritDoc} */
    @Override
    public void push(RowT row) throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        waiting--;

        downstream().push(row);
    }

    /** {@inheritDoc} */
    @Override
    public void end() throws Exception {
        assert downstream() != null;
        assert waiting > 0;

        if (++curSrc < sources().size()) {
            source().request(waiting);
        } else {
            waiting = NOT_WAITING;
            downstream().end();
        }
    }

    /** {@inheritDoc} */
    @Override
    protected void rewindInternal() {
        curSrc = 0;
        waiting = 0;
    }

    @Override
    protected void dumpDebugInfo0(IgniteStringBuilder buf) {
        buf.app("class=").app(getClass().getSimpleName())
                .app(", waiting=").app(waiting)
                .app(", currentSource=").app(curSrc);
    }

    private Node<RowT> source() {
        return sources().get(curSrc);
    }
}
