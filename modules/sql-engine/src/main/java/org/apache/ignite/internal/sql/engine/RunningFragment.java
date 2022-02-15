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

package org.apache.ignite.internal.sql.engine;

import java.util.Objects;
import org.apache.ignite.internal.sql.engine.exec.ExecutionContext;
import org.apache.ignite.internal.sql.engine.exec.rel.AbstractNode;
import org.apache.ignite.internal.tostring.S;

/**
 * Part of the query which is currently executing on the server.
 */
public class RunningFragment<RowT> {
    private final AbstractNode<RowT> root;

    private final ExecutionContext<RowT> ectx;

    /** Creates the object. */
    public RunningFragment(
            AbstractNode<RowT> root,
            ExecutionContext<RowT> ectx) {
        this.root = root;
        this.ectx = ectx;
    }

    public ExecutionContext<RowT> context() {
        return ectx;
    }

    public AbstractNode<RowT> root() {
        return root;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        RunningFragment<RowT> fragment = (RunningFragment<RowT>) o;

        return Objects.equals(ectx, fragment.ectx);
    }

    /** {@inheritDoc} */
    @Override
    public int hashCode() {
        return Objects.hash(ectx);
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        return S.toString(RunningFragment.class, this);
    }
}
