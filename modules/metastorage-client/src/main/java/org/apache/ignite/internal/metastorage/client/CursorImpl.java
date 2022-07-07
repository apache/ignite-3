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

package org.apache.ignite.internal.metastorage.client;

import static java.util.Collections.singleton;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorCloseCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorHasNextCommand;
import org.apache.ignite.internal.metastorage.common.command.cursor.CursorNextCommand;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.raft.client.service.RaftGroupService;

/**
 * Meta storage service side implementation of cursor.
 *
 * @param <T> Cursor parameter.
 */
public class CursorImpl<T> implements Cursor<T> {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(CursorImpl.class);

    /** Future that runs meta storage service operation that provides cursor. */
    private final CompletableFuture<IgniteUuid> initOp;

    /** Meta storage raft group service. */
    private final RaftGroupService metaStorageRaftGrpSvc;

    private final Iterator<T> it;

    /**
     * Constructor.
     *
     * @param metaStorageRaftGrpSvc Meta storage raft group service.
     * @param initOp                Future that runs meta storage service operation that provides cursor.
     * @param fn                    Function transforming the element of type {@link M} to the type of {@link T}.
     */
    public <M> CursorImpl(
            RaftGroupService metaStorageRaftGrpSvc,
            CompletableFuture<IgniteUuid> initOp,
            Function<M, T> fn
    ) {
        this.metaStorageRaftGrpSvc = metaStorageRaftGrpSvc;
        this.initOp = initOp;
        this.it = new InnerIterator(e -> singleton(e), fn);
    }

    /**
     * Constructor.
     *
     * @param metaStorageRaftGrpSvc Meta storage raft group service.
     * @param initOp                Future that runs meta storage service operation that provides cursor.
     * @param internalCacheFn       Function transforming the result of {@link CursorNextCommand} to the
     *                              {@link Iterable} of elements of type {@link M}.
     * @param fn                    Function transforming the element of type {@link M} to the type of {@link T}.
     */
    public <M> CursorImpl(
            RaftGroupService metaStorageRaftGrpSvc,
            CompletableFuture<IgniteUuid> initOp,
            Function<Object, Iterable<M>> internalCacheFn,
            Function<M, T> fn
    ) {
        this.metaStorageRaftGrpSvc = metaStorageRaftGrpSvc;
        this.initOp = initOp;
        this.it = new InnerIterator(internalCacheFn, fn);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        try {
            initOp.thenCompose(
                    cursorId -> metaStorageRaftGrpSvc.run(new CursorCloseCommand(cursorId))).get();

            ((InnerIterator) it).close();
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() != null && e.getCause().getClass().equals(NodeStoppingException.class)) {
                return;
            }

            LOG.debug("Unable to evaluate cursor close command", e);

            throw new IgniteInternalException(e);
        }
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasNext() {
        return it.hasNext();
    }

    /** {@inheritDoc} */
    @Override
    public T next() {
        return it.next();
    }

    /**
     * Extension of {@link Iterator}.
     */
    private class InnerIterator<M> implements Iterator<T> {
        private final Function<Object, Iterable<M>> internalCacheFn;

        private final Function<M, T> fn;

        private Iterator<M> internalCacheIterator;

        public InnerIterator(Function<Object, Iterable<M>> internalCacheFn, Function<M, T> fn) {
            this.internalCacheFn = internalCacheFn;
            this.fn = fn;
        }

        /** {@inheritDoc} */
        @Override
        public boolean hasNext() {
            try {
                if (internalCacheIterator != null && internalCacheIterator.hasNext()) {
                    return true;
                } else {
                    return initOp
                            .thenCompose(cursorId -> metaStorageRaftGrpSvc.<Boolean>run(new CursorHasNextCommand(cursorId)))
                            .get();
                }
            } catch (InterruptedException | ExecutionException e) {
                if (e.getCause() != null && e.getCause().getClass().equals(NodeStoppingException.class)) {
                    return false;
                }

                LOG.debug("Unable to evaluate cursor hasNext command", e);

                throw new IgniteInternalException(e);
            }
        }

        /** {@inheritDoc} */
        @Override
        public T next() {
            try {
                if (internalCacheIterator != null && internalCacheIterator.hasNext()) {
                    return fn.apply(internalCacheIterator.next());
                } else {
                    Object res = initOp
                            .thenCompose(cursorId -> metaStorageRaftGrpSvc.run(new CursorNextCommand(cursorId)))
                            .get();

                    internalCacheIterator = internalCacheFn.apply(res).iterator();

                    return fn.apply(internalCacheIterator.next());
                }
            } catch (InterruptedException | ExecutionException e) {
                Throwable cause = e.getCause();

                if (cause != null) {
                    if (cause.getClass().equals(NodeStoppingException.class)) {
                        throw new NoSuchElementException();
                    } else {
                        if (cause.getClass().equals(NoSuchElementException.class)) {
                            throw (NoSuchElementException) cause;
                        }
                    }
                }

                LOG.debug("Unable to evaluate cursor hasNext command", e);

                throw new IgniteInternalException(e);
            }
        }

        public void close() {
            internalCacheIterator = null;
        }
    }
}
