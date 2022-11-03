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

package org.apache.ignite.internal.tx.storage.state;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Decorator for {@link TxStateStorage} on full rebalance.
 *
 * <p>Allows you to use methods to change data, throws {@link IllegalStateException} when reading data.
 */
public class TxStateStorageOnRebalance implements TxStateStorage {
    private static final String ERROR_MESSAGE = "Transaction state storage is in full rebalancing, data reading is not available.";

    private final TxStateStorage delegate;

    /**
     * Constructor.
     *
     * @param delegate Delegate.
     */
    public TxStateStorageOnRebalance(TxStateStorage delegate) {
        this.delegate = delegate;
    }

    @Override
    public TxMeta get(UUID txId) {
        throw createDataWriteOnlyException();
    }

    @Override
    public void put(UUID txId, TxMeta txMeta) {
        delegate.put(txId, txMeta);
    }

    @Override
    public boolean compareAndSet(UUID txId, @Nullable TxState txStateExpected, TxMeta txMeta, long commandIndex) {
        return delegate.compareAndSet(txId, txStateExpected, txMeta, commandIndex);
    }

    @Override
    public void remove(UUID txId) {
        delegate.remove(txId);
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        throw createDataWriteOnlyException();
    }

    @Override
    public CompletableFuture<Void> flush() {
        return delegate.flush();
    }

    @Override
    public long lastAppliedIndex() {
        return delegate.lastAppliedIndex();
    }

    @Override
    public void lastAppliedIndex(long lastAppliedIndex) {
        delegate.lastAppliedIndex(lastAppliedIndex);
    }

    @Override
    public long persistedIndex() {
        return delegate.persistedIndex();
    }

    @Override
    public void destroy() {
        delegate.destroy();
    }

    @Override
    public void close() throws Exception {
        delegate.close();
    }

    private IllegalStateException createDataWriteOnlyException() {
        return new IllegalStateException(ERROR_MESSAGE);
    }
}
