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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.tx.TxMeta;
import org.apache.ignite.internal.tx.TxState;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

/**
 * Decorator class for a {@link TxStateStorage}.
 */
public class TxStateStorageDecorator implements TxStateStorage {
    private static final VarHandle DELEGATE;

    static {
        try {
            DELEGATE = MethodHandles.lookup().findVarHandle(TxStateStorageDecorator.class, "delegate", TxStateStorage.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private volatile TxStateStorage delegate;

    /**
     * Constructor.
     *
     * @param delegate Delegate.
     */
    public TxStateStorageDecorator(TxStateStorage delegate) {
        this.delegate = delegate;
    }

    /**
     * Replaces a delegate.
     *
     * @param newDelegate New delegate.
     * @return Previous delegate.
     */
    public TxStateStorage replaceDelegate(TxStateStorage newDelegate) {
        return (TxStateStorage) DELEGATE.getAndSet(this, newDelegate);
    }

    /**
     * Returns delegate.
     */
    public TxStateStorage getDelegate() {
        return delegate;
    }

    @Override
    public TxMeta get(UUID txId) {
        return delegate.get(txId);
    }

    @Override
    public void put(UUID txId, TxMeta txMeta) {
        delegate.put(txId, txMeta);
    }

    @Override
    public boolean compareAndSet(UUID txId, @Nullable TxState txStateExpected, TxMeta txMeta, long commandIndex, long commandTerm) {
        return delegate.compareAndSet(txId, txStateExpected, txMeta, commandIndex, commandTerm);
    }

    @Override
    public void remove(UUID txId) {
        delegate.remove(txId);
    }

    @Override
    public Cursor<IgniteBiTuple<UUID, TxMeta>> scan() {
        return delegate.scan();
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
    public long lastAppliedTerm() {
        return delegate.lastAppliedTerm();
    }

    @Override
    public void lastApplied(long lastAppliedIndex, long lastAppliedTerm) {
        delegate.lastApplied(lastAppliedIndex, lastAppliedTerm);
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
    public void close() {
        delegate.close();
    }
}
