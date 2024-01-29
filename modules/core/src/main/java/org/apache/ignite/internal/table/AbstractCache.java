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

package org.apache.ignite.internal.table;

import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.mapToPublicException;
import static org.apache.ignite.internal.util.ExceptionUtils.unwrapCause;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.function.Supplier;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import org.apache.ignite.cache.IgniteCache;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.lang.ErrorGroups.Transactions;
import org.apache.ignite.table.mapper.TypeConverter;
import org.apache.ignite.tx.Transaction;
import org.apache.ignite.tx.TransactionException;
import org.jetbrains.annotations.Nullable;

/**
 * Abstract cache.
 *
 * @param <K>
 * @param <V>
 */
public abstract class AbstractCache<K, V, T extends Transaction> implements IgniteCache<K, V> {
    protected static final byte[] TOMBSTONE = new byte[0];
    protected static final long TOMBSTONE_TTL = 5 * 60 * 1000L;
    protected static final String KEY_COL = "KEY";
    protected static final String VAL_COL = "VALUE";
    protected static final String TTL_COL = "TTL";

    protected final TypeConverter<K, byte[]> keySerializer;
    protected final TypeConverter<V, byte[]> valueSerializer;

    protected final @Nullable CacheLoader<K, V> loader;
    protected final @Nullable CacheWriter<K, V> writer;

    private final ThreadLocal<T> txHolder = new ThreadLocal<>();

    protected final @Nullable ExpiryPolicy expiryPolicy;

    public AbstractCache(
            @Nullable TypeConverter<K, byte[]> keySerializer,
            @Nullable TypeConverter<V, byte[]> valueSerializer,
            @Nullable CacheLoader<K, V> loader,
            @Nullable CacheWriter<K, V> writer,
            @Nullable ExpiryPolicy expiryPolicy
    ) {
        this.keySerializer = keySerializer == null ? new SerializingConverter<>() : keySerializer;
        this.valueSerializer = valueSerializer == null ? new SerializingConverter<>() : valueSerializer;
        this.loader = loader;
        this.writer = writer;
        this.expiryPolicy = expiryPolicy;
    }

    @Override
    public @Nullable V get(K key) {
        T tx = txHolder.get();

        boolean implicit = tx == null;

        if (implicit) {
            // TODO can do get out of tx to improve happy-path speed.
            tx = beginTransaction();
        } else {
            if (isValidState(tx)) {
                throw new TransactionException(Transactions.TX_UNEXPECTED_STATE_ERR, "Transaction is not active");
            }
        }

        try {
            V v = get(tx, key);

            if (implicit) {
                tx.commit();
            }

            return v;
        } catch (Exception e) {
            try {
                tx.rollback();
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }

            unwrapAndRethrow(e);

            return null;
        } finally {
            if (implicit) {
                txHolder.remove();
            }
        }
    }

    @Override
    public void put(K key, V value) {
        T tx = txHolder.get();
        boolean implicit = tx == null;

        if (implicit) {
            tx = beginTransaction();
        } else {
            if (isValidState(tx)) {
                throw new TransactionException(Transactions.TX_UNEXPECTED_STATE_ERR, "Transaction is not active");
            }
        }

        try {
            put(tx, key, value);

            if (implicit) {
                tx.commit();
            }
        } catch (Exception e) {
            try {
                tx.rollback();
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }

            unwrapAndRethrow(e);
        } finally {
            if (implicit) {
                txHolder.remove();
            }
        }
    }

    @Override
    public boolean remove(K key) {
        T tx = txHolder.get();
        boolean implicit = tx == null;

        if (implicit) {
            tx = beginTransaction();
        } else {
            if (isValidState(tx)) {
                throw new TransactionException(Transactions.TX_UNEXPECTED_STATE_ERR, "Transaction is not active");
            }
        }

        try {
            boolean removed = remove(tx, key);

            if (implicit) {
                tx.commit();
            }

            return removed;
        } catch (Exception e) {
            try {
                tx.rollback();
            } catch (Exception ex) {
                e.addSuppressed(ex);
            }

            unwrapAndRethrow(e);

            return false;
        } finally {
            if (implicit) {
                txHolder.remove();
            }
        }
    }

    protected abstract boolean isValidState(T tx);

    protected abstract @Nullable V get(T tx, K key) throws Exception;

    protected abstract void put(T tx, K key, V value) throws Exception;

    protected abstract boolean remove(T tx, K key) throws Exception;

    protected final T beginTransaction() {
        T tx = beginTransaction0();

        txHolder.set(tx);

        return tx;
    }

    protected abstract T beginTransaction0();

    private static class SerializingConverter<T> implements TypeConverter<T, byte[]> {
        /** {@inheritDoc} */
        @Override
        public byte[] toColumnType(T obj) throws Exception {
            ByteArrayOutputStream out = new ByteArrayOutputStream(512);

            try (ObjectOutputStream oos = new ObjectOutputStream(out)) {
                oos.writeObject(obj);
            }

            return out.toByteArray();
        }

        /** {@inheritDoc} */
        @Override
        public T toObjectType(byte[] data) throws Exception {
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
                return (T) ois.readObject();
            }
        }
    }

    protected long getExpiration() {
        if (expiryPolicy != null && expiryPolicy.getExpiryForCreation() != null) {
            Duration dur = expiryPolicy.getExpiryForCreation();
            // TODO use caching clocks.
            return System.currentTimeMillis() + dur.getTimeUnit().toMillis(dur.getDurationAmount());
        }

        return 0;
    }

    private static void unwrapAndRethrow(Exception e) {
        Throwable t = mapToPublicException(unwrapCause(e));

        ExceptionUtils.sneakyThrow(t);
    }

    @Override
    public void runAtomically(Runnable clo) {
        Transaction tx = txHolder.get();

        if (tx != null) {
            throw new TransactionException(Transactions.TX_UNEXPECTED_STATE_ERR, "Transaction is already bound to the thread");
        }

        tx = beginTransaction();

        try {
            clo.run();

            tx.commit();
        } catch (Throwable t) {
            try {
                tx.rollback(); // Try rolling back on user exception.
            } catch (Exception e) {
                t.addSuppressed(e);
            }

            throw t;
        } finally {
            txHolder.remove();
        }
    }

    @Override
    public <R> R runAtomically(Supplier<R> clo) {
        Transaction tx = txHolder.get();

        if (tx != null) {
            throw new TransactionException(Transactions.TX_UNEXPECTED_STATE_ERR, "Transaction is already bound to the thread");
        }

        tx = beginTransaction();

        try {
            R r = clo.get();

            tx.commit();

            return r;
        } catch (Throwable t) {
            try {
                tx.rollback(); // Try rolling back on user exception.
            } catch (Exception e) {
                t.addSuppressed(e);
            }

            throw t;
        } finally {
            txHolder.remove();
        }
    }
}
