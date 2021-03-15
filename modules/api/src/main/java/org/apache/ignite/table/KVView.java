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

package org.apache.ignite.table;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;

/**
 * Key-Value interface.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 */
public interface KVView<K, V> extends KVFacade<K, V> {
    /**
     * Invokes an invoke processor code against the value associated with the provided key.
     *
     * @param key Key that associated with the value that invoke processor will be applied to.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @param <R> Invoke processor result type.
     * @return Result of the processing.
     * @see RecordView.InvokeProcessor
     */
    <R extends Serializable> R invoke(K key, KVInvokeProcessor<K, V, R> proc, Serializable... args);

    /**
     * Invokes an invoke processor code against values associated with the provided keys.
     *
     * @param <R> Invoke processor result type.
     * @param keys Ordered collection of keys which values associated with should be processed.
     * @param proc Invoke processor.
     * @param args Optional invoke processor arguments.
     * @return Results of the processing.
     * @see RecordView.InvokeProcessor
     */
    <R extends Serializable> Map<K, R> invokeAll(Collection<K> keys, KVInvokeProcessor<K, V, R> proc,
        Serializable... args);

    /**
     * Invoke processor provide ability to run code against a value associated with provided key on server-side.
     * <p>
     * Note: Processor will be always deployed to nodes and will never got from a classpath.
     *
     * @param <T> Target object type.
     * @param <V> Value type.
     * @param <R> Processor result type.
     */
    interface KVInvokeProcessor<T, V, R extends Serializable> extends Serializable {
        /**
         * Process entry and return the result.
         *
         * @param ctx Invocation context.
         * @return Invoke processor result.
         */
        R process(InvocationContext<T, V> ctx);
    }

    /**
     * Invocation context.
     *
     * @param <T> Target object type.
     * @param <V> Value type.
     */
    interface InvocationContext<T, V> {
        /**
         * @return Invocation arguments.
         */
        Object[] args();

        /**
         * @return Key object.
         */
        T key();

        /**
         * @return Current value.
         */
        V value();

        /**
         * Sets new value.
         *
         * @param val Value object to set.
         */
        void value(V val);
    }
}
