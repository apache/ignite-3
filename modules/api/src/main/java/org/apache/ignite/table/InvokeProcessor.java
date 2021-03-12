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
import org.apache.ignite.table.binary.Row;

/**
 * Invoke processor provide ability to run code against a value associated with a target on server-side.
 * //TODO: Processor will be always deployed to nodes. Never get IP from classpath.
 *
 * @param <T> Target object type.
 * @param <V> Value type.
 * @param <R> Processor result type.
 */
public interface InvokeProcessor<T, V, R extends Serializable> extends Serializable {
    /**
     * Process entry and return the result.
     *
     * @param ctx Invocation context.
     * @return Invoke processor result.
     */
    R process(InvocationContext<T, V> ctx);

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
         * @return Target object.
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

        Row rowBuilder();
    }
}
