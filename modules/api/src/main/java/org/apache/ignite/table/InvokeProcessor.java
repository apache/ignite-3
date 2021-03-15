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

/**
 * Invoke processor provide ability to run code against a record on server-side.
 *
 * Note: Processor will be always deployed to nodes and will never got from a classpath.
 *
 * @param <T> Record object type.
 * @param <R> Processor result type.
 */
public interface InvokeProcessor<T, R extends Serializable> extends Serializable {
    /**
     * Process entry and return the result.
     *
     * @param ctx Invocation context.
     * @return Invoke processor result.
     */
    R process(InvocationContext<T> ctx);

    /**
     * Invocation context.
     *
     * @param <T> Record type.
     */
    interface InvocationContext<T> {
        /**
         * @return Invocation arguments.
         */
        Object[] args();

        /**
         * @return Requested row.
         */
        T key();

        /**
         * @return Current row.
         */
        T row();

        /**
         * Sets new row.
         *
         * @param row Object to set.
         */
        void row(T row);
    }
}
