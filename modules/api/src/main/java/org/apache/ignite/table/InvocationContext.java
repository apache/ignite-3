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

/**
 * Invocation context.
 * //TODO: Describe. Impossible to update schema inside InvokeProc.
 * //TODO: Describe methods. Add examples.
 *
 * @param <K> Target key object type.
 * @param <V> Value object type.
 */
public interface InvocationContext<K, V> {
    /**
     * @return Processor invocation arguments provided by user.
     */
    Object[] args();

    /**
     * @return Key object which associated value is processed.
     */
    K key();

    /**
     * @return Current value associated with the requested key.
     */
    V value();

    /**
     * Sets new value for the requested key.
     *
     * @param val Value object to set.
     */
    void value(V val);
}
