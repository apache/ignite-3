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
 * Invocation context provides access for the invoke operation's parameters, a method to set a new value for the key.
 *
 * <p>InvokeProcessor executes atomically under lock, which makes triggering 'live-schema' upgrade impossible within the 
 * invoke operation. Any attempt to update the row leading to schema change results in {@link InvokeProcessorException}.
 *
 * <p>New value MUST be compliant with the current schema version.
 *
 * @param <K> Target object type.
 * @param <V> Value object type.
 * @see InvokeProcessor
 */
public interface InvocationContext<K, V> {
    /**
     * Returns user-provided arguments for the invoke operation.
     *
     * @return Agruments for invocation processor.
     */
    Object[] args();

    /**
     * Returns a user-provided object for the invoke call to run the invoke processor against a specified row.
     *
     * <p>Depending on the Table view the invoke operation is called on, the returned value is either a value object, a record object, or a tuple with value fields set.
     *
     * @return Object the target row is associated with.
     */
    K key();

    /**
     * Returns the current value object for the target row.
     *
     * <p>Depending on the Table view the invoke operation is called on, the returning value is either a value object, 
     * a record object, a tuple with value fields set, or {@code null} for non-existent row.
     *
     * @return Current value of the target row or {@code null} if the value associated with the key does not exist.
     */
    V value();

    /**
     * Sets a new value object for the target row.
     *
     * <p>Depending on the Table view the invoke operation is called on, a new value can be either a value object,
     * a record object, a tuple with value fields set, or {@code null} for removal.
     *
     * @param val Value object to set.
     * @throws InvokeProcessorException if new value is not compliant with the current schema.
     */
    void value(V val);
}
