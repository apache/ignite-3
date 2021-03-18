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
 * //TODO: Describe methods. Add examples.
 *
 * @param <K> Target object type.
 * @param <V> Value object type.
 */
public interface InvocationContext<K, V> {
    /**
     * @return Processor invocation arguments provided by user to invoke() method.
     */
    Object[] args();

    /**
     * Returns an object the user provide associated with the target row.
     *
     * @return Object is associated with target row. Either Key object or record object with key fields set or
     * tuple with key fields set.
     */
    K key();

    /**
     * Returns current value of row associated with the requested key.
     *
     * @return Current value of target row. Either value object or record object or tuple with value fields set.
     */
    V value();

    /**
     * Sets new value for the target row.
     *
     * New value MUST be compliant with the current schema version.
     * InvokeProcessor executes atomically under lock which makes impossible to trigger 'live-schema' upgrade.
     *
     * @param val Value object to set. Either value object or record object or tuple with value fields set.
     * @throws InvokeProcessorException if new value is not compliant with the current schema.
     */
    void value(V val);
}
