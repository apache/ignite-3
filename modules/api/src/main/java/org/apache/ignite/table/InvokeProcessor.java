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
 * Invoke processor interface provides API to run code on server side against a value associated with provided key.
 *
 * @param <K> Target object type.
 * @param <V> Value type.
 * @param <R> Processor result type.
 * @apiNote Processor must always be deployed to nodes. A server node must never got it from the classpath.
 */
public interface InvokeProcessor<K, V, R extends Serializable> extends Serializable {
    /**
     * Process entry and return the result.
     *
     * @param ctx Invocation context.
     * @return Invoke processor result.
     * @throws InvokeProcessorException If failed during data processing.
     */
    R process(InvocationContext<K, V> ctx) throws InvokeProcessorException;
}
