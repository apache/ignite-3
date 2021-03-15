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
import org.apache.ignite.table.binary.ColSpan;

/**
 * Invoke processor provide ability to run code against a value associated
 * with given key on server-side regarding the BinaryObject concept.
 *
 * @param <R> Processor result type.
 */
public interface BinaryKVInvokeProcessor<R extends Serializable> extends KVInvokeProcessor<ColSpan, ColSpan, R> {
   /** {@inheritDoc} */
    R process(BinaryInvocationContext ctx);

    /**
     * Invocation context.
     */
    interface BinaryInvocationContext extends KVInvokeProcessor.InvocationContext<ColSpan, ColSpan> {
        /**
         * @return ColSpan builder.
         */
        ColSpan colSpanBuilder();
    }
}
