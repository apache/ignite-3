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

package org.apache.ignite.internal.metastorage.dsl;

import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;

/**
 * Root building block for the compound meta storage invoke command.
 * Contains of boolean condition and 2 branches of execution, like usual programming language's if.
 * Every branch can be either new {@link Iif} statement (non-terminal) or result terminal statement {@link Update}.
 *
 * <p>The easiest way to construct the needed {@link Iif} conditional statement is the builtin shortcut methods.
 * For example to create the statement, which implement the following pseudocode:
 * <pre>
 * {@code
 *                {@link CompoundCondition}
 *                           |
 *  {@link SimpleCondition} |  {@link SimpleCondition}
 *                |        |            |
 * if (key1.value == val1 || key2.value != val2):
 *     if (key3.revision == 3):-------------------|
 *         put(key1, rval1)                       |
 *         return 1                               |
 *     else:                                      | {@link If}
 *         put(key1, rval1)    -|                 |
 *         remove(key2, rval2) -| {@link Update}  |
 *         return 2            -|-----------------|
 * else:
 *   put(key2, rval2)
 *   return 3
 * }
 * </pre>
 * you can use the following code with static shortcut methods from the according classes:
 * <pre>
 * {@code
 * iif(or(value(key1).eq(val1), value(key2).ne(val2)),
 *     iif(revision(key3).eq(3),
 *         ops(put(key1, rval1)).yield(1),
 *         ops(put(key1, rval1), remove(key2)).yield(2)),
 *     ops(put(key2, rval2)).yield(3))
 * }
 * </pre>
 */
@Transferable(MetaStorageMessageGroup.IF)
public interface Iif extends NetworkMessage {
    /** Boolean condition. */
    Condition condition();

    /** Execution branch, if condition evaluates to true (aka left branch). */
    Statement andThen();

    /** Execution branch, if condition evaluates to false (aka right branch). */
    Statement orElse();
}
