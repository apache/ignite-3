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

package org.apache.ignite.internal.metastorage.server;

import java.util.Arrays;
import org.apache.ignite.internal.util.ArrayUtils;
import org.jetbrains.annotations.NotNull;

public abstract class AbstractCompoundCondition implements Condition {

    private final Condition leftCondition;

    private final Condition rightCondition;

    private final byte[][] keys;


    public AbstractCompoundCondition(Condition leftCondition, Condition rightCondition) {
        this.leftCondition = leftCondition;
        this.rightCondition = rightCondition;
        keys = ArrayUtils.concat(leftCondition.keys(), rightCondition.keys());
    }

    @Override
    public @NotNull byte[][] keys() {
        return keys;
    }

    @Override
    public boolean test(Entry... e) {
        return combine(
                leftCondition.test(Arrays.copyOf(e, leftCondition.keys().length)),
                rightCondition.test(
                        Arrays.copyOfRange(e,
                                leftCondition.keys().length,
                                leftCondition.keys().length + rightCondition.keys().length)));
    }

    protected abstract boolean combine(boolean left, boolean right);


    public Condition leftCondition() {
        return leftCondition;
    }

    public Condition rightCondition() {
        return rightCondition;
    }
}
