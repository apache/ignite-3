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

import org.apache.ignite.lang.ByteArray;

/**
 * This class contains fabric methods which produce conditions needed for a conditional multi update functionality provided by the meta
 * storage service.
 *
 * @see SimpleCondition
 */
public final class Conditions {
    /**
     * Creates condition on entry revision.
     *
     * @param key Identifies an entry which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry revision.
     * @see SimpleCondition.RevisionCondition
     */
    public static SimpleCondition.RevisionConditionBuilder revision(ByteArray key) {
        return SimpleCondition.revision(key.bytes());
    }

    /**
     * Creates condition on entry value.
     *
     * @param key Identifies an entry, which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry value.
     * @see SimpleCondition.ValueCondition
     */
    public static SimpleCondition.ValueConditionBuilder value(ByteArray key) {
        return SimpleCondition.value(key.bytes());
    }

    /**
     * Creates condition on an entry existence.
     *
     * @param key Identifies an entry, which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry existence.
     */
    public static SimpleCondition exists(ByteArray key) {
        return SimpleCondition.exists(key.bytes());
    }

    /**
     * Creates a condition on an entry not existence.
     *
     * @param key Identifies an entry, which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry not existence.
     */
    public static SimpleCondition notExists(ByteArray key) {
        return SimpleCondition.notExists(key.bytes());
    }

    /**
     * Creates a condition on an entry's value, which checks whether a value is tombstone or not.
     *
     * @param key Identifies an entry, which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry's value is tombstone.
     */
    public static SimpleCondition tombstone(ByteArray key) {
        return SimpleCondition.tombstone(key.bytes());
    }

    /**
     * Default no-op constructor.
     */
    private Conditions() {
        // No-op.
    }
}
