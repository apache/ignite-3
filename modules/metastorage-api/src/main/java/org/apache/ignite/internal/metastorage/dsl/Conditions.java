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
import org.jetbrains.annotations.NotNull;

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
    public static SimpleCondition.RevisionCondition revision(@NotNull ByteArray key) {
        return new SimpleCondition.RevisionCondition(key.bytes());
    }

    /**
     * Creates condition on entry value.
     *
     * @param key Identifies an entry, which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry value.
     * @see SimpleCondition.ValueCondition
     */
    public static SimpleCondition.ValueCondition value(@NotNull ByteArray key) {
        return new SimpleCondition.ValueCondition(key.bytes());
    }

    /**
     * Creates condition on an entry existence.
     *
     * @param key Identifies an entry, which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry existence.
     */
    public static SimpleCondition exists(@NotNull ByteArray key) {
        return new SimpleCondition.ExistenceCondition(key.bytes()).exists();
    }

    /**
     * Creates a condition on an entry not existence.
     *
     * @param key Identifies an entry, which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry not existence.
     */
    public static SimpleCondition notExists(@NotNull ByteArray key) {
        return new SimpleCondition.ExistenceCondition(key.bytes()).notExists();
    }

    /**
     * Creates a condition on an entry's value, which checks whether a value is tombstone or not.
     *
     * @param key Identifies an entry, which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry's value is tombstone.
     */
    public static SimpleCondition tombstone(@NotNull ByteArray key) {
        return new SimpleCondition.TombstoneCondition(key.bytes()).tombstone();
    }

    /**
     * Default no-op constructor.
     */
    private Conditions() {
        // No-op.
    }
}
