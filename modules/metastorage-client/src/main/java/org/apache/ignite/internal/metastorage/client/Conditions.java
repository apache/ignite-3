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

package org.apache.ignite.internal.metastorage.client;

import org.apache.ignite.lang.ByteArray;
import org.jetbrains.annotations.NotNull;

/**
 * This class contains fabric methods which produce conditions needed for conditional multi update functionality provided by meta storage
 * service.
 *
 * @see UnaryCondition
 */
public final class Conditions {
    /**
     * Creates condition on entry revision.
     *
     * @param key Identifies an entry which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry revision.
     * @see UnaryCondition.RevisionCondition
     */
    public static UnaryCondition.RevisionCondition revision(@NotNull ByteArray key) {
        return new UnaryCondition.RevisionCondition(key.bytes());
    }

    /**
     * Creates condition on entry value.
     *
     * @param key Identifies an entry which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry value.
     * @see UnaryCondition.ValueCondition
     */
    public static UnaryCondition.ValueCondition value(@NotNull ByteArray key) {
        return new UnaryCondition.ValueCondition(key.bytes());
    }

    /**
     * Creates condition on entry existence.
     *
     * @param key Identifies an entry which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry existence.
     */
    public static UnaryCondition exists(@NotNull ByteArray key) {
        return new UnaryCondition.ExistenceCondition(key.bytes()).exists();
    }

    /**
     * Creates condition on entry not existence.
     *
     * @param key Identifies an entry which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry not existence.
     */
    public static UnaryCondition notExists(@NotNull ByteArray key) {
        return new UnaryCondition.ExistenceCondition(key.bytes()).notExists();
    }

    /**
     * Creates condition on an entry's value which checks whether value is tombstone or not.
     *
     * @param key Identifies an entry which condition will be applied to. Can't be {@code null}.
     * @return Condition on entry's value is tombstone.
     */
    public static UnaryCondition tombstone(@NotNull ByteArray key) {
        return new UnaryCondition.TombstoneCondition(key.bytes()).tombstone();
    }
    
    /**
     * Default no-op constructor.
     */
    private Conditions() {
        // No-op.
    }
}
