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
import org.jetbrains.annotations.Nullable;

/**
 * Defines operation for meta storage conditional update (invoke).
 */
@Transferable(MetaStorageMessageGroup.OPERATION)
public interface Operation extends NetworkMessage {
    /**
     * Key identifies an entry which operation will be applied to. Key is {@code null} for {@link OperationType#NO_OP} operation.
     */
    byte @Nullable [] key();

    /**
     * Value which will be associated with the {@link #key}. Value is not {@code null} only for {@link OperationType#PUT} operation.
     */
    byte @Nullable [] value();

    /**
     * Operation type (integer representation).
     *
     * @see OperationType
     */
    int operationType();

    /** Operation type. */
    default OperationType type() {
        return OperationType.values()[operationType()];
    }
}
