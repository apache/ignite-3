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

package org.apache.ignite.internal.storage;

import java.util.UUID;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** No doc. */
public class AddWriteResult {
    private final AddWriteResultStatus status;

    private final @Nullable BinaryRow previousWriteIntent;

    private final @Nullable UUID currentWriteIntentTxId;

    private final @Nullable HybridTimestamp previousCommitTimestamp;

    /** Constructor. */
    private AddWriteResult(
            AddWriteResultStatus status,
            @Nullable BinaryRow previousWriteIntent,
            @Nullable UUID currentWriteIntentTxId,
            @Nullable HybridTimestamp previousCommitTimestamp
    ) {
        this.status = status;
        this.previousWriteIntent = previousWriteIntent;
        this.currentWriteIntentTxId = currentWriteIntentTxId;
        this.previousCommitTimestamp = previousCommitTimestamp;
    }

    /** No doc. */
    public static AddWriteResult success(@Nullable BinaryRow previousWriteIntent) {
        return new AddWriteResult(AddWriteResultStatus.SUCCESS, previousWriteIntent, null, null);
    }

    /** No doc. */
    public static AddWriteResult writeIntentExists(UUID currentWriteIntentTxId, @Nullable HybridTimestamp previousCommitTimestamp) {
        return new AddWriteResult(AddWriteResultStatus.WRITE_INTENT_EXISTS, null, currentWriteIntentTxId, previousCommitTimestamp);
    }

    /** No doc. */
    public AddWriteResultStatus status() {
        return status;
    }

    /** No doc. */
    public @Nullable BinaryRow previousWriteIntent() {
        return previousWriteIntent;
    }

    /** No doc. */
    public @Nullable UUID currentWriteIntentTxId() {
        return currentWriteIntentTxId;
    }

    /** No doc. */
    public @Nullable HybridTimestamp previousCommitTimestamp() {
        return previousCommitTimestamp;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
