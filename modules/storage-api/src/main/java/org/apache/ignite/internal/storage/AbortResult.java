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
import org.apache.ignite.internal.schema.BinaryRow;
import org.jetbrains.annotations.Nullable;

/** Description will be here soon. */
// TODO: IGNITE-20347 Add javadocs
public class AbortResult {
    private final AbortResultStatus status;

    private final @Nullable UUID rowTxId;

    private final @Nullable BinaryRow row;

    /** Description will be here soon. */
    public AbortResult(AbortResultStatus status, @Nullable UUID rowTxId, @Nullable BinaryRow row) {
        this.status = status;
        this.rowTxId = rowTxId;
        this.row = row;
    }

    /** Description will be here soon. */
    public AbortResultStatus status() {
        return status;
    }

    /** Description will be here soon. */
    public @Nullable UUID rowTxId() {
        return rowTxId;
    }

    /** Description will be here soon. */
    public @Nullable BinaryRow row() {
        return row;
    }
}
