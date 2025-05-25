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
import org.jetbrains.annotations.Nullable;

/** Will be removed soon. */
// TODO: IGNITE-25477 Get rid of it
public class CommitResult {
    private final CommitResultStatus status;

    private final @Nullable UUID expectedTxId;

    /** Constructor. */
    public CommitResult(CommitResultStatus status, @Nullable UUID expectedTxId) {
        this.status = status;
        this.expectedTxId = expectedTxId;
    }

    /** Will be removed soon. */
    public @Nullable UUID expectedTxId() {
        return expectedTxId;
    }

    /** Will be removed soon. */
    public CommitResultStatus status() {
        return status;
    }
}
