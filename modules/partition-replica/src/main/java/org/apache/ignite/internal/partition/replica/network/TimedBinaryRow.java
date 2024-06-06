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

package org.apache.ignite.internal.partition.replica.network;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.jetbrains.annotations.Nullable;

/**
 * Row with time.
 */
public class TimedBinaryRow {

    private final @Nullable BinaryRow binaryRow;

    private final @Nullable HybridTimestamp commitTimestamp;

    public TimedBinaryRow(@Nullable BinaryRow binaryRow, @Nullable HybridTimestamp commitTimestamp) {
        this.binaryRow = binaryRow;
        this.commitTimestamp = commitTimestamp;
    }

    public TimedBinaryRow(@Nullable BinaryRow binaryRow) {
        this(binaryRow, null);
    }

    public @Nullable BinaryRow binaryRow() {
        return binaryRow;
    }

    public @Nullable HybridTimestamp commitTimestamp() {
        return commitTimestamp;
    }
}
