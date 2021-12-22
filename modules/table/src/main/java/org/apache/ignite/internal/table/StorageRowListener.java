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

package org.apache.ignite.internal.table;

import org.apache.ignite.internal.schema.BinaryRow;
import org.jetbrains.annotations.Nullable;

/**
 * Listen storage changes.
 */
public interface StorageRowListener {
    StorageRowListener NO_OP = new StorageRowListener() {
        @Override
        public void onUpdate(@Nullable BinaryRow oldRow, BinaryRow newRow, int partId) {
            // No-op.
        }

        @Override
        public void onRemove(BinaryRow row) {
            // No-op.
        }
    };

    /**
     * Called when row is updated.
     *
     * @param oldRow Old row.
     * @param newRow New row.
     */
    void onUpdate(@Nullable BinaryRow oldRow, BinaryRow newRow, int partId);

    /**
     * Called when row is removed.
     *
     * @param row Removed row.
     */
    void onRemove(BinaryRow row);
}
