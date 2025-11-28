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

package org.apache.ignite.internal.storage.pagememory.mv;

import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.schema.BinaryRow;
import org.jetbrains.annotations.Nullable;

/**
 * Abstracts out reading of {@link RowVersion}s of different formats.
 *
 * <p>Implementations are stateful and meant to be used for reading a single row version.
 */
interface RowVersionReader {
    static RowVersionReader newRowVersionReader(byte dataType, long link, int partitionId) {
        switch (dataType) {
            case RowVersion.DATA_TYPE:
                return new PlainRowVersionReader(link, partitionId);
            default:
                throw new IllegalStateException("Unsupported data type: " + dataType);
        }
    }

    /**
     * Reads row version data from the given page address and offset.
     *
     * <p>This is executed under a read lock over the corresponding page.
     *
     * @param pageAddr Page address.
     * @param offset Row version offset within the page.
     */
    void readFromPage(long pageAddr, int offset);

    /** Returns the timestamp of the row version that was read by {@link #readFromPage(long, int)}. */
    @Nullable
    HybridTimestamp timestamp();

    /** Returns the schema version of the row version that was read by {@link #readFromPage(long, int)}. */
    int schemaVersion();

    /** Returns value size of the row version that was read by {@link #readFromPage(long, int)}. */
    int valueSize();

    /**
     * Creates a {@link RowVersion} instance using the data read by {@link #readFromPage(long, int)}.
     *
     * @param valueSize Value size.
     * @param value Binary value (can be {@code null} if not loaded).
     * @return New row version.
     */
    RowVersion createRowVersion(int valueSize, @Nullable BinaryRow value);
}
