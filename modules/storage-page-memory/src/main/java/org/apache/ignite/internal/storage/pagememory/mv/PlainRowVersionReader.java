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
import org.apache.ignite.internal.pagememory.io.DataPagePayload;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.schema.BinaryRow;
import org.jetbrains.annotations.Nullable;

class PlainRowVersionReader implements RowVersionReader {
    protected final long link;
    protected final int partitionId;

    protected long nextLink;

    @Nullable
    protected HybridTimestamp timestamp;

    private int schemaVersion;
    private int valueSize;

    PlainRowVersionReader(long link, int partitionId) {
        this.link = link;
        this.partitionId = partitionId;
    }

    @Override
    public void readFromPage(long pageAddr, DataPagePayload payload) {
        nextLink = RowVersion.readNextLink(partitionId, pageAddr, payload.offset());
        timestamp = readTimestamp(pageAddr, payload.offset());
        schemaVersion = Short.toUnsignedInt(PageUtils.getShort(pageAddr, payload.offset() + RowVersion.SCHEMA_VERSION_OFFSET));
        valueSize = PageUtils.getInt(pageAddr, payload.offset() + RowVersion.VALUE_SIZE_OFFSET);
    }

    protected @Nullable HybridTimestamp readTimestamp(long pageAddr, int offset) {
        return HybridTimestamps.readTimestamp(pageAddr, offset + RowVersion.TIMESTAMP_OFFSET);
    }

    @Override
    public @Nullable HybridTimestamp timestamp() {
        return timestamp;
    }

    @Override
    public int schemaVersion() {
        return schemaVersion;
    }

    @Override
    public int valueSize() {
        return valueSize;
    }

    @Override
    public RowVersion createRowVersion(int valueSize, @Nullable BinaryRow value) {
        return new RowVersion(partitionId, link, timestamp, nextLink, valueSize, value);
    }
}
