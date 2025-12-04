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
import org.apache.ignite.internal.storage.RowId;
import org.jetbrains.annotations.Nullable;

class WiLinkableWriteIntentReader extends WiLinkableRowVersionReader {
    private RowId rowId;

    WiLinkableWriteIntentReader(long link, int partitionId) {
        super(link, partitionId);
    }

    @Override
    public void readFromPage(long pageAddr, DataPagePayload payload) {
        super.readFromPage(pageAddr, payload);

        long rowIdMsb = PageUtils.getLong(pageAddr, payload.offset() + WiLinkableRowVersion.ROW_ID_MSB_OFFSET);
        long rowIdLsb = PageUtils.getLong(pageAddr, payload.offset() + WiLinkableRowVersion.ROW_ID_LSB_OFFSET);

        rowId = new RowId(partitionId, rowIdMsb, rowIdLsb);
    }

    @Override
    protected @Nullable HybridTimestamp readTimestamp(long pageAddr, int offset) {
        return null;
    }

    @Override
    public WiLinkableRowVersion createRowVersion(int valueSize, @Nullable BinaryRow value) {
        return new WiLinkableRowVersion(
                rowId,
                partitionId,
                link,
                null,
                nextLink,
                nextWiLink,
                prevWiLink,
                valueSize,
                value
        );
    }
}
