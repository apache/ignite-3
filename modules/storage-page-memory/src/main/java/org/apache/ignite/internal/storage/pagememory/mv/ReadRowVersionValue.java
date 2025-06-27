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

import org.apache.ignite.internal.pagememory.datapage.ReadPageMemoryRowValue;

/**
 * Reads {@link RowVersion#value()} from page-memory.
 */
class ReadRowVersionValue extends ReadPageMemoryRowValue {
    @Override
    protected int valueSizeOffsetInFirstSlot(byte dataType) {
        switch (dataType) {
            case RowVersion.DATA_TYPE:
                return RowVersion.VALUE_SIZE_OFFSET;
            case WiLinkableRowVersion.DATA_TYPE:
                return WiLinkableRowVersion.VALUE_SIZE_OFFSET;
            default:
                throw new IllegalStateException("Unsupported data type: " + dataType);
        }
    }

    @Override
    protected int valueOffsetInFirstSlot(byte dataType) {
        switch (dataType) {
            case RowVersion.DATA_TYPE:
                return RowVersion.VALUE_OFFSET;
            case WiLinkableRowVersion.DATA_TYPE:
                return WiLinkableRowVersion.VALUE_OFFSET;
            default:
                throw new IllegalStateException("Unsupported data type: " + dataType);
        }
    }

    @Override
    protected boolean supportsDataType(byte dataType) {
        return dataType == RowVersion.DATA_TYPE || dataType == WiLinkableRowVersion.DATA_TYPE;
    }
}
