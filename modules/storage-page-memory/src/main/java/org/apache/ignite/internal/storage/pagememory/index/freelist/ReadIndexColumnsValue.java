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

package org.apache.ignite.internal.storage.pagememory.index.freelist;

import org.apache.ignite.internal.pagememory.datapage.ReadPageMemoryRowValue;

/**
 * Reads {@link IndexColumns#valueBuffer()} value from page-memory.
 */
public class ReadIndexColumnsValue extends ReadPageMemoryRowValue {
    @Override
    protected int valueSizeOffsetInFirstSlot(byte dataType) {
        assert dataType == IndexColumns.DATA_TYPE;

        return IndexColumns.SIZE_OFFSET;
    }

    @Override
    protected int valueOffsetInFirstSlot(byte dataType) {
        assert dataType == IndexColumns.DATA_TYPE;

        return IndexColumns.VALUE_OFFSET;
    }
}
