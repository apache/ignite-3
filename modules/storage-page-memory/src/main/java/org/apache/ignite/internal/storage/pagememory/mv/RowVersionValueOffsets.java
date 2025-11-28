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

/**
 * Provides offsets of value bytes of {@link RowVersion}s of different versions.
 *
 * <p>Implementations are stateless and can be reused.
 */
interface RowVersionValueOffsets {
    static RowVersionValueOffsets offsetsFor(byte dataType) {
        switch (dataType) {
            case RowVersion.DATA_TYPE:
                return PlainRowVersionValueOffsets.INSTANCE;
            default:
                throw new IllegalStateException("Unsupported data type: " + dataType);
        }
    }

    int valueSizeOffsetInFirstSlot();

    int valueOffsetInFirstSlot();
}
