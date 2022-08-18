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

package org.apache.ignite.internal.storage.pagememory.index.hash.io;

import java.util.UUID;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.storage.pagememory.index.hash.HashIndexRowKey;
import org.apache.ignite.internal.storage.pagememory.index.meta.IndexMeta;

/**
 * Interface for {@link IndexMeta} B+Tree-related IO.
 *
 * <p>Defines a following data layout:
 * <ul>
 *     <li>Index ID - {@link UUID} (16 bytes);</li>
 *     <li>Index root page ID - long (8 bytes).</li>
 * </ul>
 */
public interface HashIndexRowIo {
    /**
     * Returns an offset of the element inside the page.
     *
     * @see BplusIo#offset(int)
     */
    int offset(int idx);

    /**
     * Compare the {@link HashIndexRowKey} from the page with passed {@link HashIndexRowKey}.
     *
     * @param pageAddr Page address.
     * @param idx Element's index.
     * @param rowKey Lookup index row key.
     * @return Comparison result.
     */
    default int compare(long pageAddr, int idx, HashIndexRowKey rowKey) {
        int elementOffset = offset(idx);

        /*
        int cmp = Long.compare(getLong(pageAddr, elementOffset + INDEX_ID_MSB_OFFSET), indexMeta.id().getMostSignificantBits());

        if (cmp != 0) {
            return cmp;
        }

        return Long.compare(getLong(pageAddr, elementOffset + INDEX_ID_LSB_OFFSET), indexMeta.id().getLeastSignificantBits());
         */
        return 0;
    }
}
