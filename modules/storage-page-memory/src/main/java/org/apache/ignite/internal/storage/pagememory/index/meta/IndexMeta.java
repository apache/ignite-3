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

package org.apache.ignite.internal.storage.pagememory.index.meta;

import java.util.UUID;
import org.apache.ignite.internal.tostring.IgniteToStringExclude;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Index tree meta information.
 */
public class IndexMeta {
    private final UUID id;

    @IgniteToStringExclude
    private final long metaPageId;

    private final int inlineSize;

    /**
     * Constructor.
     *
     * @param id Index ID.
     * @param metaPageId Index tree meta page ID.
     * @param inlineSize Inline size in bytes.
     */
    public IndexMeta(UUID id, long metaPageId, int inlineSize) {
        assert inlineSize >= 0 && inlineSize <= 0xFFFF : inlineSize;

        this.id = id;
        this.metaPageId = metaPageId;
        this.inlineSize = inlineSize;
    }

    /**
     * Returns the index ID.
     */
    public UUID id() {
        return id;
    }

    /**
     * Returns page ID of the index tree meta page.
     */
    public long metaPageId() {
        return metaPageId;
    }

    /**
     * Returns inline size in bytes.
     */
    public int inlineSize() {
        return inlineSize;
    }

    @Override
    public String toString() {
        return S.toString(IndexMeta.class, this, "metaPageId", IgniteUtils.hexLong(metaPageId));
    }
}
