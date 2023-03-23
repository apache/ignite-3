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
import org.jetbrains.annotations.Nullable;

/**
 * Index meta information.
 */
public class IndexMeta extends IndexMetaKey {
    @IgniteToStringExclude
    private final long metaPageId;

    private final @Nullable UUID lastBuiltRowIdUuid;

    /**
     * Constructor.
     *
     * @param indexId Index ID.
     * @param metaPageId Index tree meta page ID.
     * @param lastBuiltRowIdUuid Last row ID for which the index was built, {@code null} means that the index was built.
     */
    public IndexMeta(UUID indexId, long metaPageId, @Nullable UUID lastBuiltRowIdUuid) {
        super(indexId);

        this.metaPageId = metaPageId;
        this.lastBuiltRowIdUuid = lastBuiltRowIdUuid;
    }

    /**
     * Returns page ID of the index tree meta page.
     */
    public long metaPageId() {
        return metaPageId;
    }

    /**
     * Returns last row ID for which the index was built, {@code null} means that the index was built.
     */
    public @Nullable UUID lastBuiltRowIdUuid() {
        return lastBuiltRowIdUuid;
    }

    @Override
    public String toString() {
        return S.toString(IndexMeta.class, this, "indexId", indexId, "metaPageId", IgniteUtils.hexLong(metaPageId));
    }
}
