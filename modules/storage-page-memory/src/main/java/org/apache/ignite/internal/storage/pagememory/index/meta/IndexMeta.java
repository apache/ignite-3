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
import org.apache.ignite.internal.util.StringUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Index meta information.
 */
public class IndexMeta extends IndexMetaKey {
    /** Index type. */
    public enum IndexType {
        HASH((byte) 0),

        SORTED((byte) 1);

        private final byte serializationValue;

        IndexType(byte serializationValue) {
            this.serializationValue = serializationValue;
        }

        /** Converts an index type to its serialized representation. */
        public byte serialize() {
            return serializationValue;
        }

        /** Restores an index type from its serialized representation. */
        public static IndexType deserialize(byte serializationValue) {
            if (HASH.serializationValue == serializationValue) {
                return HASH;
            } else if (SORTED.serializationValue == serializationValue) {
                return SORTED;
            } else {
                throw new AssertionError("Unknown serialization value: " + serializationValue);
            }
        }
    }

    private final IndexType indexType;

    @IgniteToStringExclude
    private final long metaPageId;

    private final @Nullable UUID nextRowIdUuidToBuild;

    /**
     * Constructor.
     *
     * @param id Index ID.
     * @param metaPageId Index tree meta page ID.
     * @param nextRowIdUuidToBuild Row ID uuid for which the index needs to be built, {@code null} means that the index building has
     *      completed.
     */
    public IndexMeta(int id, IndexType indexType, long metaPageId, @Nullable UUID nextRowIdUuidToBuild) {
        super(id);

        this.indexType = indexType;
        this.metaPageId = metaPageId;
        this.nextRowIdUuidToBuild = nextRowIdUuidToBuild;
    }

    public IndexType indexType() {
        return indexType;
    }

    /**
     * Returns page ID of the index tree meta page.
     */
    public long metaPageId() {
        return metaPageId;
    }

    /**
     * Returns row ID uuid for which the index needs to be built, {@code null} means that the index building has completed.
     */
    public @Nullable UUID nextRowIdUuidToBuild() {
        return nextRowIdUuidToBuild;
    }

    @Override
    public String toString() {
        return S.toString(IndexMeta.class, this, "indexId=", indexId(), "metaPageId", StringUtils.hexLong(metaPageId));
    }
}
