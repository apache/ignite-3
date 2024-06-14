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

package org.apache.ignite.internal.pagememory.persistence;

import java.util.UUID;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.jetbrains.annotations.Nullable;

/**
 * Simple implementation of {@link PartitionMeta} for testing purposes.
 */
public class FakePartitionMeta extends PartitionMeta {

    public static final PartitionMetaFactory FACTORY = new FakePartitionMetaFactory();

    /**
     * Constructor.
     */
    public FakePartitionMeta() {
        super(0);
    }

    /**
     * Initializes the partition meta. Should be called right after the instance is created.
     *
     * @param checkpointId Checkpoint ID.
     * @return This instance.
     */
    public FakePartitionMeta init(@Nullable UUID checkpointId) {
        initSnapshot(checkpointId);

        return this;
    }

    @Override
    protected FakePartitionMetaSnapshot buildSnapshot(@Nullable UUID checkpointId) {
        return new FakePartitionMetaSnapshot(checkpointId);
    }

    /**
     * Simple implementation of {@link PartitionMetaSnapshot} for testing purposes.
     */
    public static class FakePartitionMetaSnapshot implements PartitionMetaSnapshot {
        private final UUID checkpointId;

        public FakePartitionMetaSnapshot(@Nullable UUID checkpointId) {
            this.checkpointId = checkpointId;
        }

        @Override
        public void writeTo(PartitionMetaIo metaIo, long pageAddr) {
        }

        @Override
        public @Nullable UUID checkpointId() {
            return checkpointId;
        }
    }

    /**
     * Simple implementation of {@link PartitionMetaIo} for testing purposes.
     */
    public static class FakePartitionMetaIo extends PartitionMetaIo {
        /** I/O versions. */
        public static final IoVersions<FakePartitionMetaIo> VERSIONS = new IoVersions<>(new FakePartitionMetaIo(1, 1));

        /**
         * Constructor.
         *
         * @param ver Page format version.
         */
        protected FakePartitionMetaIo(int type, int ver) {
            super(type, ver);
        }

        @Override
        protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
            sb.app("FakePartitionMeta [").nl()
                    .app("pageCount=").app(getPageCount(addr)).nl()
                    .app(']');
        }
    }

    /**
     * Simple implementation of {@link PartitionMetaFactory} for testing purposes.
     */
    public static class FakePartitionMetaFactory implements PartitionMetaFactory {
        @Override
        public FakePartitionMeta createPartitionMeta(UUID checkpointId, PartitionMetaIo metaIo, long pageAddr) {
            return new FakePartitionMeta().init(checkpointId);
        }

        @Override
        public FakePartitionMetaIo partitionMetaIo() {
            return FakePartitionMetaIo.VERSIONS.latest();
        }
    }
}
