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
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.jetbrains.annotations.Nullable;

/**
 * Simple implementation of {@link PartitionMeta} for testing purposes.
 */
public class TestPartitionMeta extends PartitionMeta {

    public static final PartitionMetaFactory FACTORY = new TestPartitionMetaFactory();

    /**
     * Constructor.
     *
     * @param checkpointId Checkpoint ID.
     */
    public TestPartitionMeta(@Nullable UUID checkpointId) {
        super(checkpointId);
    }

    @Override
    protected TestPartitionMetaSnapshot buildSnapshot(@Nullable UUID checkpointId) {
        return new TestPartitionMetaSnapshot(checkpointId);
    }

    /**
     * Simple implementation of {@link PartitionMetaSnapshot} for testing purposes.
     */
    public static class TestPartitionMetaSnapshot implements PartitionMetaSnapshot {
        private final UUID checkpointId;

        public TestPartitionMetaSnapshot(@Nullable UUID checkpointId) {
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
    public static class TestPartitionMetaIo extends PartitionMetaIo {
        /** I/O versions. */
        public static final IoVersions<TestPartitionMetaIo> VERSIONS = new IoVersions<>(new TestPartitionMetaIo(7, 1));

        /**
         * Constructor.
         *
         * @param ver Page format version.
         */
        protected TestPartitionMetaIo(int type, int ver) {
            super(type, ver, 0);
        }
    }

    /**
     * Simple implementation of {@link PartitionMetaFactory} for testing purposes.
     */
    public static class TestPartitionMetaFactory implements PartitionMetaFactory {
        @Override public PartitionMeta createPartitionMeta(UUID checkpointId, PartitionMetaIo metaIo, long pageAddr) {
            return new TestPartitionMeta(checkpointId);
        }

        @Override
        public PartitionMetaIo partitionMetaIo() {
            return TestPartitionMetaIo.VERSIONS.latest();
        }
    }
}
