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
import org.apache.ignite.internal.pagememory.persistence.io.PartitionMetaIo;
import org.jetbrains.annotations.Nullable;

/**
 * Factory for creating {@link PartitionMeta} instances.
 */
public interface PartitionMetaFactory {
    /**
     * Creates a new {@link PartitionMeta} instance.
     *
     * @param checkpointId Checkpoint ID.
     * @param metaIo Partition meta IO.
     * @param pageAddr Page address.
     * @return Partition meta.
     */
    PartitionMeta createPartitionMeta(@Nullable UUID checkpointId, PartitionMetaIo metaIo, long pageAddr);

    /**
     * Returns an instance of {@link PartitionMetaIo} suitable for current {@link PartitionMetaFactory#createPartitionMeta} implementation.
     *
     * @return Partition meta IO.
     */
    PartitionMetaIo partitionMetaIo();
}
