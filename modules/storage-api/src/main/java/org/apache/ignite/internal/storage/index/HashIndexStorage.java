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

package org.apache.ignite.internal.storage.index;

import java.util.UUID;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.RowId;
import org.apache.ignite.internal.storage.StorageException;
import org.apache.ignite.internal.storage.engine.MvTableStorage;

/**
 * Storage for a Hash Index.
 *
 * <p>This storage serves as an unordered mapping from a subset of a table's columns (a.k.a. index columns) to a set of {@link RowId}s
 * from a single {@link MvPartitionStorage} from the same table.
 */
public interface HashIndexStorage extends IndexStorage {
    /**
     * Returns the Index Descriptor of this storage.
     */
    HashIndexDescriptor indexDescriptor();

    /**
     * Removes all data from this index.
     *
     * @throws StorageException If failed to destory index.
     * @deprecated IGNITE-17626 Synchronous API should be removed. {@link MvTableStorage#destroyIndex(UUID)} must be the only public
     *      option.
     */
    @Deprecated
    void destroy() throws StorageException;
}
