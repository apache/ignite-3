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

package org.apache.ignite.internal.storage.engine;

import org.apache.ignite.internal.storage.Storage;
import org.apache.ignite.internal.storage.StorageException;

/**
 * Table storage that contains meta, partitions and SQL indexes.
 */
public interface TableStorage {
    /**
     * Gets or creates a partition for current table.
     *
     * @param partId Partition id.
     * @return Partition storage.
     */
    Storage getOrCreatePartition(int partId);

    /**
     * Starts the storage.
     *
     * @throws StorageException If something went wrong.
     */
    public void start() throws StorageException;

    /**
     * Stops the storage.
     *
     * @throws StorageException If something went wrong.
     */
    void stop() throws StorageException;
}
