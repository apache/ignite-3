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

package org.apache.ignite.internal.pagememory;

import org.apache.ignite.internal.lang.IgniteInternalException;

/**
 * Class responsible for pages storage and handling.
 */
// TODO IGNITE-16350 Improve javadoc in this class.
// TODO IGNITE-28429 Remove the inheritance.
public interface PageMemory {
    /**
     * Stops page memory.
     *
     * @param deallocate {@code True} to deallocate memory, {@code false} to allow memory reuse on subsequent {@code start()}
     */
    void stop(boolean deallocate) throws IgniteInternalException;

    /**
     * Returns a page's size in bytes.
     */
    int pageSize();

    /**
     * Returns a page's size with system overhead, in bytes.
     */
    // TODO IGNITE-16350 Consider renaming.
    int systemPageSize();

    /**
     * Returns the total number of pages loaded into memory.
     */
    long loadedPages();

    /**
     * Creates a new instance of {@link PartitionPageMemory} for a specified partition.
     *
     * @param groupId Group ID for the specific partition.
     * @param partitionId Partition ID of the specific partition.
     */
    PartitionPageMemory createPartitionPageMemory(int groupId, int partitionId);
}
