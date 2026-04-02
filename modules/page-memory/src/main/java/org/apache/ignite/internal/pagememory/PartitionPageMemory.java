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

import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;

/**
 * A page memory instance that's supposed to be bound to a single partition only. Currently it's not the case, the job is split into phases.
 */
// TODO IGNITE-28429 Remove "groupId" parameter from all methods.
public interface PartitionPageMemory extends PageSupport, PageIdAllocator {
    /**
     * Returns a group ID associated with this instance.
     */
    int groupId();

    /**
     * Returns a partition ID associated with this instance.
     */
    int partitionId();

    /**
     * Returns a registry to obtain {@link PageIo} instances for pages.
     */
    PageIoRegistry ioRegistry();

    /**
     * Returns a page's size in bytes.
     */
    int pageSize();

    /**
     * Returns a page size without the encryption overhead, in bytes.
     *
     * @param groupId Group id.
     */
    // TODO IGNITE-16350 Consider renaming.
    int realPageSize(int groupId);
}
