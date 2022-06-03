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

package org.apache.ignite.internal.storage.pagememory;

import org.apache.ignite.internal.pagememory.FullPageId;

/**
 * Class for storing {@link TableTree} partition metadata.
 */
class TableTreePartitionMetas {
    /** {@link TableTree} root. */
    final FullPageId treeRoot;

    /** {@link TableFreeList} root. */
    final FullPageId reuseListRoot;

    /** Have been allocated (created) or read. */
    final boolean allocated;

    /**
     * Constructor.
     *
     * @param reuseListRoot {@link TableFreeList} root.
     * @param treeRoot {@link TableTree} root.
     * @param allocated Have been allocated (created) or read.
     */
    public TableTreePartitionMetas(
            FullPageId treeRoot,
            FullPageId reuseListRoot,
            boolean allocated
    ) {
        this.treeRoot = treeRoot;
        this.reuseListRoot = reuseListRoot;
        this.allocated = allocated;
    }
}
