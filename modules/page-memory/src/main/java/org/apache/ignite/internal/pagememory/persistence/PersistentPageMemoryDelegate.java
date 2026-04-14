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

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.PartitionPageMemory;
import org.apache.ignite.internal.pagememory.io.PageIoRegistry;
import org.apache.ignite.internal.pagememory.util.PageIdUtils;

class PersistentPageMemoryDelegate implements PartitionPageMemory {
    private final PersistentPageMemory delegate;
    private final int groupId;
    private final int partitionId;

    PersistentPageMemoryDelegate(PersistentPageMemory delegate, int groupId, int partitionId) {
        this.delegate = delegate;
        this.groupId = groupId;
        this.partitionId = partitionId;
    }

    @Override
    public int groupId() {
        return groupId;
    }

    @Override
    public int partitionId() {
        return partitionId;
    }

    @Override
    public PageIoRegistry ioRegistry() {
        return delegate.ioRegistry();
    }

    @Override
    public int pageSize() {
        return delegate.pageSize();
    }

    @Override
    public int realPageSize(int groupId) {
        validateGroupId(groupId);

        return delegate.realPageSize(groupId);
    }

    @Override
    public long allocatePageNoReuse(int groupId, int partitionId, byte flags) throws IgniteInternalCheckedException {
        validateGroupId(groupId);
        validatePartitionId(partitionId);

        return delegate.allocatePageNoReuse(groupId, partitionId, flags);
    }

    @Override
    public boolean freePage(int groupId, long pageId) {
        throw new UnsupportedOperationException("Free page should be never called directly when persistence is enabled.");
    }

    @Override
    public long acquirePage(int groupId, long pageId) throws IgniteInternalCheckedException {
        validateGroupId(groupId);
        validatePartitionId(PageIdUtils.partitionId(pageId));

        return delegate.acquirePage(groupId, pageId);
    }

    @Override
    public void releasePage(int groupId, long pageId, long page) {
        validateGroupId(groupId);
        validatePartitionId(PageIdUtils.partitionId(pageId));

        delegate.releasePage(groupId, pageId, page);
    }

    @Override
    public long readLock(int groupId, long pageId, long page) {
        validateGroupId(groupId);
        validatePartitionId(PageIdUtils.partitionId(pageId));

        return delegate.readLock(pageId, page);
    }

    @Override
    public long readLockForce(int groupId, long pageId, long page) {
        validateGroupId(groupId);
        validatePartitionId(PageIdUtils.partitionId(pageId));

        return delegate.readLockForce(pageId, page);
    }

    @Override
    public void readUnlock(int groupId, long pageId, long page) {
        validateGroupId(groupId);
        validatePartitionId(PageIdUtils.partitionId(pageId));

        delegate.readUnlock(page);
    }

    @Override
    public long writeLock(int groupId, long pageId, long page) {
        validateGroupId(groupId);
        validatePartitionId(PageIdUtils.partitionId(pageId));

        return delegate.writeLock(groupId, pageId, page);
    }

    @Override
    public long writeLockForce(int groupId, long pageId, long page) {
        validateGroupId(groupId);
        validatePartitionId(PageIdUtils.partitionId(pageId));

        return delegate.writeLock(groupId, pageId, page, true);
    }

    @Override
    public long tryWriteLock(int groupId, long pageId, long page) {
        validateGroupId(groupId);
        validatePartitionId(PageIdUtils.partitionId(pageId));

        return delegate.tryWriteLock(groupId, pageId, page);
    }

    @Override
    public void writeUnlock(int groupId, long pageId, long page, boolean dirtyFlag) {
        validateGroupId(groupId);
        validatePartitionId(PageIdUtils.partitionId(pageId));

        delegate.writeUnlock(groupId, pageId, page, dirtyFlag);
    }

    private void validateGroupId(int groupId) {
        assert this.groupId == groupId : "Wrong groupId [expected=" + this.groupId + ", actual=" + groupId + ']';
    }

    private void validatePartitionId(int partitionId) {
        assert this.partitionId == partitionId : "Wrong partitionId [expected=" + this.partitionId + ", actual=" + partitionId + ']';
    }
}
