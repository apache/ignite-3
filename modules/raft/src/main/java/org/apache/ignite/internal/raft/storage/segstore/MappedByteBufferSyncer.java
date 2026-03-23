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

package org.apache.ignite.internal.raft.storage.segstore;

import static org.apache.ignite.internal.util.IgniteUtils.jdkVersion;
import static org.apache.ignite.internal.util.IgniteUtils.majorJavaVersion;
import static org.apache.ignite.lang.ErrorGroups.Common.INTERNAL_ERR;

import java.io.FileDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.MappedByteBuffer;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Class that encapsulates different strategies of syncing a region of a memory-mapped byte buffer to the underlying storage for
 * different JDK versions.
 */
abstract class MappedByteBufferSyncer {
    /**
     * Forces any changes made to a region of given buffer's content to be written to the storage device containing the mapped file. The
     * region starts at the given {@code index} in this buffer and is {@code length} bytes.
     *
     * @param buf mmap-ed byte buffer which content should be synced.
     * @param index The index of the first byte in the buffer region that is to be written back to storage; must be non-negative and
     *         less than {@code capacity()}.
     * @param length The length of the region in bytes; must be non-negative and no larger than {@code capacity() - index}.
     */
    abstract void force(MappedByteBuffer buf, int index, int length);

    static MappedByteBufferSyncer createSyncer() {
        return majorJavaVersion(jdkVersion()) >= 13 ? new Jdk13Syncer() : new LegacySyncer();
    }

    private static class Jdk13Syncer extends MappedByteBufferSyncer {
        /** {@code MappedByteBuffer#force(int, int)}. */
        private static final Method force = findMethod("force", int.class, int.class);

        @Override
        public void force(MappedByteBuffer buf, int index, int length) {
            try {
                force.invoke(buf, index, length);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IgniteInternalException(INTERNAL_ERR, e);
            }
        }
    }

    private static class LegacySyncer extends MappedByteBufferSyncer {
        /** {@code MappedByteBuffer#fd}. */
        private static final Field fd = findField("fd");

        /** {@code MappedByteBuffer#force0(FileDescriptor, long, long)}. */
        private static final Method force0 = findMethod("force0", FileDescriptor.class, long.class, long.class);

        /** {@code MappedByteBuffer#mappingOffset()}. */
        private static final Method mappingOffset = findMethod("mappingOffset");

        /** {@code MappedByteBuffer#mappingAddress(long)}. */
        private static final Method mappingAddress = findMethod("mappingAddress", long.class);

        @Override
        public void force(MappedByteBuffer buf, int index, int length) {
            try {
                long mappingOffset = (Long) LegacySyncer.mappingOffset.invoke(buf);

                assert mappingOffset == 0 : mappingOffset;

                long mappingAddress = (Long) LegacySyncer.mappingAddress.invoke(buf, mappingOffset);

                long alignmentDelta = (mappingAddress + index) % GridUnsafe.pageSize();

                // Given an alignment delta calculate the largest page aligned address
                // of the mapping less than or equal to the address of the buffer
                // element identified by the index.
                long alignedAddress = (mappingAddress + index) - alignmentDelta;

                force0.invoke(buf, fd.get(buf), alignedAddress, length + alignmentDelta);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new IgniteInternalException(INTERNAL_ERR, e);
            }
        }
    }

    private static Method findMethod(String name, Class<?>... paramTypes) {
        try {
            Method mtd = MappedByteBuffer.class.getDeclaredMethod(name, paramTypes);

            mtd.setAccessible(true);

            return mtd;
        } catch (NoSuchMethodException e) {
            throw new IgniteInternalException(INTERNAL_ERR, e);
        }
    }

    private static Field findField(String name) {
        try {
            Field field = MappedByteBuffer.class.getDeclaredField(name);

            field.setAccessible(true);

            return field;
        } catch (NoSuchFieldException e) {
            throw new IgniteInternalException(INTERNAL_ERR, e);
        }
    }
}
