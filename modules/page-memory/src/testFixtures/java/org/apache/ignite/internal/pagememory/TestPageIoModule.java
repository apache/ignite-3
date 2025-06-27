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

import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;

import com.google.auto.service.AutoService;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.io.PageIoModule;
import org.apache.ignite.internal.pagememory.persistence.FakePartitionMeta.FakePartitionMetaIo;

/**
 * Test implementation of {@link PageIoModule}.
 */
@AutoService(PageIoModule.class)
public class TestPageIoModule implements PageIoModule {
    /** Last possible value for IO type. */
    public static final int TEST_PAGE_TYPE = PageIo.MAX_IO_TYPE;

    /** {@link FakePartitionMetaIo} type. */
    public static final int FAKE_PARTITION_META_PAGE_IO_TYPE = TEST_PAGE_TYPE - 1;

    /** {@link TestSimpleValuePageIo} type.*/
    public static final int TEST_SIMPLE_VALUE_PAGE_IO_TYPE = FAKE_PARTITION_META_PAGE_IO_TYPE - 1;

    /** Version 1, minimal possible value. */
    public static final int TEST_PAGE_VER = 1;

    @Override
    public Collection<IoVersions<?>> ioVersions() {
        return List.of(new IoVersions<>(new TestPageIo()), FakePartitionMetaIo.VERSIONS, new IoVersions<>(new TestSimpleValuePageIo()));
    }

    /**
     * Test implementation of {@link PageIo}.
     */
    public static class TestPageIo extends PageIo {
        /**
         * Constructor.
         */
        public TestPageIo() {
            super(TEST_PAGE_TYPE, TEST_PAGE_VER, FLAG_DATA);
        }

        @Override
        protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        }
    }

    /** Test implementation of {@link PageIo} with simple value (long). */
    public static class TestSimpleValuePageIo extends PageIo {
        private static final int LONG_VALUE_OFF = PageIo.COMMON_HEADER_END;

        /** Constructor. */
        public TestSimpleValuePageIo() {
            super(TEST_SIMPLE_VALUE_PAGE_IO_TYPE, TEST_PAGE_VER, FLAG_DATA);
        }

        @Override
        public void initNewPage(long pageAddr, long pageId, int pageSize) {
            super.initNewPage(pageAddr, pageId, pageSize);

            setLongValue(pageAddr, 0);
        }

        @Override
        protected void printPage(long addr, int pageSize, IgniteStringBuilder sb) {
        }

        /** Reads long value. */
        public static long getLongValue(long pageAddr) {
            return getLong(pageAddr, LONG_VALUE_OFF);
        }

        /** Writes long value. */
        public static void setLongValue(long pageAddr, long value) {
            putLong(pageAddr, LONG_VALUE_OFF, value);
        }
    }
}
