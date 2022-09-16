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

package org.apache.ignite.internal.pagememory.persistence.store;

import static java.nio.ByteOrder.nativeOrder;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_DATA;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.pageId;
import static org.apache.ignite.internal.util.GridUnsafe.bufferAddress;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.ignite.internal.pagememory.TestPageIoModule.TestPageIo;

/**
 * Auxiliary class for testing page stores.
 */
class TestPageStoreUtils {
    /**
     * Returns random bytes.
     *
     * @param len Number of random bytes.
     */
    static byte[] randomBytes(int len) {
        byte[] res = new byte[len];

        ThreadLocalRandom.current().nextBytes(res);

        return res;
    }

    /**
     * Returns a buffer containing the contents of the {@link TestPageIo}.
     *
     * @param pageId Page ID.
     * @param pageSize Page size in bytes.
     */
    static ByteBuffer createPageByteBuffer(long pageId, int pageSize) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize).order(nativeOrder());

        new TestPageIo().initNewPage(bufferAddress(buffer), pageId, pageSize);

        return buffer;
    }

    /**
     * Returns the ID of the data page.
     *
     * @param allocatePage Page index allocator.
     */
    static long createDataPageId(PageIndexSupplier allocatePage) throws Exception {
        return pageId(0, FLAG_DATA, allocatePage.pageIndex());
    }

    /**
     * Converts varags to an array of integers.
     *
     * @param ints Array of integers.
     */
    public static int[] arr(int... ints) {
        return ints;
    }

    /**
     * Page index supplier.
     */
    interface PageIndexSupplier {
        /**
         * Returns the page index.
         */
        int pageIndex() throws Exception;
    }
}
