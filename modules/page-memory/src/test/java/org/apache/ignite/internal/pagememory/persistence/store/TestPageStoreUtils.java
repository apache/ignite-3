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
     * @param pageSize Page size in bytes.
     */
    static ByteBuffer createPageByteBuffer(int pageSize) {
        ByteBuffer buffer = ByteBuffer.allocateDirect(pageSize).order(nativeOrder());

        new TestPageIo().initNewPage(bufferAddress(buffer), 0, pageSize);

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
