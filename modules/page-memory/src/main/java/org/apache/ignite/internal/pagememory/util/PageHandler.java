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

package org.apache.ignite.internal.pagememory.util;

import static java.lang.Boolean.FALSE;
import static org.apache.ignite.internal.util.StringUtils.hexLong;

import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.jetbrains.annotations.Nullable;

/**
 * Page handler.
 *
 * @param <X> Type of the arbitrary parameter.
 * @param <R> Type of the result.
 */
public interface PageHandler<X, R> {
    IgniteLogger LOG = Loggers.forClass(PageHandler.class);

    /** No-op page handler. */
    PageHandler<Void, Boolean> NO_OP = (groupId, pageId, page, pageAddr, io, arg, intArg) -> Boolean.TRUE;

    /**
     * Handles the page.
     *
     * @param groupId Group ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param io IO.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return Result.
     * @throws IgniteInternalCheckedException If failed.
     */
    R run(
            int groupId,
            long pageId,
            long page,
            long pageAddr,
            PageIo io,
            X arg,
            int intArg
    ) throws IgniteInternalCheckedException;

    /**
     * Checks whether write lock (and acquiring if applicable) should be released after handling.
     *
     * @param groupId Group ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param pageAddr Page address.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @return {@code true} If release.
     */
    default boolean releaseAfterWrite(
            int groupId,
            long pageId,
            long page,
            long pageAddr,
            X arg,
            int intArg
    ) {
        return true;
    }

    /**
     * Executes handler under the read lock or returns {@code lockFailed} if lock failed.
     *
     * @param pageMem Page memory.
     * @param groupId Group ID.
     * @param pageId Page ID.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    static <X, R> R readPage(
            PageMemory pageMem,
            int groupId,
            long pageId,
            PageHandler<X, R> h,
            X arg,
            int intArg,
            R lockFailed
    ) throws IgniteInternalCheckedException {
        long page = pageMem.acquirePage(groupId, pageId);

        try {
            return readPage(pageMem, groupId, pageId, page, h, arg, intArg, lockFailed);
        } finally {
            pageMem.releasePage(groupId, pageId, page);
        }
    }

    /**
     * Executes handler under the read lock or returns {@code lockFailed} if lock failed. Page must already be acquired.
     *
     * @param pageMem Page memory.
     * @param groupId Group ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param h Handler.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    static <X, R> R readPage(
            PageMemory pageMem,
            int groupId,
            long pageId,
            long page,
            PageHandler<X, R> h,
            X arg,
            int intArg,
            R lockFailed
    ) throws IgniteInternalCheckedException {
        long pageAddr = 0L;

        try {
            if ((pageAddr = pageMem.readLock(groupId, pageId, page)) == 0L) {
                return lockFailed;
            }

            PageIo io = pageMem.ioRegistry().resolve(pageAddr);

            return h.run(groupId, pageId, page, pageAddr, io, arg, intArg);
        } finally {
            if (pageAddr != 0L) {
                pageMem.readUnlock(groupId, pageId, page);
            }
        }
    }

    /**
     * Initializes a new page.
     *
     * @param pageMem Page memory.
     * @param groupId Group ID.
     * @param pageId Page ID.
     * @param init IO for new page initialization.
     * @throws IgniteInternalCheckedException If failed.
     * @see PageIo#initNewPage(long, long, int)
     */
    static void initPage(
            PageMemory pageMem,
            int groupId,
            long pageId,
            PageIo init
    ) throws IgniteInternalCheckedException {
        Boolean res = writePage(
                pageMem,
                groupId,
                pageId,
                NO_OP,
                init,
                null,
                0,
                FALSE
        );

        assert res != FALSE;
    }

    /**
     * Executes handler under the write lock or returns {@code lockFailed} if lock failed.
     *
     * @param pageMem Page memory.
     * @param groupId Group ID.
     * @param pageId Page ID.
     * @param h Handler.
     * @param init IO for new page initialization or {@code null} if it is an existing page.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    // TODO IGNITE-16350 Consider splitting into two separate methods for init and regular locking.
    static <X, R> R writePage(
            PageMemory pageMem,
            int groupId,
            final long pageId,
            PageHandler<X, R> h,
            @Nullable PageIo init,
            X arg,
            int intArg,
            R lockFailed
    ) throws IgniteInternalCheckedException {
        boolean releaseAfterWrite = true;
        long page = pageMem.acquirePage(groupId, pageId);
        try {
            long pageAddr = pageMem.writeLock(groupId, pageId, page);

            if (pageAddr == 0L) {
                return lockFailed;
            }

            boolean ok = false;

            try {
                if (init != null) {
                    // It is a new page and we have to initialize it.
                    doInitPage(pageMem, groupId, pageId, pageAddr, init);
                } else {
                    init = pageMem.ioRegistry().resolve(pageAddr);
                }

                R res = h.run(groupId, pageId, page, pageAddr, init, arg, intArg);

                ok = true;

                return res;
            } finally {
                assert PageIo.getCrc(pageAddr) == 0;

                if (releaseAfterWrite = h.releaseAfterWrite(groupId, pageId, page, pageAddr, arg, intArg)) {
                    pageMem.writeUnlock(groupId, pageId, page, ok);
                }
            }
        } catch (Throwable t) {
            // This logging was added because of the case when an exception was thrown from "pageMem.writeLock" and it was not got in the
            // "catch" in the calling code of this method, the exception seems to disappear.
            LOG.error("Error writing page: [grpId={}, pageId={}]", t, groupId, hexLong(page));

            throw t;
        } finally {
            if (releaseAfterWrite) {
                pageMem.releasePage(groupId, pageId, page);
            }
        }
    }

    /**
     * Executes handler under the write lock or returns {@code lockFailed} if lock failed. Page must already be acquired.
     *
     * @param pageMem Page memory.
     * @param groupId Group ID.
     * @param pageId Page ID.
     * @param page Page pointer.
     * @param h Handler.
     * @param init IO for new page initialization or {@code null} if it is an existing page.
     * @param arg Argument.
     * @param intArg Argument of type {@code int}.
     * @param lockFailed Result in case of lock failure due to page recycling.
     * @return Handler result.
     * @throws IgniteInternalCheckedException If failed.
     */
    static <X, R> R writePage(
            PageMemory pageMem,
            int groupId,
            long pageId,
            long page,
            PageHandler<X, R> h,
            PageIo init,
            X arg,
            int intArg,
            R lockFailed
    ) throws IgniteInternalCheckedException {
        long pageAddr = pageMem.writeLock(groupId, pageId, page);

        if (pageAddr == 0L) {
            return lockFailed;
        }

        boolean ok = false;

        try {
            if (init != null) {
                // It is a new page and we have to initialize it.
                doInitPage(pageMem, groupId, pageId, pageAddr, init);
            } else {
                init = pageMem.ioRegistry().resolve(pageAddr);
            }

            R res = h.run(groupId, pageId, page, pageAddr, init, arg, intArg);

            ok = true;

            return res;
        } finally {
            assert PageIo.getCrc(pageAddr) == 0;

            if (h.releaseAfterWrite(groupId, pageId, page, pageAddr, arg, intArg)) {
                pageMem.writeUnlock(groupId, pageId, page, ok);
            }
        }
    }

    /**
     * Invokes {@link PageIo#initNewPage(long, long, int)} and does additional checks.
     */
    private static void doInitPage(
            PageMemory pageMem,
            int groupId,
            long pageId,
            long pageAddr,
            PageIo init
    ) {
        assert PageIo.getCrc(pageAddr) == 0;

        init.initNewPage(pageAddr, pageId, pageMem.realPageSize(groupId));
    }
}
