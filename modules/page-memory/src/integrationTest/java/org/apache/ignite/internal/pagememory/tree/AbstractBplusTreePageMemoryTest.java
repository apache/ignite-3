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

package org.apache.ignite.internal.pagememory.tree;

import static java.util.Collections.emptyIterator;
import static java.util.Collections.shuffle;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.datastructure.DataStructure.rnd;
import static org.apache.ignite.internal.pagememory.io.PageIo.getPageId;
import static org.apache.ignite.internal.pagememory.tree.AbstractBplusTreePageMemoryTest.TestTree.threadId;
import static org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType.NOOP;
import static org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType.PUT;
import static org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType.REMOVE;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.effectivePageId;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.pagememory.util.PageUtils.getLong;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putLong;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runMultiThreaded;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runMultiThreadedAsync;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.Constants.GiB;
import static org.apache.ignite.internal.util.StringUtils.hexLong;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.function.Predicate;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.TestPageIoRegistry;
import org.apache.ignite.internal.pagememory.datastructure.DataStructure;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.BplusTree.TreeRowClosure;
import org.apache.ignite.internal.pagememory.tree.BplusTree.TreeRowMapClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.InvokeClosure;
import org.apache.ignite.internal.pagememory.tree.IgniteTree.OperationType;
import org.apache.ignite.internal.pagememory.tree.io.BplusInnerIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusLeafIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusMetaIo;
import org.apache.ignite.internal.pagememory.util.PageLockListener;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteRandom;
import org.apache.ignite.internal.util.IgniteStripedLock;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * An abstract class for testing {@link BplusTree} using different implementations of {@link PageMemory}.
 */
public abstract class AbstractBplusTreePageMemoryTest extends BaseIgniteAbstractTest {
    private static final short LONG_INNER_IO = 30000;

    private static final short LONG_LEAF_IO = 30001;

    private static final short LONG_META_IO = 30002;

    protected static final int CPUS = Math.min(8, Runtime.getRuntime().availableProcessors());

    private static final int GROUP_ID = 100500;

    protected static int MAX_PER_PAGE = 0;

    protected static int CNT = 10;

    private static int PUT_INC = 1;

    private static int RMV_INC = 1;

    protected static final int PAGE_SIZE = 512;

    protected static final long MAX_MEMORY_SIZE = GiB;

    /** Forces printing lock/unlock events on the test tree. */
    private static boolean PRINT_LOCKS = false;

    private static final Collection<Long> rmvdIds = ConcurrentHashMap.newKeySet();

    @Nullable
    protected PageMemory pageMem;

    @Nullable
    private ReuseList reuseList;

    /** Stop. */
    private final AtomicBoolean stop = new AtomicBoolean();

    /** Future. */
    private volatile CompletableFuture<?> asyncRunFut;

    /** Print fat logs. */
    private boolean debugPrint = false;

    @BeforeEach
    protected void beforeEach() throws Exception {
        stop.set(false);

        long seed = System.nanoTime();

        println("Test seed: " + seed + "L; // ");

        rnd = new Random(seed);

        pageMem = createPageMemory();

        pageMem.start();

        reuseList = createReuseList(GROUP_ID, 0, pageMem, 0, true);
    }

    @AfterEach
    protected void afterTest() throws Exception {
        rnd = null;

        try {
            if (asyncRunFut != null && !asyncRunFut.isDone()) {
                stop.set(true);

                try {
                    asyncRunFut.cancel(true);
                    asyncRunFut.get(60_000, MILLISECONDS);
                } catch (Throwable ex) {
                    // Ignore
                }
            }

            if (reuseList != null) {
                long size = reuseList.recycledPagesCount();

                assertTrue(size < 7000, "Reuse size: " + size);
            }

            for (int i = 0; i < 10; i++) {
                if (acquiredPages() != 0) {
                    println("!!!");
                    Thread.sleep(10);
                }
            }

            assertEquals(0, acquiredPages());
        } finally {
            if (pageMem != null) {
                pageMem.stop(true);
            }

            MAX_PER_PAGE = 0;
            PUT_INC = 1;
            RMV_INC = -1;
            CNT = 10;
        }
    }

    /**
     * Returns test case timeout.
     */
    protected long getTestTimeout() {
        return 10 * 60 * 1000L;
    }

    /**
     * Creates a reuse list, {@code null} if not needed.
     *
     * @param grpId Cache ID.
     * @param partId Partition ID.
     * @param pageMem Page memory.
     * @param rootId Root page ID.
     * @param initNew Init new flag.
     * @throws Exception If failed.
     */
    protected abstract @Nullable ReuseList createReuseList(
            int grpId,
            int partId,
            PageMemory pageMem,
            long rootId,
            boolean initNew
    ) throws Exception;

    @Test
    public void testFind() throws Exception {
        TestTree tree = createTestTree(true);
        TreeMap<Long, Long> map = new TreeMap<>();

        long size = CNT * CNT;

        for (long i = 1; i <= size; i++) {
            tree.put(i);
            map.put(i, i);
        }

        checkCursor(tree.find(null, null), map.values().iterator());
        checkCursor(tree.find(10L, 70L), map.subMap(10L, true, 70L, true).values().iterator());
    }

    @Test
    public void testRetries() throws Exception {
        TestTree tree = createTestTree(true);

        tree.numRetries = 1;

        long size = CNT * CNT;

        try {
            for (long i = 1; i <= size; i++) {
                tree.put(i);
            }

            fail();
        } catch (IgniteInternalCheckedException ignored) {
            // Ignore.
        }
    }

    @Test
    public void testIsEmpty() throws Exception {
        TestTree tree = createTestTree(true);

        assertTrue(tree.isEmpty());

        for (long i = 1; i <= 500; i++) {
            tree.put(i);

            assertFalse(tree.isEmpty());
        }

        for (long i = 1; i <= 500; i++) {
            assertFalse(tree.isEmpty());

            tree.remove(i);
        }

        assertTrue(tree.isEmpty());
    }

    @Test
    public void testFindWithClosure() throws Exception {
        TestTree tree = createTestTree(true);
        TreeMap<Long, Long> map = new TreeMap<>();

        long size = CNT * CNT;

        for (long i = 1; i <= size; i++) {
            tree.put(i);
            map.put(i, i);
        }

        checkCursor(tree.find(null, null, new TestTreeFindFilteredClosure(Set.of()), null), emptyIterator());

        checkCursor(tree.find(null, null, new TestTreeFindFilteredClosure(map.keySet()), null), map.values().iterator());

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 100; i++) {
            Long val = rnd.nextLong(size) + 1;

            checkCursor(tree.find(null, null, new TestTreeFindFilteredClosure(singleton(val)), null), Set.of(val).iterator());
        }

        for (int i = 0; i < 200; i++) {
            long vals = rnd.nextLong(size) + 1;

            TreeSet<Long> exp = new TreeSet<>();

            for (long k = 0; k < vals; k++) {
                exp.add(rnd.nextLong(size) + 1);
            }

            checkCursor(tree.find(null, null, new TestTreeFindFilteredClosure(exp), null), exp.iterator());

            checkCursor(tree.find(0L, null, new TestTreeFindFilteredClosure(exp), null), exp.iterator());

            checkCursor(tree.find(0L, size, new TestTreeFindFilteredClosure(exp), null), exp.iterator());

            checkCursor(tree.find(null, size, new TestTreeFindFilteredClosure(exp), null), exp.iterator());
        }
    }

    private void checkCursor(Cursor<Long> cursor, Iterator<Long> iterator) {
        while (cursor.hasNext()) {
            assertTrue(iterator.hasNext());

            assertEquals(iterator.next(), cursor.next());
        }

        assertFalse(iterator.hasNext());
    }

    @Test
    public void testPutRemove_1_20_mm_1() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = -1;
        RMV_INC = -1;

        doTestPutRemove(true);
    }

    @Test
    public void testPutRemove_1_20_mm_0() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = -1;
        RMV_INC = -1;

        doTestPutRemove(false);
    }

    @Test
    public void testPutRemove_1_20_pm_1() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = 1;
        RMV_INC = -1;

        doTestPutRemove(true);
    }

    @Test
    public void testPutRemove_1_20_pm_0() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = 1;
        RMV_INC = -1;

        doTestPutRemove(false);
    }

    @Test
    public void testPutRemove_1_20_pp_1() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = 1;
        RMV_INC = 1;

        doTestPutRemove(true);
    }

    @Test
    public void testPutRemove_1_20_pp_0() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = 1;
        RMV_INC = 1;

        doTestPutRemove(false);
    }

    @Test
    public void testPutRemove_1_20_mp_1() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = -1;
        RMV_INC = 1;

        doTestPutRemove(true);
    }

    @Test
    public void testPutRemove_1_20_mp_0() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 20;
        PUT_INC = -1;
        RMV_INC = 1;

        doTestPutRemove(false);
    }

    // ------- 2 - 40

    @Test
    public void testPutRemove_2_40_mm_1() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = -1;
        RMV_INC = -1;

        doTestPutRemove(true);
    }

    @Test
    public void testPutRemove_2_40_mm_0() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = -1;
        RMV_INC = -1;

        doTestPutRemove(false);
    }

    @Test
    public void testPutRemove_2_40_pm_1() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = 1;
        RMV_INC = -1;

        doTestPutRemove(true);
    }

    @Test
    public void testPutRemove_2_40_pm_0() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = 1;
        RMV_INC = -1;

        doTestPutRemove(false);
    }

    @Test
    public void testPutRemove_2_40_pp_1() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = 1;
        RMV_INC = 1;

        doTestPutRemove(true);
    }

    @Test
    public void testPutRemove_2_40_pp_0() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = 1;
        RMV_INC = 1;

        doTestPutRemove(false);
    }

    @Test
    public void testPutRemove_2_40_mp_1() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = -1;
        RMV_INC = 1;

        doTestPutRemove(true);
    }

    @Test
    public void testPutRemove_2_40_mp_0() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 40;
        PUT_INC = -1;
        RMV_INC = 1;

        doTestPutRemove(false);
    }

    // ------- 3 - 60

    @Test
    public void testPutRemove_3_60_mm_1() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = -1;
        RMV_INC = -1;

        doTestPutRemove(true);
    }

    @Test
    public void testPutRemove_3_60_mm_0() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = -1;
        RMV_INC = -1;

        doTestPutRemove(false);
    }

    @Test
    public void testPutRemove_3_60_pm_1() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = 1;
        RMV_INC = -1;

        doTestPutRemove(true);
    }

    @Test
    public void testPutRemove_3_60_pm_0() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = 1;
        RMV_INC = -1;

        doTestPutRemove(false);
    }

    @Test
    public void testPutRemove_3_60_pp_1() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = 1;
        RMV_INC = 1;

        doTestPutRemove(true);
    }

    @Test
    public void testPutRemove_3_60_pp_0() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = 1;
        RMV_INC = 1;

        doTestPutRemove(false);
    }

    @Test
    public void testPutRemove_3_60_mp_1() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = -1;
        RMV_INC = 1;

        doTestPutRemove(true);
    }

    @Test
    public void testPutRemove_3_60_mp_0() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 60;
        PUT_INC = -1;
        RMV_INC = 1;

        doTestPutRemove(false);
    }

    private void doTestPutRemove(boolean canGetRow) throws Exception {
        TestTree tree = createTestTree(canGetRow);

        long cnt = CNT;

        for (long x = PUT_INC > 0 ? 0 : cnt - 1; x >= 0 && x < cnt; x += PUT_INC) {
            assertNull(tree.findOne(x));

            tree.put(x);

            assertNoLocks();

            assertEquals(x, tree.findOne(x).longValue());
            checkIterate(tree, x, x, x, true);

            assertNoLocks();

            tree.validateTree();

            assertNoLocks();
        }

        if (debugPrint) {
            println(tree.printTree());
        }

        assertNoLocks();

        assertNull(tree.findOne(-1L));

        for (long x = 0; x < cnt; x++) {
            assertEquals(x, tree.findOne(x).longValue());
            checkIterate(tree, x, x, x, true);
        }

        assertNoLocks();

        assertNull(tree.findOne(cnt));
        checkIterate(tree, cnt, cnt, null, false);

        for (long x = RMV_INC > 0 ? 0 : cnt - 1; x >= 0 && x < cnt; x += RMV_INC) {
            println(" -- " + x);

            assertEquals(Long.valueOf(x), tree.remove(x));

            assertNoLocks();

            if (debugPrint) {
                println(tree.printTree());
            }

            assertNoLocks();

            assertNull(tree.findOne(x));
            checkIterate(tree, x, x, null, false);

            assertNoLocks();

            tree.validateTree();

            assertNoLocks();
        }

        assertFalse(tree.find(null, null).hasNext());
        assertEquals(0, tree.size());
        assertEquals(0, tree.rootLevel());

        assertNoLocks();
    }

    private void checkIterate(TestTree tree, long lower, long upper, Long exp, boolean expFound) throws IgniteInternalCheckedException {
        TestTreeRowClosure c = new TestTreeRowClosure(exp);

        tree.iterate(lower, upper, c);

        assertEquals(expFound, c.found);
    }

    private void checkIterateC(
            TestTree tree,
            long lower,
            long upper,
            TestTreeRowClosure c,
            boolean expFound
    ) throws IgniteInternalCheckedException {
        c.found = false;

        tree.iterate(lower, upper, c);

        assertEquals(expFound, c.found);
    }

    @Test
    public void testRandomInvoke_1_30_1() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomInvoke(true);
    }

    @Test
    public void testRandomInvoke_1_30_0() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomInvoke(false);
    }

    private void doTestRandomInvoke(boolean canGetRow) throws Exception {
        TestTree tree = createTestTree(canGetRow);

        Map<Long, Long> map = new HashMap<>();

        int loops = reuseList == null ? 20_000 : 60_000;

        for (int i = 0; i < loops; i++) {
            final Long x = (long) BplusTree.randomInt(CNT);
            final int rnd = BplusTree.randomInt(11);

            if (i % 10_000 == 0) {
                if (debugPrint) {
                    println(tree.printTree());
                }
                println(" --> " + i + "  ++> " + x);
            }

            // Update map.
            if (!map.containsKey(x)) {
                if (rnd % 2 == 0) {
                    map.put(x, x);

                    // println("put0: " + x);
                } else {
                    // println("noop0: " + x);
                }
            } else {
                if (rnd % 2 == 0) {
                    // println("put1: " + x);
                } else if (rnd % 3 == 0) {
                    map.remove(x);

                    // println("rmv1: " + x);
                } else {
                    // println("noop1: " + x);
                }
            }

            // Consistently update tree.
            tree.invoke(x, null, new IgniteTree.InvokeClosure<>() {

                OperationType op;

                Long newRow;

                @Override
                public void call(@Nullable Long row) {
                    if (row == null) {
                        if (rnd % 2 == 0) {
                            op = PUT;
                            newRow = x;
                        } else {
                            op = NOOP;
                            newRow = null;
                        }
                    } else {
                        assertEquals(x, row);

                        if (rnd % 2 == 0) {
                            op = PUT;
                            newRow = x; // We can not replace x with y here, because keys must be equal.
                        } else if (rnd % 3 == 0) {
                            op = REMOVE;
                            newRow = null;
                        } else {
                            op = NOOP;
                            newRow = null;
                        }
                    }
                }

                @Override
                public Long newRow() {
                    return newRow;
                }

                @Override
                public OperationType operationType() {
                    return op;
                }
            });

            assertNoLocks();

            // println(tree.printTree());

            tree.validateTree();

            if (i % 100 == 0) {
                assertEqualContents(tree, map);
            }
        }
    }

    @Test
    public void testRandomPutRemove_1_30_0() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomPutRemove(false);
    }

    @Test
    public void testRandomPutRemove_1_30_1() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomPutRemove(true);
    }

    @Test
    public void testMassiveRemove3_false() throws Exception {
        MAX_PER_PAGE = 3;

        doTestMassiveRemove(false);
    }

    @Test
    public void testMassiveRemove3_true() throws Exception {
        MAX_PER_PAGE = 3;

        doTestMassiveRemove(true);
    }

    @Test
    public void testMassiveRemove2_false() throws Exception {
        MAX_PER_PAGE = 2;

        doTestMassiveRemove(false);
    }

    @Test
    public void testMassiveRemove2_true() throws Exception {
        MAX_PER_PAGE = 2;

        doTestMassiveRemove(true);
    }

    @Test
    public void testMassiveRemove1_false() throws Exception {
        MAX_PER_PAGE = 1;

        doTestMassiveRemove(false);
    }

    @Test
    public void testMassiveRemove1_true() throws Exception {
        MAX_PER_PAGE = 1;

        doTestMassiveRemove(true);
    }

    private void doTestMassiveRemove(final boolean canGetRow) throws Exception {
        final int threads = 64;
        final int keys = 3000;

        final AtomicLongArray rmvd = new AtomicLongArray(keys);

        final TestTree tree = createTestTree(canGetRow);

        // Put keys in reverse order to have a better balance in the tree (lower height).
        for (long i = keys - 1; i >= 0; i--) {
            tree.put(i);
            // println(tree.printTree());
        }

        assertEquals(keys, tree.size());

        tree.validateTree();

        info("Remove...");

        try {
            runMultiThreaded(() -> {
                Random rnd = new IgniteRandom();

                for (; ; ) {
                    int idx = 0;
                    boolean found = false;

                    for (int i = 0, shift = rnd.nextInt(keys); i < keys; i++) {
                        idx = (i + shift) % keys;

                        if (rmvd.get(idx) == 0 && rmvd.compareAndSet(idx, 0, 1)) {
                            found = true;

                            break;
                        }
                    }

                    if (!found) {
                        break;
                    }

                    assertEquals(Long.valueOf(idx), tree.remove((long) idx));

                    if (canGetRow) {
                        rmvdIds.add((long) idx);
                    }
                }

                return null;
            }, threads, "remove");

            assertEquals(0, tree.size());

            tree.validateTree();
        } finally {
            rmvdIds.clear();
        }
    }

    @Test
    public void testMassivePut1_true() throws Exception {
        MAX_PER_PAGE = 1;

        doTestMassivePut(true);
    }

    @Test
    public void testMassivePut1_false() throws Exception {
        MAX_PER_PAGE = 1;

        doTestMassivePut(false);
    }

    @Test
    public void testMassivePut2_true() throws Exception {
        MAX_PER_PAGE = 2;

        doTestMassivePut(true);
    }

    @Test
    public void testMassivePut2_false() throws Exception {
        MAX_PER_PAGE = 2;

        doTestMassivePut(false);
    }

    @Test
    public void testMassivePut3_true() throws Exception {
        MAX_PER_PAGE = 3;

        doTestMassivePut(true);
    }

    @Test
    public void testMassivePut3_false() throws Exception {
        MAX_PER_PAGE = 3;

        doTestMassivePut(false);
    }

    private void doTestMassivePut(final boolean canGetRow) throws Exception {
        final int threads = 16;
        final int keys = 26; // We may fail to insert more on small pages size because of tree height.

        final TestTree tree = createTestTree(canGetRow);

        info("Put...");

        final AtomicLongArray k = new AtomicLongArray(keys);

        runMultiThreaded(() -> {
            Random rnd = new IgniteRandom();

            for (; ; ) {
                int idx = 0;
                boolean found = false;

                for (int i = 0, shift = rnd.nextInt(keys); i < keys; i++) {
                    idx = (i + shift) % keys;

                    if (k.get(idx) == 0 && k.compareAndSet(idx, 0, 1)) {
                        found = true;

                        break;
                    }
                }

                if (!found) {
                    break;
                }

                assertNull(tree.put((long) idx));

                assertNoLocks();
            }

            return null;
        }, threads, "put");

        assertEquals(keys, tree.size());

        tree.validateTree();

        Cursor<Long> c = tree.find(null, null);

        long x = 0;

        while (c.hasNext()) {
            assertEquals(Long.valueOf(x++), c.next());
        }

        assertEquals(keys, x);

        assertNoLocks();
    }

    private void doTestRandomPutRemove(boolean canGetRow) throws Exception {
        TestTree tree = createTestTree(canGetRow);

        Map<Long, Long> map = new HashMap<>();

        int loops = reuseList == null ? 50_000 : 150_000;

        for (int i = 0; i < loops; i++) {
            Long x = (long) BplusTree.randomInt(CNT);

            boolean put = BplusTree.randomInt(2) == 0;

            if (i % 10_000 == 0) {
                if (debugPrint) {
                    println(tree.printTree());
                }
                println(" --> " + (put ? "put " : "rmv ") + i + "  " + x);
            }

            if (put) {
                assertEquals(map.put(x, x), tree.put(x));
            } else {
                if (map.remove(x) != null) {
                    assertEquals(x, tree.remove(x));
                }

                assertNull(tree.remove(x));
            }

            assertNoLocks();

            // println(tree.printTree());
            tree.validateTree();

            if (i % 100 == 0) {
                assertEqualContents(tree, map);
            }
        }
    }

    private void assertEqualContents(IgniteTree<Long, Long> tree, Map<Long, Long> map) throws Exception {
        Cursor<Long> cursor = tree.find(null, null);

        while (cursor.hasNext()) {
            Long x = cursor.next();

            assert x != null;

            assertEquals(map.get(x), x);

            assertNoLocks();
        }

        assertEquals(map.size(), tree.size());

        assertNoLocks();
    }

    @Test
    public void testEmptyCursors() throws Exception {
        MAX_PER_PAGE = 5;

        TestTree tree = createTestTree(true);

        assertFalse(tree.find(null, null).hasNext());
        assertFalse(tree.find(0L, 1L).hasNext());

        tree.put(1L);
        tree.put(2L);
        tree.put(3L);

        assertEquals(3, size(tree.find(null, null)));

        assertFalse(tree.find(4L, null).hasNext());
        assertFalse(tree.find(null, 0L).hasNext());

        assertNoLocks();
    }

    @Test
    public void testCursorConcurrentMerge() throws Exception {
        MAX_PER_PAGE = 5;

        // println(" " + pageMem.pageSize());

        TestTree tree = createTestTree(true);

        TreeMap<Long, Long> map = new TreeMap<>();

        for (int i = 0; i < 20_000 + rnd.nextInt(2 * MAX_PER_PAGE); i++) {
            Long row = (long) rnd.nextInt(40_000);

            // println(" <-- " + row);

            assertEquals(map.put(row, row), tree.put(row));
            assertEquals(row, tree.findOne(row));

            assertNoLocks();
        }

        final int off = rnd.nextInt(5 * MAX_PER_PAGE);

        Long upperBound = 30_000L + rnd.nextInt(2 * MAX_PER_PAGE);

        Cursor<Long> c = tree.find(null, upperBound);
        Iterator<Long> i = map.headMap(upperBound, true).keySet().iterator();

        Long last = null;

        for (int j = 0; j < off; j++) {
            assertTrue(c.hasNext());

            // println(" <-> " + c.get());

            last = c.next();

            assertEquals(i.next(), last);

            assertNoLocks();
        }

        if (last != null) {
            // println(" >-< " + last + " " + upperBound);

            c = tree.find(last, upperBound);

            assertTrue(c.hasNext());
            assertEquals(last, c.next());

            assertNoLocks();
        }

        while (c.hasNext()) {
            // println(" --> " + c.get());

            Long t = c.next();

            assertNotNull(t);
            assertEquals(i.next(), t);
            assertEquals(t, tree.remove(t));

            i.remove();

            assertNoLocks();
        }

        assertEquals(map.size(), size(tree.find(null, null)));

        assertNoLocks();
    }

    /**
     * Verifies that {@link BplusTree#size} and {@link BplusTree#size} methods behave correctly on single-threaded addition and removal of
     * elements in random order.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSizeForPutRmvSequential() throws Exception {
        MAX_PER_PAGE = 5;

        int itemCnt = (int) Math.pow(MAX_PER_PAGE, 5) + rnd.nextInt(MAX_PER_PAGE * MAX_PER_PAGE);

        Long[] items = new Long[itemCnt];
        for (int i = 0; i < itemCnt; ++i) {
            items[i] = (long) i;
        }

        TestTree testTree = createTestTree(true);
        TreeMap<Long, Long> goldenMap = new TreeMap<>();

        assertEquals(0, testTree.size());
        assertEquals(0, goldenMap.size());

        final Predicate<Long> rowMatcher = row -> row % 7 == 0;

        final TreeRowClosure<Long, Long> rowClosure = (tree, io, pageAddr, idx) -> rowMatcher.test(io.getLookupRow(tree, pageAddr, idx));

        int correctMatchingRows = 0;

        shuffle(Arrays.asList(items), rnd);

        for (Long row : items) {
            if (debugPrint) {
                println(" --> put(" + row + ")");
                if (debugPrint) {
                    println(testTree.printTree());
                }
            }

            assertEquals(goldenMap.put(row, row), testTree.put(row));
            assertEquals(row, testTree.findOne(row));

            if (rowMatcher.test(row)) {
                ++correctMatchingRows;
            }

            assertEquals(correctMatchingRows, testTree.size(rowClosure));

            long correctSize = goldenMap.size();

            assertEquals(correctSize, testTree.size());
            assertEquals(correctSize, size(testTree.find(null, null)));

            assertNoLocks();
        }

        shuffle(Arrays.asList(items), rnd);

        for (Long row : items) {
            if (debugPrint) {
                println(" --> rmv(" + row + ")");
                if (debugPrint) {
                    println(testTree.printTree());
                }
            }

            assertEquals(row, goldenMap.remove(row));
            assertEquals(row, testTree.remove(row));
            assertNull(testTree.findOne(row));

            if (rowMatcher.test(row)) {
                --correctMatchingRows;
            }

            assertEquals(correctMatchingRows, testTree.size(rowClosure));

            long correctSize = goldenMap.size();

            assertEquals(correctSize, testTree.size());
            assertEquals(correctSize, size(testTree.find(null, null)));

            assertNoLocks();
        }
    }

    /**
     * Verifies that {@link BplusTree#size()} method behaves correctly when run concurrently with {@link BplusTree#put}, {@link
     * BplusTree#remove} methods. Please see details in {@link #doTestSizeForRandomPutRmvMultithreaded}.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSizeForRandomPutRmvMultithreaded_5_4() throws Exception {
        MAX_PER_PAGE = 5;
        CNT = 10_000;

        doTestSizeForRandomPutRmvMultithreaded(4);
    }

    @Test
    public void testSizeForRandomPutRmvMultithreaded_3_256() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 10_000;

        doTestSizeForRandomPutRmvMultithreaded(256);
    }

    /**
     * Verifies that {@link BplusTree#size()} method behaves correctly when run between series of concurrent {@link BplusTree#put}, {@link
     * BplusTree#remove} methods.
     *
     * @param rmvPutSlidingWindowSize Sliding window size (distance between items being deleted and added).
     * @throws Exception If failed.
     */
    private void doTestSizeForRandomPutRmvMultithreaded(final int rmvPutSlidingWindowSize) throws Exception {
        final TestTree tree = createTestTree(false);

        final boolean debugPrint = false;

        final AtomicLong curRmvKey = new AtomicLong(0);
        final AtomicLong curPutKey = new AtomicLong(rmvPutSlidingWindowSize);

        for (long i = curRmvKey.get(); i < curPutKey.get(); ++i) {
            assertNull(tree.put(i));
        }

        final int putRmvThreadCnt = Math.min(CPUS, rmvPutSlidingWindowSize);

        final int loopCnt = CNT / putRmvThreadCnt;

        final CyclicBarrier putRmvOpBarrier = new CyclicBarrier(putRmvThreadCnt);
        final CyclicBarrier sizeOpBarrier = new CyclicBarrier(putRmvThreadCnt);

        CompletableFuture<?> putRmvFut = runMultiThreadedAsync(() -> {
            for (int i = 0; i < loopCnt && !stop.get(); ++i) {
                putRmvOpBarrier.await();

                Long putVal = curPutKey.getAndIncrement();

                if (debugPrint || (i & 0x7ff) == 0) {
                    println(" --> put(" + putVal + ")");
                }

                assertNull(tree.put(putVal));

                assertNoLocks();

                Long rmvVal = curRmvKey.getAndIncrement();

                if (debugPrint || (i & 0x7ff) == 0) {
                    println(" --> rmv(" + rmvVal + ")");
                }

                assertEquals(rmvVal, tree.remove(rmvVal));
                assertNull(tree.remove(rmvVal));

                assertNoLocks();

                if (stop.get()) {
                    break;
                }

                sizeOpBarrier.await();

                long correctSize = curPutKey.get() - curRmvKey.get();

                if (debugPrint || (i & 0x7ff) == 0) {
                    println("====> correctSize=" + correctSize);
                }

                assertEquals(correctSize, size(tree.find(null, null)));
                assertEquals(correctSize, tree.size());
            }

            return null;
        }, putRmvThreadCnt, "put-remove-size");

        CompletableFuture<?> lockPrintingFut = runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                Thread.sleep(1_000);

                println(TestTree.printLocks());
            }

            return null;
        }, 1, "printLocks");

        asyncRunFut = CompletableFuture.allOf(putRmvFut, lockPrintingFut);

        try {
            putRmvFut.get(getTestTimeout(), MILLISECONDS);
        } finally {
            stop.set(true);
            putRmvOpBarrier.reset();
            sizeOpBarrier.reset();

            asyncRunFut.get(getTestTimeout(), MILLISECONDS);
        }

        tree.validateTree();

        assertNoLocks();
    }

    /**
     * Verifies that concurrent running of {@link BplusTree#put} + {@link BplusTree#remove} sequence and {@link BplusTree#size} methods
     * results in correct calculation of tree size.
     *
     * @see #doTestSizeForRandomPutRmvMultithreadedAsync doTestSizeForRandomPutRmvMultithreadedAsync() for details.
     */
    @Test
    public void testSizeForRandomPutRmvMultithreadedAsync_16() throws Exception {
        doTestSizeForRandomPutRmvMultithreadedAsync(16);
    }

    /**
     * Verifies that concurrent running of {@link BplusTree#put} + {@link BplusTree#remove} sequence and {@link BplusTree#size} methods
     * results in correct calculation of tree size.
     *
     * @see #doTestSizeForRandomPutRmvMultithreadedAsync doTestSizeForRandomPutRmvMultithreadedAsync() for details.
     */
    @Test
    public void testSizeForRandomPutRmvMultithreadedAsync_3() throws Exception {
        doTestSizeForRandomPutRmvMultithreadedAsync(3);
    }

    /**
     * Verifies that concurrent running of {@link BplusTree#put} + {@link BplusTree#remove} sequence and {@link BplusTree#size} methods
     * results in correct calculation of tree size.
     *
     * <p>Since in the presence of concurrent modifications the size may differ from the actual one, the test maintains sliding window of
     * records in the tree, uses a barrier between concurrent runs to limit runaway delta in the calculated size, and checks that the
     * measured size lies within certain bounds.
     */
    public void doTestSizeForRandomPutRmvMultithreadedAsync(final int rmvPutSlidingWindowSize) throws Exception {
        MAX_PER_PAGE = 5;

        final boolean debugPrint = false;

        final TestTree tree = createTestTree(false);

        final AtomicLong curRmvKey = new AtomicLong(0);
        final AtomicLong curPutKey = new AtomicLong(rmvPutSlidingWindowSize);

        for (long i = curRmvKey.get(); i < curPutKey.get(); ++i) {
            assertNull(tree.put(i));
        }

        final int putRmvThreadCnt = Math.min(CPUS, rmvPutSlidingWindowSize);
        final int sizeThreadCnt = putRmvThreadCnt;

        final CyclicBarrier putRmvOpBarrier = new CyclicBarrier(putRmvThreadCnt + sizeThreadCnt, () -> {
            if (debugPrint) {
                try {
                    println("===BARRIER=== size=" + tree.size()
                            + "; contents=[" + tree.findFirst() + ".." + tree.findLast() + "]"
                            + "; rmvVal=" + curRmvKey.get() + "; putVal=" + curPutKey.get());

                    println(tree.printTree());
                } catch (IgniteInternalCheckedException e) {
                    // ignore
                }
            }
        });

        final int loopCnt = 250;

        CompletableFuture<?> putRmvFut = runMultiThreadedAsync(() -> {
            for (int i = 0; i < loopCnt && !stop.get(); ++i) {
                int order;
                try {
                    order = putRmvOpBarrier.await();
                } catch (BrokenBarrierException e) {
                    break;
                }

                Long putVal = curPutKey.getAndIncrement();

                if (debugPrint || (i & 0x3ff) == 0) {
                    println(order + ": --> put(" + putVal + ")");
                }

                assertNull(tree.put(putVal));

                Long rmvVal = curRmvKey.getAndIncrement();

                if (debugPrint || (i & 0x3ff) == 0) {
                    println(order + ": --> rmv(" + rmvVal + ")");
                }

                assertEquals(rmvVal, tree.remove(rmvVal));
                assertNull(tree.findOne(rmvVal));
            }

            return null;
        }, putRmvThreadCnt, "put-remove");

        CompletableFuture<?> sizeFut = runMultiThreadedAsync(() -> {
            final List<Long> treeContents = new ArrayList<>(rmvPutSlidingWindowSize * 2);

            final TreeRowClosure<Long, Long> rowDumper = (tree1, io, pageAddr, idx) -> {
                treeContents.add(io.getLookupRow(tree1, pageAddr, idx));
                return true;
            };

            for (long iter = 0; !stop.get(); ++iter) {
                int order = 0;

                try {
                    order = putRmvOpBarrier.await();
                } catch (BrokenBarrierException e) {
                    break;
                }

                long correctSize = curPutKey.get() - curRmvKey.get();

                treeContents.clear();
                long treeSize = tree.size(rowDumper);

                long minBound = correctSize - putRmvThreadCnt;
                long maxBound = correctSize + putRmvThreadCnt;

                if (debugPrint || (iter & 0x3ff) == 0) {
                    println(order + ": size=" + treeSize + "; bounds=[" + minBound + ".." + maxBound + "]; contents=" + treeContents);
                }

                if (treeSize < minBound || treeSize > maxBound) {
                    fail("Tree size is not in bounds [" + minBound + ".." + maxBound + "]: " + treeSize
                            + "; Tree contents: " + treeContents);
                }
            }

            return null;
        }, sizeThreadCnt, "size");

        CompletableFuture<?> lockPrintingFut = runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                Thread.sleep(1_000);

                println(TestTree.printLocks());
            }

            return null;
        }, 1, "printLocks");

        asyncRunFut = CompletableFuture.allOf(putRmvFut, sizeFut, lockPrintingFut);

        try {
            putRmvFut.get(getTestTimeout(), MILLISECONDS);
        } finally {
            stop.set(true);

            // To ensure that an BrokenBarrierException is thrown on method CyclicBarrier#await in other threads.
            while (!asyncRunFut.isDone()) {
                putRmvOpBarrier.reset();

                Thread.sleep(10);
            }

            asyncRunFut.get(getTestTimeout(), MILLISECONDS);
        }

        tree.validateTree();

        assertNoLocks();
    }

    /**
     * The test forces {@link BplusTree#size} method to run into a livelock: during single run the method is picking up new pages which are
     * concurrently added to the tree until the new pages are not added anymore. Test verifies that despite livelock condition a size from a
     * valid range is returned.
     *
     * @throws Exception if test failed
     */
    @Test
    public void testPutSizeLivelock() throws Exception {
        MAX_PER_PAGE = 5;
        CNT = 800;

        // Sliding window size should be greater than the amount of CPU cores to avoid races between puts and removes in the tree.
        int slidingWindowSize = CPUS * 2;
        final boolean debugPrint = false;

        final TestTree tree = createTestTree(false);

        final AtomicLong curRmvKey = new AtomicLong(0);
        final AtomicLong curPutKey = new AtomicLong(slidingWindowSize);

        for (long i = curRmvKey.get(); i < curPutKey.get(); ++i) {
            assertNull(tree.put(i));
        }

        final int hwThreads = CPUS;
        final int putRmvThreadCnt = Math.max(1, hwThreads / 2);
        final int sizeThreadCnt = hwThreads - putRmvThreadCnt;

        final CyclicBarrier putRmvOpBarrier = new CyclicBarrier(putRmvThreadCnt, () -> {
            if (debugPrint) {
                try {
                    println("===BARRIER=== size=" + tree.size() + " [" + tree.findFirst() + ".." + tree.findLast() + "]");
                } catch (IgniteInternalCheckedException e) {
                    // ignore
                }
            }
        });

        final int loopCnt = CNT / hwThreads;

        CompletableFuture<?> putRmvFut = runMultiThreadedAsync(() -> {
            for (int i = 0; i < loopCnt && !stop.get(); ++i) {
                int order;
                try {
                    order = putRmvOpBarrier.await();
                } catch (BrokenBarrierException e) {
                    // barrier reset() has been called: terminate
                    break;
                }

                Long putVal = curPutKey.getAndIncrement();

                if ((i & 0xff) == 0) {
                    println(order + ": --> put(" + putVal + ")");
                }

                assertNull(tree.put(putVal));

                Long rmvVal = curRmvKey.getAndIncrement();

                if ((i & 0xff) == 0) {
                    println(order + ": --> rmv(" + rmvVal + ")");
                }

                assertEquals(rmvVal, tree.remove(rmvVal));
                assertNull(tree.findOne(rmvVal));
            }

            return null;
        }, putRmvThreadCnt, "put-remove");

        CompletableFuture<?> sizeFut = runMultiThreadedAsync(() -> {
            final List<Long> treeContents = new ArrayList<>(slidingWindowSize * 2);

            final TreeRowClosure<Long, Long> rowDumper = (tree1, io, pageAddr, idx) -> {
                treeContents.add(io.getLookupRow(tree1, pageAddr, idx));

                final long endMs = System.currentTimeMillis() + 10;
                final long endPutKey = curPutKey.get() + MAX_PER_PAGE;

                while (System.currentTimeMillis() < endMs && curPutKey.get() < endPutKey) {
                    Thread.yield();
                }

                return true;
            };

            int i = 0;

            while (!stop.get()) {
                // It has been found that if a writer wants to change the page (leaf), then he may not be able
                // to acquire a writing lock because the number of acquired reading locks does not reach 0 (no fair lock).
                if (++i % 10 == 0) {
                    // Reduce contention between writers and readers by one and the same page.
                    Thread.sleep(10);
                }

                treeContents.clear();

                long treeSize = tree.size(rowDumper);
                long curPutVal = curPutKey.get();

                println(" ======> size=" + treeSize + "; last-put-value=" + curPutVal);

                if (treeSize < slidingWindowSize || treeSize > curPutVal) {
                    fail("Tree size is not in bounds [" + slidingWindowSize + ".." + curPutVal + "]:"
                            + treeSize + "; contents=" + treeContents);
                }
            }

            return null;
        }, sizeThreadCnt, "size");

        asyncRunFut = CompletableFuture.allOf(putRmvFut, sizeFut);

        try {
            putRmvFut.get(getTestTimeout(), MILLISECONDS);
        } finally {
            stop.set(true);
            putRmvOpBarrier.reset();

            asyncRunFut.get(getTestTimeout(), MILLISECONDS);
        }

        tree.validateTree();

        assertNoLocks();
    }

    /**
     * Verifies that in case for threads concurrently calling put and remove on a tree with 1-3 pages, the size() method performs
     * correctly.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPutRmvSizeSinglePageContention() throws Exception {
        MAX_PER_PAGE = 10;
        CNT = 20_000;
        final boolean debugPrint = false;
        final int slidingWindowsSize = MAX_PER_PAGE * 2;

        final TestTree tree = createTestTree(false);

        final AtomicLong curPutKey = new AtomicLong(0);
        final BlockingQueue<Long> rowsToRemove = new ArrayBlockingQueue<>(MAX_PER_PAGE / 2);

        final int hwThreadCnt = CPUS;
        final int putThreadCnt = Math.max(1, hwThreadCnt / 4);
        final int rmvThreadCnt = Math.max(1, hwThreadCnt / 2 - putThreadCnt);
        final int sizeThreadCnt = Math.max(1, hwThreadCnt - putThreadCnt - rmvThreadCnt);

        final AtomicInteger sizeInvokeCnt = new AtomicInteger(0);

        final int loopCnt = CNT;

        CompletableFuture<?> sizeFut = runMultiThreadedAsync(() -> {
            int iter = 0;
            while (!stop.get()) {
                long size = tree.size();

                if (debugPrint || (++iter & 0xffff) == 0) {
                    println(" --> size() = " + size);
                }

                sizeInvokeCnt.incrementAndGet();
            }

            return null;
        }, sizeThreadCnt, "size");

        // Let the size threads ignite
        while (sizeInvokeCnt.get() < sizeThreadCnt * 2) {
            Thread.yield();
        }

        CompletableFuture<?> rmvFut = runMultiThreadedAsync(() -> {
            int iter = 0;
            while (!stop.get()) {
                Long rmvVal = rowsToRemove.poll(200, MILLISECONDS);
                if (rmvVal != null) {
                    assertEquals(rmvVal, tree.remove(rmvVal));
                }

                if (debugPrint || (++iter & 0x3ff) == 0) {
                    println(" --> rmv(" + rmvVal + ")");
                }
            }

            return null;
        }, rmvThreadCnt, "rmv");

        CompletableFuture<?> putFut = runMultiThreadedAsync(() -> {
            for (int i = 0; i < loopCnt && !stop.get(); ++i) {
                Long putVal = curPutKey.getAndIncrement();
                assertNull(tree.put(putVal));

                while (rowsToRemove.size() > slidingWindowsSize && !stop.get()) {
                    Thread.yield();
                }

                rowsToRemove.put(putVal);

                if (debugPrint || (i & 0x3ff) == 0) {
                    println(" --> put(" + putVal + ")");
                }
            }

            return null;
        }, putThreadCnt, "put");

        CompletableFuture<?> treePrintFut = runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                Thread.sleep(1_000);

                println(TestTree.printLocks());
                println(tree.printTree());
            }

            return null;
        }, 1, "printTree");

        asyncRunFut = CompletableFuture.allOf(sizeFut, rmvFut, putFut, treePrintFut);

        try {
            putFut.get(getTestTimeout(), MILLISECONDS);
        } finally {
            stop.set(true);

            asyncRunFut.get(getTestTimeout(), MILLISECONDS);
        }

        tree.validateTree();

        assertNoLocks();
    }

    /**
     * The test verifies that {@link BplusTree#put}, {@link BplusTree#remove}, {@link BplusTree#find}, and {@link BplusTree#size} run
     * concurrently, perform correctly and report correct values.
     *
     * <p>A sliding window of numbers is maintainted in the tests.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testPutRmvFindSizeMultithreaded() throws Exception {
        MAX_PER_PAGE = 5;
        CNT = 60_000;

        final int slidingWindowSize = 100;

        final TestTree tree = createTestTree(false);

        final AtomicLong curPutKey = new AtomicLong(0);
        final BlockingQueue<Long> rowsToRemove = new ArrayBlockingQueue<>(slidingWindowSize);

        final int hwThreadCnt = CPUS;
        final int putThreadCnt = Math.max(1, hwThreadCnt / 4);
        final int rmvThreadCnt = Math.max(1, hwThreadCnt / 4);
        final int findThreadCnt = Math.max(1, hwThreadCnt / 4);
        final int sizeThreadCnt = Math.max(1, hwThreadCnt - putThreadCnt - rmvThreadCnt - findThreadCnt);

        final AtomicInteger sizeInvokeCnt = new AtomicInteger(0);

        final int loopCnt = CNT;

        CompletableFuture<?> sizeFut = runMultiThreadedAsync(() -> {
            int iter = 0;
            while (!stop.get()) {
                long size = tree.size();

                if ((++iter & 0x3ff) == 0) {
                    println(" --> size() = " + size);
                }

                sizeInvokeCnt.incrementAndGet();
            }

            return null;
        }, sizeThreadCnt, "size");

        // Let the size threads start
        while (sizeInvokeCnt.get() < sizeThreadCnt * 2) {
            Thread.yield();
        }

        CompletableFuture<?> rmvFut = runMultiThreadedAsync(() -> {
            int iter = 0;
            while (!stop.get()) {
                Long rmvVal = rowsToRemove.poll(200, MILLISECONDS);
                if (rmvVal != null) {
                    assertEquals(rmvVal, tree.remove(rmvVal));
                }

                if ((++iter & 0x3ff) == 0) {
                    println(" --> rmv(" + rmvVal + ")");
                }
            }

            return null;
        }, rmvThreadCnt, "rmv");

        CompletableFuture<?> findFut = runMultiThreadedAsync(() -> {
            int iter = 0;
            while (!stop.get()) {
                Long findVal = curPutKey.get()
                        + slidingWindowSize / 2
                        - rnd.nextInt(slidingWindowSize * 2);

                tree.findOne(findVal);

                if ((++iter & 0x3ff) == 0) {
                    println(" --> fnd(" + findVal + ")");
                }
            }

            return null;
        }, findThreadCnt, "find");

        CompletableFuture<?> putFut = runMultiThreadedAsync(() -> {
            for (int i = 0; i < loopCnt && !stop.get(); ++i) {
                Long putVal = curPutKey.getAndIncrement();
                assertNull(tree.put(putVal));

                while (rowsToRemove.size() > slidingWindowSize) {
                    if (stop.get()) {
                        return null;
                    }

                    Thread.yield();
                }

                rowsToRemove.put(putVal);

                if ((i & 0x3ff) == 0) {
                    println(" --> put(" + putVal + ")");
                }
            }

            return null;
        }, putThreadCnt, "put");

        CompletableFuture<?> lockPrintingFut = runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                Thread.sleep(1_000);

                println(TestTree.printLocks());
            }

            return null;
        }, 1, "printLocks");

        asyncRunFut = CompletableFuture.allOf(sizeFut, rmvFut, findFut, putFut, lockPrintingFut);

        try {
            putFut.get(getTestTimeout(), MILLISECONDS);
        } finally {
            stop.set(true);

            asyncRunFut.get(getTestTimeout(), MILLISECONDS);
        }

        tree.validateTree();

        assertNoLocks();
    }

    @Test
    public void testTestRandomPutRemoveMultithreaded_1_30_0() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomPutRemoveMultithreaded(false);
    }

    @Test
    public void testTestRandomPutRemoveMultithreaded_1_30_1() throws Exception {
        MAX_PER_PAGE = 1;
        CNT = 30;

        doTestRandomPutRemoveMultithreaded(true);
    }

    @Test
    public void testTestRandomPutRemoveMultithreaded_2_50_0() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 50;

        doTestRandomPutRemoveMultithreaded(false);
    }

    @Test
    public void testTestRandomPutRemoveMultithreaded_2_50_1() throws Exception {
        MAX_PER_PAGE = 2;
        CNT = 50;

        doTestRandomPutRemoveMultithreaded(true);
    }

    @Test
    public void testTestRandomPutRemoveMultithreaded_3_70_0() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 70;

        doTestRandomPutRemoveMultithreaded(false);
    }

    @Test
    public void testTestRandomPutRemoveMultithreaded_3_70_1() throws Exception {
        MAX_PER_PAGE = 3;
        CNT = 70;

        doTestRandomPutRemoveMultithreaded(true);
    }

    @Test
    public void testFindFirstAndLast() throws Exception {
        MAX_PER_PAGE = 5;

        TestTree tree = createTestTree(true);

        Long first = tree.findFirst();
        assertNull(first);

        Long last = tree.findLast();
        assertNull(last);

        for (long idx = 1L; idx <= 10L; ++idx) {
            tree.put(idx);
        }

        first = tree.findFirst();
        assertEquals((Long) 1L, first);

        last = tree.findLast();
        assertEquals((Long) 10L, last);

        assertNoLocks();
    }

    @Test
    public void testIterate() throws Exception {
        MAX_PER_PAGE = 5;

        TestTree tree = createTestTree(true);

        checkIterate(tree, 0L, 100L, null, false);

        for (long idx = 1L; idx <= 10L; ++idx) {
            tree.put(idx);
        }

        for (long idx = 1L; idx <= 10L; ++idx) {
            checkIterate(tree, idx, 100L, idx, true);
        }

        checkIterate(tree, 0L, 100L, 1L, true);

        for (long idx = 1L; idx <= 10L; ++idx) {
            checkIterate(tree, idx, 100L, 10L, true);
        }

        checkIterate(tree, 0L, 100L, 100L, false);

        for (long idx = 1L; idx <= 10L; ++idx) {
            checkIterate(tree, 0L, 100L, idx, true);
        }

        for (long idx = 0L; idx <= 10L; ++idx) {
            checkIterate(tree, idx, 11L, -1L, false);
        }
    }

    @Test
    public void testIterateConcurrentPutRemove() throws Exception {
        iterateConcurrentPutRemove();
    }

    @Test
    public void testIterateConcurrentPutRemove_1() throws Exception {
        MAX_PER_PAGE = 1;

        iterateConcurrentPutRemove();
    }

    @Test
    public void testIterateConcurrentPutRemove_2() throws Exception {
        MAX_PER_PAGE = 2;

        iterateConcurrentPutRemove();
    }

    @Test
    public void testIteratePutRemove_10() throws Exception {
        MAX_PER_PAGE = 10;

        iterateConcurrentPutRemove();
    }

    private void iterateConcurrentPutRemove() throws Exception {
        final TestTree tree = createTestTree(true);

        // Single key per page is a degenerate case: it is very hard to merge pages in a tree because
        // to merge we need to remove a split key from a parent page and add it to a back page, but this
        // is impossible if we already have a key in a back page, thus we will have lots of empty routing pages.
        // This way the tree grows faster than shrinks and gets out of height limit of 26 (for this page size) quickly.
        // Since the tree height can not be larger than the key count for this case, we can use 26 as a safe number.
        final int keys = MAX_PER_PAGE == 1 ? 26 : 2_000;

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (int i = 0; i < 5; i++) {
            for (long idx = 0L; idx < keys; ++idx) {
                tree.put(idx);
            }

            final Long findKey;

            if (MAX_PER_PAGE > 0) {
                switch (i) {
                    case 0:
                        findKey = 1L;

                        break;

                    case 1:
                        findKey = (long) MAX_PER_PAGE;

                        break;

                    case 2:
                        findKey = (long) MAX_PER_PAGE - 1;

                        break;

                    case 3:
                        findKey = (long) MAX_PER_PAGE + 1;

                        break;

                    case 4:
                        findKey = (long) (keys / MAX_PER_PAGE / 2) * MAX_PER_PAGE;

                        break;

                    case 5:
                        findKey = (long) (keys / MAX_PER_PAGE / 2) * MAX_PER_PAGE - 1;

                        break;

                    case 6:
                        findKey = (long) (keys / MAX_PER_PAGE / 2) * MAX_PER_PAGE + 1;

                        break;

                    case 7:
                        findKey = (long) keys - 1;

                        break;

                    default:
                        findKey = rnd.nextLong(keys);
                }
            } else {
                findKey = rnd.nextLong(keys);
            }

            info("Iteration [iter=" + i + ", key=" + findKey + ']');

            assertEquals(findKey, tree.findOne(findKey));
            checkIterate(tree, findKey, findKey, findKey, true);

            CompletableFuture<?> getFut = runMultiThreadedAsync(() -> {
                ThreadLocalRandom rnd1 = ThreadLocalRandom.current();

                TestTreeRowClosure p = new TestTreeRowClosure(findKey);

                TestTreeRowClosure falseP = new TestTreeRowClosure(-1L);

                int cnt = 0;

                while (!stop.get()) {
                    int shift = MAX_PER_PAGE > 0 ? rnd1.nextInt(MAX_PER_PAGE * 2) : rnd1.nextInt(100);

                    checkIterateC(tree, findKey, findKey, p, true);

                    checkIterateC(tree, findKey - shift, findKey, p, true);

                    checkIterateC(tree, findKey - shift, findKey + shift, p, true);

                    checkIterateC(tree, findKey, findKey + shift, p, true);

                    checkIterateC(tree, -100L, keys + 100L, falseP, false);

                    cnt++;
                }

                info("Done, read count: " + cnt);

                return null;
            }, 5, "find");

            asyncRunFut = getFut;

            try {
                Thread.sleep(50);

                for (int j = 0; j < 20; j++) {
                    for (long idx = 0L; idx < keys / 2; ++idx) {
                        long toRmv = rnd.nextLong(keys);

                        if (toRmv != findKey) {
                            tree.remove(toRmv);
                        }
                    }

                    for (long idx = 0L; idx < keys / 2; ++idx) {
                        long put = rnd.nextLong(keys);

                        tree.put(put);
                    }
                }
            } finally {
                stop.set(true);
            }

            asyncRunFut.get(getTestTimeout(), MILLISECONDS);

            stop.set(false);
        }
    }

    @Test
    public void testConcurrentGrowDegenerateTreeAndConcurrentRemove() throws Exception {
        // Calculate tree size when split happens.
        final TestTree t = createTestTree(true);
        long i = 0;

        for (; ; i++) {
            t.put(i);

            if (t.rootLevel() > 0) { // Split happened.
                break;
            }
        }

        final long treeStartSize = i;

        final AtomicReference<Throwable> failed = new AtomicReference<>();

        for (int k = 0; k < 100; k++) {
            final TestTree tree = createTestTree(true);

            final AtomicBoolean start = new AtomicBoolean();

            final AtomicInteger ready = new AtomicInteger();

            Thread first = new Thread(() -> {
                ready.incrementAndGet();

                while (!start.get()) {
                    // Waiting without blocking.
                }

                try {
                    tree.remove(treeStartSize / 2L);
                } catch (Throwable th) {
                    failed.set(th);
                }
            });

            Thread second = new Thread(() -> {
                ready.incrementAndGet();

                while (!start.get()) {
                    // Waiting without blocking.
                }

                try {
                    tree.put(treeStartSize + 1);
                } catch (Throwable th) {
                    failed.set(th);
                }
            });

            for (int j = 0; j < treeStartSize; j++) {
                tree.put((long) j);
            }

            first.start();
            second.start();

            while (ready.get() != 2) {
                // No-op.
            }

            start.set(true);

            first.join();
            second.join();

            assertNull(failed.get());
        }
    }

    /**
     * Test checks a rare case when, after a parallel removal from the b+tree (cleaning), an empty leaf could remain.
     *
     * <p>Schematically, this can happen like this:
     *
     * <p>B+tree before clearing:
     * <pre>
     *            [ 2 ]
     *         /         \
     *    [ 1 ]         [ 3 | 4 ]
     *    /   \       /     |    \
     * [ 1 ] [ 2 ]  [ 3 ] [ 4 ] [ 5 ]
     * </pre>
     *
     * <p>Parallel deletions of keys:
     *
     * <p>Remove 2:
     * <pre>
     *       [ 1 ]
     *     /       \
     *  [ ]       [ 3 | 4 ]
     *   |       /    |    \
     * [ 1 ]  [ 3 ] [ 4 ] [ 5 ]
     * </pre>
     *
     * <p>Remove 5:
     * <pre>
     *       [ 1 ]
     *     /       \
     *  [ ]       [ 3 ]
     *   |       /    \
     * [ 1 ]  [ 3 ]  [ 4 ]
     * </pre>
     *
     * <p>Remove 4:
     * <pre>
     *     [ 1 ]
     *    /     \
     *  [ ]     [ ]
     *   |       |
     * [ 1 ]   [ 3 ]
     * </pre>
     *
     * <p>Remove 3:
     * <pre>
     * [ 1 ]
     *   |
     *  [ ]
     *   |
     * [ 1 ]
     * </pre>
     *
     * <p>Remove 1 before cutting root and inner node:
     * <pre>
     *  [ ]
     *   |
     *  [ ]
     * </pre>
     *
     * <p>^^^ An empty leaf remains so that this does not happen when "1" is removed, an empty root and an inner node remain, which we will
     * cut off (BPlusTree.Remove#cutRoot).
     *
     * <p>Remove 1 after cutting root and inner node:
     * <pre>
     *  [ ]
     * </pre>
     *
     * @throws Exception If failed.
     */
    @Test
    public void testEmptyLeafAfterConcurrentRemoves() throws Exception {
        MAX_PER_PAGE = 2;

        for (int i = 0; i < 500; i++) {
            TestTree tree = createTestTree(true);

            List<Long> values = new ArrayList<>();

            for (long j = 0; j < 32; j++) {
                values.add(j);

                tree.put(j);
            }

            shuffle(values);

            Queue<Long> queue = new ConcurrentLinkedQueue<>(values);

            int threads = 8;

            CyclicBarrier barrier = new CyclicBarrier(threads);

            runMultiThreaded(() -> {
                barrier.await(getTestTimeout(), MILLISECONDS);

                Long remove;

                while ((remove = queue.poll()) != null) {
                    tree.remove(remove);
                }

                return null;
            }, threads, "remove-from-tree-test-thread");

            tree.validateTree();
        }
    }

    @Test
    void testFindNext() throws Exception {
        TestTree tree = createTestTree(true);

        assertNull(tree.findNext(0L, false));
        assertNull(tree.findNext(0L, true));

        tree.put(0L);

        assertNull(tree.findNext(0L, false));
        assertEquals(0L, tree.findNext(0L, true));

        tree.put(1L);

        assertEquals(1L, tree.findNext(0L, false));
        assertEquals(0L, tree.findNext(0L, true));

        assertNull(tree.findNext(1L, false));
        assertEquals(1L, tree.findNext(1L, true));

        assertEquals(0L, tree.findNext(-1L, false));
        assertEquals(0L, tree.findNext(-1L, true));
    }

    @Test
    void testFindOneWithMapper() throws Exception {
        TestTree tree = createTestTree(true);

        tree.put(0L);

        TreeRowMapClosure<Long, Long, String> treeRowClosure = new TreeRowMapClosure<>() {
            @Override
            public String map(Long treeRow) {
                return "row" + treeRow;
            }
        };

        assertEquals("row0", tree.findOne(0L, treeRowClosure, null));
        assertEquals("rownull", tree.findOne(1L, treeRowClosure, null));
    }

    @Test
    void testFindWithMapper() throws Exception {
        TestTree tree = createTestTree(true);

        tree.put(0L);
        tree.put(1L);

        TreeRowMapClosure<Long, Long, String> treeRowClosure = new TreeRowMapClosure<>() {
            @Override
            public String map(Long treeRow) {
                return "row" + treeRow;
            }
        };

        Cursor<String> cursor = tree.find(null, null, treeRowClosure, null);

        assertTrue(cursor.hasNext());
        assertEquals("row0", cursor.next());

        assertTrue(cursor.hasNext());
        assertEquals("row1", cursor.next());

        assertFalse(cursor.hasNext());
        assertThrows(NoSuchElementException.class, cursor::next);
    }

    @Test
    void testInvokeClosureWithOnUpdateCallbackForPut() throws Exception {
        TestTree tree = createTestTree(true);

        // Checks insert.
        CompletableFuture<Void> future0 = new CompletableFuture<>();

        tree.invoke(0L, null, new InvokeClosure<>() {
            @Override
            public void call(@Nullable Long oldRow) {
                assertNull(oldRow);
            }

            @Override
            public @Nullable Long newRow() {
                return 0L;
            }

            @Override
            public OperationType operationType() {
                return PUT;
            }

            @Override
            public void onUpdate() {
                future0.complete(null);
            }
        });

        assertThat(future0, willCompleteSuccessfully());

        assertEquals(0L, tree.findOne(0L));

        // Checks replace.
        CompletableFuture<Void> future1 = new CompletableFuture<>();

        tree.invoke(0L, null, new InvokeClosure<>() {
            @Override
            public void call(@Nullable Long oldRow) {
                assertEquals(0L, oldRow);
            }

            @Override
            public @Nullable Long newRow() {
                return 0L;
            }

            @Override
            public OperationType operationType() {
                return PUT;
            }

            @Override
            public void onUpdate() {
                future1.complete(null);
            }
        });

        assertThat(future1, willCompleteSuccessfully());

        assertEquals(0L, tree.findOne(0L));
    }

    @Test
    void testInvokeClosureWithOnUpdateCallbackForRemove() throws Exception {
        TestTree tree = createTestTree(true);

        tree.put(0L);

        CompletableFuture<Void> future = new CompletableFuture<>();

        tree.invoke(0L, null, new InvokeClosure<>() {
            @Override
            public void call(@Nullable Long oldRow) {
                assertEquals(0L, oldRow);
            }

            @Override
            public @Nullable Long newRow() {
                return null;
            }

            @Override
            public OperationType operationType() {
                return REMOVE;
            }

            @Override
            public void onUpdate() {
                future.complete(null);
            }
        });

        assertThat(future, willCompleteSuccessfully());

        assertNull(tree.findOne(0L));
    }

    private void doTestRandomPutRemoveMultithreaded(boolean canGetRow) throws Exception {
        final TestTree tree = createTestTree(canGetRow);

        final Map<Long, Long> map = new ConcurrentHashMap<>();

        final int loops = reuseList == null ? 10_000 : 30_000;

        final IgniteStripedLock lock = new IgniteStripedLock(256);

        final String[] ops = {"put", "rmv", "inv_put", "inv_rmv"};

        CompletableFuture<?> fut = runMultiThreadedAsync(() -> {
            for (int i = 0; i < loops && !stop.get(); i++) {
                final Long x = (long) DataStructure.randomInt(CNT);
                final int op = DataStructure.randomInt(4);

                if (i % 5_000 == 0) {
                    println(" --> " + ops[op] + "_" + i + "  " + x);
                }

                Lock l = lock.getLock(x.longValue());

                l.lock();

                try {
                    if (op == 0) { // Put.
                        assertEquals(map.put(x, x), tree.put(x));

                        assertNoLocks();
                    } else if (op == 1) { // Remove.
                        if (map.remove(x) != null) {
                            assertEquals(x, tree.remove(x));

                            assertNoLocks();
                        }

                        assertNull(tree.remove(x));

                        assertNoLocks();
                    } else if (op == 2) {
                        tree.invoke(x, null, new InvokeClosure<>() {
                            OperationType opType;

                            @Override
                            public void call(@Nullable Long row) {
                                opType = PUT;

                                if (row != null) {
                                    assertEquals(x, row);
                                }
                            }

                            @Override
                            public Long newRow() {
                                return x;
                            }

                            @Override
                            public OperationType operationType() {
                                return opType;
                            }
                        });

                        map.put(x, x);
                    } else if (op == 3) {
                        tree.invoke(x, null, new InvokeClosure<Long>() {
                            OperationType opType;

                            @Override
                            public void call(@Nullable Long row) {
                                if (row != null) {
                                    assertEquals(x, row);
                                    opType = REMOVE;
                                } else {
                                    opType = NOOP;
                                }
                            }

                            @Override
                            public Long newRow() {
                                return null;
                            }

                            @Override
                            public OperationType operationType() {
                                return opType;
                            }
                        });

                        map.remove(x);
                    } else {
                        fail();
                    }
                } finally {
                    l.unlock();
                }
            }

            return null;
        }, CPUS, "put-remove");

        CompletableFuture<?> fut2 = runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                Thread.sleep(1_000);

                println(TestTree.printLocks());
            }

            return null;
        }, 1, "printLocks");

        CompletableFuture<?> fut3 = runMultiThreadedAsync(() -> {
            while (!stop.get()) {
                int low = DataStructure.randomInt(CNT);
                int high = low + DataStructure.randomInt(CNT - low);

                Cursor<Long> c = tree.find((long) low, (long) high);

                Long last = null;

                while (c.hasNext()) {
                    Long t = c.next();

                    // Correct bounds.
                    assertTrue(t >= low, low + " <= " + t + " <= " + high);
                    assertTrue(t <= high, low + " <= " + t + " <= " + high);

                    if (last != null) {  // No duplicates.
                        assertTrue(t > last, low + " <= " + last + " < " + t + " <= " + high);
                    }

                    last = t;
                }

                TestTreeFindFirstClosure cl = new TestTreeFindFirstClosure();

                tree.iterate((long) low, (long) high, cl);

                last = cl.val;

                if (last != null) {
                    assertTrue(last >= low, low + " <= " + last + " <= " + high);
                    assertTrue(last <= high, low + " <= " + last + " <= " + high);
                }
            }

            return null;
        }, 4, "find");

        asyncRunFut = CompletableFuture.allOf(fut, fut2, fut3);

        try {
            fut.get(getTestTimeout(), MILLISECONDS);
        } finally {
            stop.set(true);

            asyncRunFut.get(getTestTimeout(), MILLISECONDS);
        }

        Cursor<Long> cursor = tree.find(null, null);

        while (cursor.hasNext()) {
            Long x = cursor.next();

            assert x != null;

            assertEquals(map.get(x), x);
        }

        info("size: " + map.size());

        assertEquals(map.size(), tree.size());

        tree.validateTree();

        assertNoLocks();
    }

    private static int size(Cursor<?> c) {
        int cnt = 0;

        while (c.hasNext()) {
            c.next();

            cnt++;
        }

        return cnt;
    }

    protected static void checkPageId(long pageId, long pageAddr) {
        long actual = getPageId(pageAddr);

        // Page ID must be 0L for newly allocated page, for reused page effective ID must remain the same.
        if (actual != 0L && pageId != actual) {
            throw new IllegalStateException("Page ID: " + hexLong(actual));
        }
    }

    protected TestTree createTestTree(boolean canGetRow) throws Exception {
        TestTree tree = new TestTree(allocateMetaPage(), reuseList, canGetRow, pageMem);

        assertEquals(0, tree.size());
        assertEquals(0, tree.rootLevel());

        return tree;
    }

    private FullPageId allocateMetaPage() throws Exception {
        return new FullPageId(pageMem.allocatePage(reuseList, GROUP_ID, 0, FLAG_AUX), GROUP_ID);
    }

    /**
     * Test tree.
     */
    protected static class TestTree extends BplusTree<Long, Long> {
        /** Number of retries. */
        private int numRetries = super.getLockRetries();

        /**
         * Constructor.
         *
         * @param metaPageId Meta page ID.
         * @param reuseList Reuse list.
         * @param canGetRow Can get row from inner page.
         * @param pageMem Page memory.
         * @throws Exception If failed.
         */
        public TestTree(
                FullPageId metaPageId,
                @Nullable ReuseList reuseList,
                boolean canGetRow,
                PageMemory pageMem
        ) throws Exception {
            super(
                    "test",
                    metaPageId.groupId(),
                    null,
                    partitionId(metaPageId.pageId()),
                    pageMem,
                    new TestPageLockListener(PageLockListenerNoOp.INSTANCE),
                    new AtomicLong(),
                    metaPageId.pageId(),
                    reuseList,
                    new IoVersions<>(new LongInnerIo(canGetRow)),
                    new IoVersions<>(new LongLeafIo()),
                    new IoVersions<>(new LongMetaIo())
            );

            ((TestPageIoRegistry) pageMem.ioRegistry()).load(new IoVersions<>(new LongInnerIo(canGetRow)));
            ((TestPageIoRegistry) pageMem.ioRegistry()).load(new IoVersions<>(new LongLeafIo()));
            ((TestPageIoRegistry) pageMem.ioRegistry()).load(new IoVersions<>(new LongMetaIo()));

            initTree(true);
        }

        /** {@inheritDoc} */
        @Override
        protected int compare(BplusIo<Long> io, long pageAddr, int idx, Long n2) throws IgniteInternalCheckedException {
            Long n1 = io.getLookupRow(this, pageAddr, idx);

            return Long.compare(n1, n2);
        }

        /** {@inheritDoc} */
        @Override
        public Long getRow(BplusIo<Long> io, long pageAddr, int idx, Object ignore) throws IgniteInternalCheckedException {
            assert io.canGetRow() : io;

            return io.getLookupRow(this, pageAddr, idx);
        }

        static Object threadId() {
            return Thread.currentThread().getId(); // .getName();
        }

        private static void printLocks(IgniteStringBuilder b, ConcurrentMap<Object, Map<Long, Long>> locks, Map<Object, Long> beforeLock) {
            for (Map.Entry<Object, Map<Long, Long>> entry : locks.entrySet()) {
                Object thId = entry.getKey();

                Long z = beforeLock.get(thId);

                Set<Map.Entry<Long, Long>> xx = entry.getValue().entrySet();

                if (z == null && xx.isEmpty()) {
                    continue;
                }

                b.app(" ## " + thId);

                if (z != null) {
                    b.app("   --> ").appendHex(z).app("  (").appendHex(effectivePageId(z)).app(')');
                }

                b.app('\n');

                for (Map.Entry<Long, Long> x : xx) {
                    b.app(" -  ").appendHex(x.getValue()).app("  (").appendHex(x.getKey()).app(")\n");
                }

                b.app('\n');
            }
        }

        /**
         * Returns list of locks as a string.
         */
        static String printLocks() {
            IgniteStringBuilder b = new IgniteStringBuilder();

            b.app("\n--------read---------\n");

            printLocks(b, TestPageLockListener.readLocks, TestPageLockListener.beforeReadLock);

            b.app("\n-+------write---------\n");

            printLocks(b, TestPageLockListener.writeLocks, TestPageLockListener.beforeWriteLock);

            return b.toString();
        }

        /** {@inheritDoc} */
        @Override
        protected int getLockRetries() {
            return numRetries;
        }
    }

    /**
     * Long inner.
     */
    private static final class LongInnerIo extends BplusInnerIo<Long> {
        /**
         * Constructor.
         *
         * @param canGetRow If we can get full row from this page.
         */
        protected LongInnerIo(boolean canGetRow) {
            super(LONG_INNER_IO, 1, canGetRow, 8);
        }

        /** {@inheritDoc} */
        @Override
        public int getMaxCount(long buf, int pageSize) {
            if (MAX_PER_PAGE != 0) {
                return MAX_PER_PAGE;
            }

            return super.getMaxCount(buf, pageSize);
        }

        /** {@inheritDoc} */
        @Override
        public void store(long dst, int dstIdx, BplusIo<Long> srcIo, long src, int srcIdx) throws IgniteInternalCheckedException {
            Long row = srcIo.getLookupRow(null, src, srcIdx);

            store(dst, dstIdx, row, null, false);
        }

        private void checkNotRemoved(Long row) {
            if (rmvdIds.contains(row)) {
                fail("Removed row: " + row);
            }
        }

        /** {@inheritDoc} */
        @Override
        public void storeByOffset(long pageAddr, int off, Long row) {
            checkNotRemoved(row);

            putLong(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override
        public Long getLookupRow(BplusTree<Long, ?> tree, long pageAddr, int idx) {
            Long row = getLong(pageAddr, offset(idx));

            checkNotRemoved(row);

            return row;
        }
    }

    /**
     * Returns page memory.
     *
     * @throws Exception If failed.
     */
    protected abstract PageMemory createPageMemory() throws Exception;

    /**
     * Returns number of acquired pages.
     */
    protected abstract long acquiredPages();

    /**
     * Long leaf.
     */
    private static final class LongLeafIo extends BplusLeafIo<Long> {
        LongLeafIo() {
            super(LONG_LEAF_IO, 1, 8);
        }

        /** {@inheritDoc} */
        @Override
        public int getMaxCount(long pageAddr, int pageSize) {
            if (MAX_PER_PAGE != 0) {
                return MAX_PER_PAGE;
            }

            return super.getMaxCount(pageAddr, pageSize);
        }

        /** {@inheritDoc} */
        @Override
        public void storeByOffset(long pageAddr, int off, Long row) {
            putLong(pageAddr, off, row);
        }

        /** {@inheritDoc} */
        @Override
        public void store(long dst, int dstIdx, BplusIo<Long> srcIo, long src, int srcIdx) {
            assertSame(srcIo, this);

            putLong(dst, offset(dstIdx), getLong(src, offset(srcIdx)));
        }

        /** {@inheritDoc} */
        @Override
        public Long getLookupRow(BplusTree<Long, ?> tree, long pageAddr, int idx) {
            return getLong(pageAddr, offset(idx));
        }
    }

    /**
     * {@link TreeRowClosure} implementation for the test.
     */
    static class TestTreeRowClosure implements TreeRowClosure<Long, Long> {
        private final Long expVal;

        private boolean found;

        /**
         * Constructor.
         *
         * @param expVal Value to find or {@code null} to find first.
         */
        TestTreeRowClosure(Long expVal) {
            this.expVal = expVal;
        }

        /** {@inheritDoc} */
        @Override
        public boolean apply(BplusTree<Long, Long> tree, BplusIo<Long> io, long pageAddr, int idx) throws IgniteInternalCheckedException {
            assertFalse(found);

            found = expVal == null || io.getLookupRow(tree, pageAddr, idx).equals(expVal);

            return !found;
        }
    }

    /**
     * {@link TreeRowClosure} implementation for the test.
     */
    static class TestTreeFindFirstClosure implements TreeRowClosure<Long, Long> {
        private Long val;

        /** {@inheritDoc} */
        @Override
        public boolean apply(BplusTree<Long, Long> tree, BplusIo<Long> io, long pageAddr, int idx) throws IgniteInternalCheckedException {
            assertNull(val);

            val = io.getLookupRow(tree, pageAddr, idx);

            return false;
        }
    }

    /**
     * {@link TreeRowClosure} implementation for the test.
     */
    static class TestTreeFindFilteredClosure implements TreeRowMapClosure<Long, Long, Long> {
        private final Set<Long> vals;

        /**
         * Constructor.
         *
         * @param vals Values to allow in filter.
         */
        TestTreeFindFilteredClosure(Set<Long> vals) {
            this.vals = vals;
        }

        /** {@inheritDoc} */
        @Override
        public boolean apply(BplusTree<Long, Long> tree, BplusIo<Long> io, long pageAddr, int idx) throws IgniteInternalCheckedException {
            Long val = io.getLookupRow(tree, pageAddr, idx);

            return vals.contains(val);
        }
    }

    /**
     * {@link PageLockListener} implementation for the test.
     */
    private static class TestPageLockListener implements PageLockListener {
        static ConcurrentMap<Object, Long> beforeReadLock = new ConcurrentHashMap<>();

        static ConcurrentMap<Object, Long> beforeWriteLock = new ConcurrentHashMap<>();

        static ConcurrentMap<Object, Map<Long, Long>> readLocks = new ConcurrentHashMap<>();

        static ConcurrentMap<Object, Map<Long, Long>> writeLocks = new ConcurrentHashMap<>();

        private final PageLockListener delegate;

        /**
         * Constructor.
         *
         * @param delegate Real implementation of page lock listener.
         */
        private TestPageLockListener(PageLockListener delegate) {
            this.delegate = delegate;
        }

        /** {@inheritDoc} */
        @Override
        public void onBeforeReadLock(int cacheId, long pageId, long page) {
            delegate.onBeforeReadLock(cacheId, pageId, page);

            if (PRINT_LOCKS) {
                println("  onBeforeReadLock: " + hexLong(pageId));
            }

            assertNull(beforeReadLock.put(threadId(), pageId));
        }

        /** {@inheritDoc} */
        @Override
        public void onReadLock(int cacheId, long pageId, long page, long pageAddr) {
            delegate.onReadLock(cacheId, pageId, page, pageAddr);

            if (PRINT_LOCKS) {
                println("  onReadLock: " + hexLong(pageId));
            }

            if (pageAddr != 0L) {
                long actual = getPageId(pageAddr);

                checkPageId(pageId, pageAddr);

                assertNull(locks(true).put(pageId, actual));
            }

            assertEquals(Long.valueOf(pageId), beforeReadLock.remove(threadId()));
        }

        /** {@inheritDoc} */
        @Override
        public void onReadUnlock(int cacheId, long pageId, long page, long pageAddr) {
            delegate.onReadUnlock(cacheId, pageId, page, pageAddr);

            if (PRINT_LOCKS) {
                println("  onReadUnlock: " + hexLong(pageId));
            }

            checkPageId(pageId, pageAddr);

            long actual = getPageId(pageAddr);

            assertEquals(Long.valueOf(actual), locks(true).remove(pageId));
        }

        /** {@inheritDoc} */
        @Override
        public void onBeforeWriteLock(int cacheId, long pageId, long page) {
            delegate.onBeforeWriteLock(cacheId, pageId, page);

            if (PRINT_LOCKS) {
                println("  onBeforeWriteLock: " + hexLong(pageId));
            }

            assertNull(beforeWriteLock.put(threadId(), pageId));
        }

        /** {@inheritDoc} */
        @Override
        public void onWriteLock(int cacheId, long pageId, long page, long pageAddr) {
            delegate.onWriteLock(cacheId, pageId, page, pageAddr);

            if (PRINT_LOCKS) {
                println("  onWriteLock: " + hexLong(pageId));
            }

            if (pageAddr != 0L) {
                checkPageId(pageId, pageAddr);

                long actual = getPageId(pageAddr);

                if (actual == 0L) {
                    actual = pageId; // It is a newly allocated page.
                }

                assertNull(locks(false).put(pageId, actual));
            }

            assertEquals(Long.valueOf(pageId), beforeWriteLock.remove(threadId()));
        }

        /** {@inheritDoc} */
        @Override
        public void onWriteUnlock(int cacheId, long pageId, long page, long pageAddr) {
            delegate.onWriteUnlock(cacheId, pageId, page, pageAddr);

            if (PRINT_LOCKS) {
                println("  onWriteUnlock: " + hexLong(pageId));
            }

            assertEquals(effectivePageId(pageId), effectivePageId(getPageId(pageAddr)));

            assertEquals(Long.valueOf(pageId), locks(false).remove(pageId));
        }

        /** {@inheritDoc} */
        @Override
        public void close() {
            delegate.close();
        }

        private static Map<Long, Long> locks(boolean readLock) {
            ConcurrentMap<Object, Map<Long, Long>> m = readLock ? readLocks : writeLocks;

            Object thId = threadId();

            Map<Long, Long> locks = m.get(thId);

            if (locks == null) {
                // TODO: https://issues.apache.org/jira/browse/IGNITE-16350
                locks = new ConcurrentHashMap<>();

                if (m.putIfAbsent(thId, locks) != null) {
                    locks = m.get(thId);
                }
            }

            return locks;
        }

        /**
         * Returns {@code true} if current thread does not keep any locks.
         */
        static boolean checkNoLocks() {
            return locks(true).isEmpty() && locks(false).isEmpty();
        }
    }

    /**
     * Alias for {@code System.out.println}.
     *
     * @param msg String to print.
     */
    protected static void println(@Nullable String msg) {
        System.out.println(msg);
    }

    /**
     * Alias for {@code System.out.print}.
     *
     * @param msg String to print.
     */
    protected static void print(@Nullable String msg) {
        System.out.print(msg);
    }

    /**
     * Output a message to the log.
     *
     * @param msg Message to print.
     */
    protected void info(String msg) {
        IgniteLogger log = logger();

        if (log.isInfoEnabled()) {
            log.info(msg);
        }
    }

    /**
     * Check that we do not keep any locks at the moment.
     */
    protected void assertNoLocks() {
        assertTrue(TestPageLockListener.checkNoLocks());
    }

    /**
     * Long meta.
     */
    private static class LongMetaIo extends BplusMetaIo {
        public LongMetaIo() {
            super(LONG_META_IO, 1);
        }
    }
}
