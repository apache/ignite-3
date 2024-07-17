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

import static org.apache.ignite.internal.configuration.ConfigurationTestUtils.fixConfiguration;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.partitionId;
import static org.apache.ignite.internal.pagememory.util.PageUtils.putInt;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.runAsync;
import static org.apache.ignite.internal.util.Constants.MiB;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.Externalizable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.TestPageIoRegistry;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileChange;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfiguration;
import org.apache.ignite.internal.pagememory.configuration.schema.VolatilePageMemoryProfileConfigurationSchema;
import org.apache.ignite.internal.pagememory.inmemory.VolatilePageMemory;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.tree.io.BplusInnerIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusLeafIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusMetaIo;
import org.apache.ignite.internal.pagememory.util.PageLockListenerNoOp;
import org.apache.ignite.internal.pagememory.util.PageUtils;
import org.apache.ignite.internal.storage.configurations.StorageProfileConfiguration;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test is based on {@link AbstractBplusTreePageMemoryTest} and has a partial copy of its code.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItBplusTreeReplaceRemoveRaceTest extends BaseIgniteAbstractTest {
    private static final short PAIR_INNER_IO = 30000;

    private static final short PAIR_LEAF_IO = 30001;

    private static final short PAIR_META_IO = 30002;

    private static final int GROUP_ID = 100500;

    @InjectConfiguration(polymorphicExtensions = { VolatilePageMemoryProfileConfigurationSchema.class }, value = "mock.engine = aimem")
    private StorageProfileConfiguration dataRegionCfg;

    @Nullable
    protected PageMemory pageMem;

    @BeforeEach
    protected void beforeEach() throws Exception {
        pageMem = createPageMemory();

        pageMem.start();
    }

    @AfterEach
    protected void afterTest() {
        if (pageMem != null) {
            pageMem.stop(true);
        }
    }

    protected PageMemory createPageMemory() throws Exception {
        dataRegionCfg
                .change(c -> ((VolatilePageMemoryProfileChange) c)
                        .changeInitSize(1024 * MiB)
                        .changeMaxSize(1024 * MiB))
                .get(1, TimeUnit.SECONDS);

        TestPageIoRegistry ioRegistry = new TestPageIoRegistry();

        ioRegistry.loadFromServiceLoader();

        return new VolatilePageMemory(
                (VolatilePageMemoryProfileConfiguration) fixConfiguration(dataRegionCfg),
                ioRegistry,
                512
        );
    }

    private FullPageId allocateMetaPage() throws IgniteInternalCheckedException {
        return new FullPageId(pageMem.allocatePageNoReuse(GROUP_ID, 0, FLAG_AUX), GROUP_ID);
    }

    /**
     * Short for {@code IgniteBiTuple<Integer, Integer>}.
     */
    protected static class Pair extends IgniteBiTuple<Integer, Integer> {
        /** Serial version uid. */
        private static final long serialVersionUID = 0L;

        /**
         * No-arg constructor for {@link Externalizable}.
         */
        public Pair() {
        }

        public Pair(Integer key, Integer val) {
            super(key, val);
        }
    }

    /**
     * Test tree, maps {@link Integer} to {@code Integer}.
     */
    protected static class TestPairTree extends BplusTree<Pair, Pair> {
        /**
         * Constructor.
         *
         * @param metaPageId Meta page ID.
         * @param pageMem Page memory.
         * @throws IgniteInternalCheckedException If failed.
         */
        public TestPairTree(
                FullPageId metaPageId,
                PageMemory pageMem
        ) throws IgniteInternalCheckedException {
            super(
                    "test",
                    metaPageId.groupId(),
                    null,
                    partitionId(metaPageId.pageId()),
                    pageMem,
                    PageLockListenerNoOp.INSTANCE,
                    new AtomicLong(),
                    metaPageId.pageId(),
                    null,
                    new IoVersions<>(new TestPairInnerIo()),
                    new IoVersions<>(new TestPairLeafIo()),
                    new IoVersions<>(new TestPairMetaIo())
            );

            ((TestPageIoRegistry) pageMem.ioRegistry()).load(new IoVersions<>(new TestPairInnerIo()));
            ((TestPageIoRegistry) pageMem.ioRegistry()).load(new IoVersions<>(new TestPairLeafIo()));
            ((TestPageIoRegistry) pageMem.ioRegistry()).load(new IoVersions<>(new TestPairMetaIo()));

            initTree(true);
        }

        /** {@inheritDoc} */
        @Override
        protected int compare(BplusIo<Pair> io, long pageAddr, int idx, Pair n2) throws IgniteInternalCheckedException {
            Pair n1 = io.getLookupRow(this, pageAddr, idx);

            return Integer.compare(n1.getKey(), n2.getKey());
        }

        /** {@inheritDoc} */
        @Override
        public Pair getRow(BplusIo<Pair> io, long pageAddr, int idx, Object ignore) throws IgniteInternalCheckedException {
            return io.getLookupRow(this, pageAddr, idx);
        }
    }

    /**
     * Test inner node, for {@link Pair}.
     */
    private static final class TestPairInnerIo extends BplusInnerIo<Pair> {
        TestPairInnerIo() {
            super(PAIR_INNER_IO, 1, true, 8);
        }

        /** {@inheritDoc} */
        @Override
        public int getMaxCount(long buf, int pageSize) {
            return 2;
        }

        /** {@inheritDoc} */
        @Override
        public void store(long dst, int dstIdx, BplusIo<Pair> srcIo, long src, int srcIdx) throws IgniteInternalCheckedException {
            Pair row = srcIo.getLookupRow(null, src, srcIdx);

            store(dst, dstIdx, row, null, false);
        }

        /** {@inheritDoc} */
        @Override
        public void storeByOffset(long pageAddr, int off, Pair row) {
            putInt(pageAddr, off, row.getKey());
            putInt(pageAddr, off + 4, row.getValue());
        }

        /** {@inheritDoc} */
        @Override
        public Pair getLookupRow(BplusTree<Pair, ?> tree, long pageAddr, int idx) {
            int key = PageUtils.getInt(pageAddr, offset(idx));
            int val = PageUtils.getInt(pageAddr, offset(idx) + 4);

            return new Pair(key, val);
        }
    }

    /**
     * Test leaf node, for {@link Pair}.
     */
    private static final class TestPairLeafIo extends BplusLeafIo<Pair> {
        TestPairLeafIo() {
            super(PAIR_LEAF_IO, 1, 8);
        }

        /** {@inheritDoc} */
        @Override
        public int getMaxCount(long pageAddr, int pageSize) {
            return 2;
        }

        /** {@inheritDoc} */
        @Override
        public void storeByOffset(long pageAddr, int off, Pair row) {
            putInt(pageAddr, off, row.getKey());
            putInt(pageAddr, off + 4, row.getValue());
        }

        /** {@inheritDoc} */
        @Override
        public void store(long dst, int dstIdx, BplusIo<Pair> srcIo, long src, int srcIdx) throws IgniteInternalCheckedException {
            Pair row = srcIo.getLookupRow(null, src, srcIdx);

            store(dst, dstIdx, row, null, false);
        }

        /** {@inheritDoc} */
        @Override
        public Pair getLookupRow(BplusTree<Pair, ?> tree, long pageAddr, int idx) {
            int key = PageUtils.getInt(pageAddr, offset(idx));
            int val = PageUtils.getInt(pageAddr, offset(idx) + 4);

            return new Pair(key, val);
        }
    }

    /**
     * Tests a very specific scenario for concurrent replace and remove that used to corrupt the tree before the fix.
     * <p/>
     *
     * <p>Consider the following tree, represented by a {@code tree} variable in test:<br>
     * <pre><code>
     *                                    [ 5:0 ]
     *                                /            \
     *                 [ 2:0 | 4:0 ]                  [ 6:0 ]
     *               /       |       \               /       \
     * [ 1:0 | 2:0 ]   [ 3:0 | 4:0 ]   [ 5:0 ]   [ 6:0 ]   [ 7:0 ]
     * </code></pre>
     *
     * <p>Individual replace {@code 4:0} to {@code 4:8} would take two steps and look like this:
     * <pre><code>
     * // Inner node goes first.
     *                                    [ 5:0 ]
     *                                /            \
     *                 [ 2:0 | 4:8 ]                  [ 6:0 ]
     *               /       |       \               /       \
     * [ 1:0 | 2:0 ]   [ 3:0 | 4:0 ]   [ 5:0 ]   [ 6:0 ]   [ 7:0 ]
     *
     * // Leaf node goes last.
     *                                    [ 5:0 ]
     *                                /            \
     *                 [ 2:0 | 4:8 ]                  [ 6:0 ]
     *               /       |       \               /       \
     * [ 1:0 | 2:0 ]   [ 3:0 | 4:8 ]   [ 5:0 ]   [ 6:0 ]   [ 7:0 ]
     * </code></pre>
     *
     * <p>Note that inbetween these two updates tree is fully unlocked and available for modifications. So, if one tries to remove
     * {@code 5:0} during the replacement, following modifications would happen:
     * <pre><code>
     * // Inner node update from replacement goes first, as before.
     *                                    [ 5:0 ]
     *                                /            \
     *                 [ 2:0 | 4:8 ]                  [ 6:0 ]
     *               /       |       \               /       \
     * [ 1:0 | 2:0 ]   [ 3:0 | 4:0 ]   [ 5:0 ]   [ 6:0 ]   [ 7:0 ]
     *
     * // Removal of 5:0 starts from the leaf.
     *                                    [ 5:0 ]
     *                                /            \
     *                 [ 2:0 | 4:8 ]                  [ 6:0 ]
     *               /       |       \               /       \
     * [ 1:0 | 2:0 ]   [ 3:0 | 4:0 ]   []        [ 6:0 ]   [ 7:0 ]
     *
     * // Merge of empty branch is now required, 4:8 is removed from inner node.
     *                                [ 5:0 ]
     *                            /              \
     *                 [ 2:0 ]                        [ 6:0 ]
     *               /         \                     /       \
     * [ 1:0 | 2:0 ]             [ 3:0 | 4:0 ]   [ 6:0 ]   [ 7:0 ]
     *
     * // Inner replace is happening in the root. To do that, closest left value is retrieved from the leaf, it's 4:0.
     *                                [ 4:0 ]
     *                            /              \
     *                 [ 2:0 ]                        [ 6:0 ]
     *               /         \                     /       \
     * [ 1:0 | 2:0 ]             [ 3:0 | 4:0 ]   [ 6:0 ]   [ 7:0 ]
     *
     * // At this point removal is complete. Last replacement step will do the following.
     *                               [ 4:0 ]
     *                            /              \
     *                 [ 2:0 ]                        [ 6:0 ]
     *               /         \                     /       \
     * [ 1:0 | 2:0 ]             [ 3:0 | 4:8 ]   [ 6:0 ]   [ 7:0 ]
     * </code></pre>
     *
     * <p>It is clear that root has an invalid value {@code 4:0}, hence the tree should be considered corrupted. This is the exact situation
     * that test is trying to check.
     * <p/>
     *
     * <p>Several iterations are required for this, given that there's no guaranteed way to force a tree to perform page modifications in
     * the desired order. Typically, less than {@code 10} attempts have been required to get a corrupted tree. Value {@code 100} is
     * arbitrary and has been chosen to be big enough for test to fail in case of regression, but not too big so that test won't run for too
     * long.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentPutRemove() throws Exception {
        for (int i = 0; i < 100; i++) {
            TestPairTree tree = prepareBplusTree();

            // Exact tree from the description is constructed at this point.
            CyclicBarrier barrier = new CyclicBarrier(2);

            // This is the replace operation.
            CompletableFuture<?> putFut = runAsync(() -> {
                barrier.await();

                tree.putx(new Pair(4, 999));
            });

            // This is the remove operation.
            CompletableFuture<?> remFut = runAsync(() -> {
                barrier.await();

                tree.removex(new Pair(5, -1));
            });

            // Wait for both operations.
            try {
                putFut.get(1, TimeUnit.SECONDS);
            } finally {
                remFut.get(1, TimeUnit.SECONDS);
            }

            // Just in case.
            tree.validateTree();

            // Find a value associated with 4. It'll be right in the root page.
            Pair pair = tree.findOne(new Pair(4, -1));

            // Assert that it is valid.
            assertEquals(999, pair.getValue().intValue());
        }
    }

    /**
     * Checks that there will be no corrupted B+tree during concurrent update and deletion of the same key that is contained in the inner
     * and leaf nodes of the B+tree.
     *
     * <p>NOTE: Test logic is the same as of {@link #testConcurrentPutRemove}, the only difference is that it operates (puts and removes)
     * on a single key.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testConcurrentPutRemoveSameRow() throws Exception {
        for (int i = 0; i < 100; i++) {
            TestPairTree tree = prepareBplusTree();

            // Exact tree from the description is constructed at this point.
            CyclicBarrier barrier = new CyclicBarrier(2);

            // This is the replace operation.
            CompletableFuture<?> putFut = runAsync(() -> {
                barrier.await();

                tree.putx(new Pair(5, 999));
            });

            // This is the remove operation.
            CompletableFuture<?> remFut = runAsync(() -> {
                barrier.await();

                tree.removex(new Pair(5, 0));
            });

            // Wait for both operations.
            try {
                putFut.get(1, TimeUnit.SECONDS);
            } finally {
                remFut.get(1, TimeUnit.SECONDS);
            }

            // Just in case.
            tree.validateTree();
        }
    }

    /**
     * Creates and fills a tree.
     * <pre><code>
     *                                    [ 5:0 ]
     *                                /            \
     *                 [ 2:0 | 4:0 ]                  [ 6:0 ]
     *               /       |       \              /      |
     * [ 1:0 | 2:0 ]->[ 3:0 | 4:0 ]->[ 5:0 ]->[ 6:0 ]->[ 7:0 ]
     * </code></pre>
     *
     * @return New B+tree.
     * @throws Exception If failed.
     */
    private TestPairTree prepareBplusTree() throws Exception {
        TestPairTree tree = new TestPairTree(allocateMetaPage(), pageMem);

        tree.putx(new Pair(1, 0));
        tree.putx(new Pair(2, 0));
        tree.putx(new Pair(4, 0));
        tree.putx(new Pair(6, 0));
        tree.putx(new Pair(7, 0));

        // Split root.
        tree.putx(new Pair(5, 0));

        // Split its left subtree.
        tree.putx(new Pair(3, 0));

        return tree;
    }

    /**
     * Test meta node, for {@link Pair}.
     */
    private static class TestPairMetaIo extends BplusMetaIo {
        public TestPairMetaIo() {
            super(PAIR_META_IO, 1);
        }
    }
}
