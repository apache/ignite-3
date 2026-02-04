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

import static org.apache.ignite.internal.lang.IgniteSystemProperties.getInteger;
import static org.apache.ignite.internal.pagememory.PageIdAllocator.FLAG_AUX;
import static org.apache.ignite.internal.pagememory.tree.BplusTree.Bool.DONE;
import static org.apache.ignite.internal.pagememory.tree.BplusTree.Bool.FALSE;
import static org.apache.ignite.internal.pagememory.tree.BplusTree.Bool.READY;
import static org.apache.ignite.internal.pagememory.tree.BplusTree.Bool.TRUE;
import static org.apache.ignite.internal.pagememory.tree.BplusTree.Result.FOUND;
import static org.apache.ignite.internal.pagememory.tree.BplusTree.Result.GO_DOWN;
import static org.apache.ignite.internal.pagememory.tree.BplusTree.Result.GO_DOWN_X;
import static org.apache.ignite.internal.pagememory.tree.BplusTree.Result.NOT_FOUND;
import static org.apache.ignite.internal.pagememory.tree.BplusTree.Result.RETRY;
import static org.apache.ignite.internal.pagememory.tree.BplusTree.Result.RETRY_ROOT;
import static org.apache.ignite.internal.pagememory.util.PageIdUtils.effectivePageId;
import static org.apache.ignite.internal.util.ArrayUtils.OBJECT_EMPTY_ARRAY;
import static org.apache.ignite.internal.util.ArrayUtils.clearTail;
import static org.apache.ignite.internal.util.ArrayUtils.set;
import static org.apache.ignite.internal.util.StringUtils.hexLong;

import it.unimi.dsi.fastutil.longs.LongArrayFIFOQueue;
import it.unimi.dsi.fastutil.longs.LongArrays;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringBuilder;
import org.apache.ignite.internal.lang.IgniteTuple3;
import org.apache.ignite.internal.pagememory.CorruptedDataStructureException;
import org.apache.ignite.internal.pagememory.PageMemory;
import org.apache.ignite.internal.pagememory.datastructure.DataStructure;
import org.apache.ignite.internal.pagememory.io.IoVersions;
import org.apache.ignite.internal.pagememory.io.PageIo;
import org.apache.ignite.internal.pagememory.reuse.LongListReuseBag;
import org.apache.ignite.internal.pagememory.reuse.ReuseBag;
import org.apache.ignite.internal.pagememory.reuse.ReuseList;
import org.apache.ignite.internal.pagememory.tree.io.BplusInnerIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusLeafIo;
import org.apache.ignite.internal.pagememory.tree.io.BplusMetaIo;
import org.apache.ignite.internal.pagememory.util.GradualTask;
import org.apache.ignite.internal.pagememory.util.PageHandler;
import org.apache.ignite.internal.tostring.S;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.FastTimestamps;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.jetbrains.annotations.Nullable;

/**
 * <h3>Abstract B+Tree</h3>
 *
 * <p>B+Tree is a block-based tree structure. Each block is represented with the page ({@link PageIo}) and contains a single tree node.
 * There are two types of pages/nodes: {@link BplusInnerIo} and {@link BplusLeafIo}.
 *
 * <p>Every page in the tree contains a list of <i>items</i>. Item is just a fixed-size binary payload. Inner nodes and leaves may have
 * different item sizes. There's a limit on how many items each page can hold. It is defined by a {@link BplusIo#getMaxCount(int)}
 * method of the corresponding IO. There should be no empty pages in trees, so:
 * <ul>
 *     <li>a leaf page must have from {@code 1} to {@code max} items</li>
 *     <li>
 *         an inner page must have from {@code 0} to {@code max} items (an inner page with 0 items is a routing page,
 *         it still has 1 pointer to 1 child, it's not considered an empty page; see below)
 *     </li>
 * </ul>
 *
 * <p>Items might have different meaning depending on the type of the page. In case of leaves, every item must describe a key and a value.
 * In case of inner nodes, items describe only keys if {@link #canGetRowFromInner} is {@code false}, or a key and a value otherwise. Items
 * in every page are sorted according to the order dscribed by {@link #compare(BplusIo, long, int, Object)} method. Specifics of the data
 * stored in items are defined in the implementation and generally don't matter.
 *
 * <p>All pages in the tree are divided into levels. Leaves are always at the level {@code 0}. Levels of inner pages are thus positive.
 * Each
 * level represents a singly linked list - each page has a link to the <i>forward</i> page at the same level. It can be retrieved by calling
 * {@link BplusIo#getForward(long, int)}. This link must be a zero if there's no forward page. Forward links on level {@code 0} allow
 * iterating tree's keys and values effectively without traversing any inner nodes ({@code AbstractForwardCursor}). Forward links in inner
 * nodes have different purpose, more on that later.
 *
 * <p>Leaves have no links other than forward links. But inner nodes also have links to their children nodes. Every inner node can be
 * viewed
 * like the following structure:
 * <pre>{@code
 *       item(0)     item(1)        ...          item(N-1)
 * link(0)     link(1)     link(2)  ...  link(N-1)       link(N)
 * }</pre>
 * There are {@code N} items and {@code N+1} links. Each link points to page of a lower level. For example, pages on level {@code 2} always
 * point to pages of level {@code 1}. For an item {@code i} left subtree is defined by {@code link(i)} and right subtree is defined by
 * {@code link(i+1)} ({@link BplusInnerIo#getLeft(long, int, int)} and {@link BplusInnerIo#getRight(long, int, int)}). All items in the left
 * subtree are less or equal to the original item (basic property for the trees).
 *
 * <p>There's one more important property of these links: {@code forward(left(i)) == right(i)}. It is called the
 * <i>triangle invariant</i>. More information on B+Tree structure can easily be found online. Following documentation
 * concentrates more on specifics of this particular B+Tree implementation.
 *
 * <h3>General operations</h3>
 * This implementation allows for concurrent reads and update. Given that each page locks individually, there are general rules to avoid
 * deadlocks.
 * <ul>
 *     <li>
 *         Pages within a level always locked from left to right.
 *     </li>
 *     <li>
 *         If there's already a lock on the page of level X then no locks should be acquired on levels less than X.
 *         In other words, locks are aquired from the bottom to the top (in the direction from leaves to root). The only exception to this
 *         rule is the allocation of a new page on a lower level that no one sees yet.
 *         </li>
 * </ul>
 * All basic operations fit into a similar pattern. First, the search is performed ({@link Get}). It goes recursively
 * from the root to the leaf (if it's needed). On each level several outcomes are possible.
 * <ul>
 *     <li>Exact value is found on the leaf level and operation can be completed.</li>
 *     <li>Insertion point is found and recursive procedure continues on the lower level.</li>
 *     <li>Insertion point is not found due to concurrent modifications, but retry in the same node is possible.</li>
 *     <li>Insertion point is not found due to concurrent modifications, but retry in the same node is impossible.</li>
 * </ul>
 * All these options, and more, are described in the class {@link Result}. Please refer to its usages for specifics of
 * each operation. Once the path and the leaf for put/remove is found, the operation is then performed from the bottom
 * to the top. Specifics are described in corresponding classes ({@link Put}, {@link Remove}).
 *
 * <h3>Maintained invariants</h3>
 * <ol>
 *     <li>Triangle invariant (see above), used to detect concurrent tree structure changes</li>
 *     <li>Each key existing in an inner page also exists in exactly one leaf, as its rightmost key</li>
 *     <li>
 *         For each leaf that is not the rightmost leaf in the tree (i.e. its forwardId is not 0), its rightmost key
 *         exists in exactly one of its ancestor blocks.
 *         <p/>
 *         The invariant is maintained using special cases in insert with split, replace and remove scenarios.
 *     </li>
 * </ol>
 *
 * <h3>Invariants that are NOT maintained</h3>
 * <ol>
 *     <li>
 *         Classic <a href="https://en.wikipedia.org/wiki/B-tree">B-Tree</a> (and B+Tree as well) makes sure
 *         that each non-root node is at least half-full. This implementation does NOT maintain this invariant.
 *     </li>
 * </ol>
 *
 * <h3>Merge properties</h3>
 * When a key is removed from a leaf node, the node might become empty and hence a mandatory merge happens. If
 * the parent is a <em>routing page</em> (see below), another mandatory merge will happen. (Mandatory merges are
 * the ones that must happen to maintain the tree invariants). This procedure may propagate a few levels up if there
 * is a chain of routing pages as ancestors.
 *
 * <p>After all mandatory merges happen, we try to go up and make another merge (by merging the reached ancestor and its
 * sibling, if they fit in one block). Such a merge is called a <em>regular merge</em> in the code. It is not
 * mandatory to maintain invariants, but it improves tree structure from the point of view of performance. If first
 * regular merge is successful, the attempt will be repeated one level higher, and so on.
 *
 * <h3>Routing pages</h3>
 * An inner (i.e. non-leaf) page is called a <em>routing page</em> if it contains zero items (hence, zero keys), but
 * it still contains one pointer to a child one level below. (This is valid because an inner page contains one pointer
 * more than item count.)
 *
 * <p>An inner page becomes a routing page when removing last item from it (as a consequence to one of its children
 * becoming empty due to a removal somewhere below), AND due to inability to merge the page with its sibling because
 * the sibling is full.
 *
 * <p>A confusion might arise between routing pages and empty pages. A routing page does not contain any items, but it does
 * contain a pointer to its single child, so it is not treated as an empty page (and we keep such pages in the tree).
 */
@SuppressWarnings("ConstantValueVariableUse")
public abstract class BplusTree<L, T extends L> extends DataStructure implements IgniteTree<L, T> {
    /** Destroy msg. */
    private static final String CONC_DESTROY_MSG = "Tree is being concurrently destroyed: ";

    /** Number of repetitions to capture a lock in the B+Tree. */
    // TODO: IGNITE-16350 Delete or move to configuration.
    private static final String IGNITE_BPLUS_TREE_LOCK_RETRIES = "IGNITE_BPLUS_TREE_LOCK_RETRIES";

    /** Number of retries. */
    private static final int LOCK_RETRIES = getInteger(IGNITE_BPLUS_TREE_LOCK_RETRIES, 1000);

    /** Flag that the tree is destroyed. */
    private final AtomicBoolean destroyed = new AtomicBoolean(false);

    private final float minFill;

    private final float maxFill;

    /** Meta page id. */
    protected final long metaPageId;

    private boolean canGetRowFromInner;

    // TODO: IGNITE-16350 Make it final.
    private IoVersions<? extends BplusInnerIo<L>> innerIos;

    // TODO: IGNITE-16350 Make it final.
    private IoVersions<? extends BplusLeafIo<L>> leafIos;

    // TODO: IGNITE-16350 Make it final.
    private IoVersions<? extends BplusMetaIo> metaIos;

    /**
     * Global remove ID, for a tree that was created for the first time it can be {@code 0}, for restored ones it  must be greater than or
     * equal to the previous value.
     *
     * <p>Used to define an inner replace when deleting the rightmost element of a leaf (but strictly not a tree) that was also stored in
     * the root inner. If, when restoring a tree, the value is less than what was previously in the tree, it can lead to a loop in the tree
     * when searching through it if there were inner replace.</p>
     */
    private final AtomicLong globalRmvId;

    /** Tree meta data. */
    private volatile TreeMetaData treeMeta;

    /** Flag for enabling single-threaded append-only tree creation. */
    private boolean sequentialWriteOptsEnabled;

    /**
     * B+tree structure printer.
     */
    private final IgniteTreePrinter<Long> treePrinter = new IgniteTreePrinter<>() {
        @Override
        protected List<Long> getChildren(Long pageId) {
            if (pageId == null || pageId == 0L) {
                return null;
            }

            try {
                long page = acquirePage(pageId);

                try {
                    long pageAddr = readLock(pageId, page); // No correctness guaranties.

                    if (pageAddr == 0) {
                        return null;
                    }

                    try {
                        BplusIo<L> io = io(pageAddr);

                        if (io.isLeaf()) {
                            return Collections.emptyList();
                        }

                        int cnt = io.getCount(pageAddr);

                        assert cnt >= 0 : cnt;

                        List<Long> res;

                        if (cnt > 0) {
                            res = new ArrayList<>(cnt + 1);

                            for (int i = 0; i < cnt; i++) {
                                res.add(inner(io).getLeft(pageAddr, i, partId));
                            }

                            res.add(inner(io).getRight(pageAddr, cnt - 1, partId));
                        } else {
                            long left = inner(io).getLeft(pageAddr, 0, partId);

                            res = left == 0 ? Collections.emptyList() : Collections.singletonList(left);
                        }

                        return res;
                    } finally {
                        readUnlock(pageId, page, pageAddr);
                    }
                } finally {
                    releasePage(pageId, page);
                }
            } catch (IgniteInternalCheckedException ignored) {
                throw new AssertionError("Can not acquire page.");
            }
        }

        @Override
        protected String formatTreeNode(Long pageId) {
            if (pageId == null) {
                return ">NPE<";
            }

            if (pageId == 0L) {
                return "<Zero>";
            }

            try {
                long page = acquirePage(pageId);
                try {
                    long pageAddr = readLock(pageId, page); // No correctness guaranties.
                    if (pageAddr == 0) {
                        return "<Obsolete>";
                    }

                    try {
                        BplusIo<L> io = io(pageAddr);

                        return printPage(io, pageAddr, true);
                    } finally {
                        readUnlock(pageId, page, pageAddr);
                    }
                } finally {
                    releasePage(pageId, page);
                }
            } catch (IgniteInternalCheckedException e) {
                throw new IllegalStateException(e);
            }
        }
    };

    private final PageHandler<Get, Result> askNeighbor;

    /**
     * Page handler to asking neighbors.
     */
    private class AskNeighbor extends GetPageHandler<Get> {
        @Override
        public Result run0(long pageId, long page, long pageAddr, BplusIo<L> io, Get g, int isBack) {
            assert !io.isLeaf(); // Inner page.

            boolean back = isBack == TRUE.ordinal();

            long res = doAskNeighbor(io, pageAddr, back);

            if (back) {
                if (io.getForward(pageAddr, partId) != g.backId) {
                    // See how g.backId is setup in removeDown for this check.
                    return RETRY;
                }

                g.backId(res);
            } else {
                assert isBack == FALSE.ordinal() : isBack;

                g.fwdId(res);
            }

            return FOUND;
        }
    }

    private final PageHandler<Get, Result> search;

    /**
     * Page handler to search page.
     */
    public class Search extends GetPageHandler<Get> {
        @Override
        public Result run0(
                long pageId,
                long page,
                long pageAddr,
                BplusIo<L> io,
                Get g,
                int lvl
        ) throws IgniteInternalCheckedException {
            // Check the triangle invariant.
            if (io.getForward(pageAddr, partId) != g.fwdId) {
                return RETRY;
            }

            boolean needBackIfRouting = g.backId != 0;

            g.backId(0L); // Usually we'll go left down and don't need it.

            int cnt = io.getCount(pageAddr);

            int idx;

            if (g.findLast) {
                // (-cnt - 1) mimics not_found result of findInsertionPoint
                idx = io.isLeaf() ? cnt - 1 : -cnt - 1;
            } else {
                // in case of cnt = 0 we end up in 'not found' branch below with idx being 0 after fix() adjustment
                idx = findInsertionPoint(lvl, io, pageAddr, 0, cnt, g.row, g.shift);
            }

            boolean found = idx >= 0;

            if (found) { // Found exact match.
                assert g.getClass() != GetCursor.class;

                if (g.found(io, pageAddr, idx, lvl)) {
                    return FOUND;
                }

                // Else we need to reach leaf page, go left down.
            } else {
                idx = fix(idx);

                if (g.notFound(io, pageAddr, idx, lvl)) {
                    // No way down, stop here.
                    return NOT_FOUND;
                }
            }

            assert !io.isLeaf() : io;

            // If idx == cnt then we go right down, else left down: getLeft(cnt) == getRight(cnt - 1).
            g.pageId(inner(io).getLeft(pageAddr, idx, partId));

            // If we see the tree in consistent state, then our right down page must be forward for our left down page,
            // we need to setup fwdId and/or backId to be able to check this invariant on lower level.
            if (idx < cnt) {
                // Go left down here.
                g.fwdId(inner(io).getRight(pageAddr, idx, partId));
            } else {
                // Go right down here or it is an empty branch.
                assert idx == cnt;

                // Here child's forward is unknown to us (we either go right or it is an empty "routing" page),
                // need to ask our forward about the child's forward (it must be leftmost child of our forward page).
                // This is ok from the locking standpoint because we take all locks in the forward direction.
                long fwdId = io.getForward(pageAddr, partId);

                // Setup fwdId.
                if (fwdId == 0) {
                    g.fwdId(0L);
                } else {
                    // We can do askNeighbor on forward page here because we always take locks in forward direction.
                    Result res = askNeighbor(fwdId, g, false);

                    if (res != FOUND) {
                        return res; // Retry.
                    }
                }

                // Setup backId.
                if (cnt != 0) {
                    // It is not a routing page and we are going to the right, can get backId here.
                    g.backId(inner(io).getLeft(pageAddr, cnt - 1, partId));
                } else if (needBackIfRouting) {
                    // Can't get backId here because of possible deadlock and it is only needed for remove operation.
                    return GO_DOWN_X;
                }
            }

            return GO_DOWN;
        }
    }

    private final PageHandler<Put, Result> replace;

    /**
     * Page handler to replace page.
     */
    public class Replace extends GetPageHandler<Put> {
        @Override
        public Result run0(
                long pageId,
                long page,
                long pageAddr,
                BplusIo<L> io,
                Put p,
                int lvl
        ) throws IgniteInternalCheckedException {
            // Check the triangle invariant.
            if (io.getForward(pageAddr, partId) != p.fwdId) {
                return RETRY;
            }

            assert p.btmLvl == 0 : "split is impossible with replace";
            assert lvl == 0 : "Replace via page handler is only possible on the leaves level.";

            int cnt = io.getCount(pageAddr);
            int idx = findInsertionPoint(lvl, io, pageAddr, 0, cnt, p.row, 0);

            if (idx < 0) {
                // Not found, split or merge happened.
                return RETRY;
            }

            assert p.oldRow == null : "The old row must be set only once.";

            // Lock the leaf if the row should be replaced in an inner node as well.
            if (canGetRowFromInner && idx + 1 == cnt && p.fwdId != 0L) {
                Tail<L> tail = p.addTail(pageId, page, pageAddr, io, lvl, Tail.EXACT);

                // Row index is cached, because it won't change until the leaf is unlocked.
                tail.idx = (short) idx;

                return FOUND;
            }

            // Row exists in this leaf only. No other actions will be required.

            // Read old row before actual replacement.
            p.oldRow = p.needOld ? getRow(io, pageAddr, idx) : (T) Boolean.TRUE;

            p.replaceRowInPage(io, pageAddr, idx);

            p.finish();

            return FOUND;
        }
    }

    private final PageHandler<Put, Result> insert;

    /**
     * Page handler to insert page.
     */
    public class Insert extends GetPageHandler<Put> {
        @Override
        public Result run0(
                long pageId,
                long page,
                long pageAddr,
                BplusIo<L> io,
                Put p,
                int lvl
        ) throws IgniteInternalCheckedException {
            assert p.btmLvl == lvl : "we must always insert at the bottom level: " + p.btmLvl + " " + lvl;

            // Check triangle invariant.
            if (io.getForward(pageAddr, partId) != p.fwdId) {
                return RETRY;
            }

            int cnt = io.getCount(pageAddr);
            int idx = findInsertionPoint(lvl, io, pageAddr, 0, cnt, p.row, 0);

            if (idx >= 0) {
                // We do not support concurrent put of the same key.
                throw new IllegalStateException("Duplicate row in index.");
            }

            idx = fix(idx);

            // Do insert.
            L moveUpRow = p.insert(pageId, pageAddr, io, idx, lvl);

            // Check if split happened.
            if (moveUpRow != null) {
                p.btmLvl++; // Get high.
                p.row = moveUpRow;

                if (p.invoke != null) {
                    p.invoke.row = moveUpRow;
                }

                // Here forward page can't be concurrently removed because we keep write lock on tail which is the only
                // page who knows about the forward page, because it was just produced by split.
                p.rightId = io.getForward(pageAddr, partId);
                p.setTailForSplit(pageId, page, pageAddr, io, p.btmLvl - 1);

                assert p.rightId != 0;
            } else {
                p.finish();
            }

            return FOUND;
        }
    }

    private final PageHandler<Remove, Result> rmvFromLeaf;

    /**
     * Page handler to remove from the sheet.
     */
    private class RemoveFromLeaf extends GetPageHandler<Remove> {
        @Override
        public Result run0(
                long leafId,
                long leafPage,
                long leafAddr,
                BplusIo<L> io,
                Remove r,
                int lvl
        ) throws IgniteInternalCheckedException {
            assert lvl == 0 : lvl; // Leaf.

            // Check the triangle invariant.
            if (io.getForward(leafAddr, partId) != r.fwdId) {
                return RETRY;
            }

            int cnt = io.getCount(leafAddr);

            assert cnt <= Short.MAX_VALUE : cnt;

            int idx = findInsertionPoint(lvl, io, leafAddr, 0, cnt, r.row, 0);

            if (idx < 0) {
                return RETRY; // We've found exact match on search but now it's gone.
            }

            assert idx >= 0 && idx < cnt : idx;

            // Need to do inner replace when we remove the rightmost element and the leaf has a forward page,
            // i.e. it is not the rightmost leaf of the tree.
            boolean needReplaceInner = canGetRowFromInner && idx == cnt - 1 && io.getForward(leafAddr, partId) != 0;

            // !!! Before modifying state we have to make sure that we will not go for retry.

            // We may need to replace inner key or want to merge this leaf with sibling after the remove -> keep lock.
            if (needReplaceInner
                    // We need to make sure that we have back or forward to be able to merge.
                    || ((r.fwdId != 0 || r.backId != 0) && mayMerge(cnt - 1, io.getMaxCount(pageSize())))) {
                // If we have backId then we've already locked back page, nothing to do here.
                if (r.fwdId != 0 && r.backId == 0) {
                    Result res = r.lockForward(0);

                    if (res != FOUND) {
                        assert r.tail == null;

                        return res; // Retry.
                    }

                    assert r.tail != null; // We've just locked forward page.
                }

                // Retry must reset these fields when we release the whole branch without remove.
                assert !r.needReplaceInner && r.needMergeEmptyBranch == FALSE
                        : "needReplaceInner=" + r.needReplaceInner + ", needMergeEmptyBranch=" + r.needMergeEmptyBranch;

                if (cnt == 1) {
                    // It was the last element on the leaf.
                    r.needMergeEmptyBranch = TRUE;
                }

                r.needReplaceInner = needReplaceInner;

                Tail<L> t = r.addTail(leafId, leafPage, leafAddr, io, 0, Tail.EXACT);

                t.idx = (short) idx;

                // We will do the actual remove only when we made sure that
                // we've locked the whole needed branch correctly.
                return FOUND;
            }

            r.removeDataRowFromLeaf(leafAddr, io, cnt, idx);

            return FOUND;
        }
    }

    private final PageHandler<Remove, Result> lockBackAndRmvFromLeaf;

    /**
     * Page handler to lock back and remove from leaf.
     */
    private class LockBackAndRmvFromLeaf extends GetPageHandler<Remove> {
        @Override
        protected Result run0(
                long backId,
                long backPage,
                long backAddr,
                BplusIo<L> io,
                Remove r,
                int lvl
        ) throws IgniteInternalCheckedException {
            // Check that we have consistent view of the world.
            if (io.getForward(backAddr, partId) != r.pageId) {
                return RETRY;
            }

            // Correct locking order: from back to forward.
            Result res = r.doRemoveFromLeaf();

            // Keep locks on back and leaf pages for subsequent merges.
            if (res == FOUND && r.tail != null) {
                r.addTail(backId, backPage, backAddr, io, lvl, Tail.BACK);
            }

            return res;
        }
    }

    private final PageHandler<Remove, Result> lockBackAndTail;

    /**
     * Page handler to lock tail back.
     */
    private class LockBackAndTail extends GetPageHandler<Remove> {
        @Override
        public Result run0(
                long backId,
                long backPage,
                long backAddr,
                BplusIo<L> io,
                Remove r,
                int lvl
        ) throws IgniteInternalCheckedException {
            // Check that we have consistent view of the world.
            if (io.getForward(backAddr, partId) != r.pageId) {
                return RETRY;
            }

            // Correct locking order: from back to forward.
            Result res = r.doLockTail(lvl);

            if (res == FOUND) {
                r.addTail(backId, backPage, backAddr, io, lvl, Tail.BACK);
            }

            return res;
        }
    }

    private final PageHandler<Remove, Result> lockTailForward;

    /**
     * Page handler to lock tail forward.
     */
    private class LockTailForward extends GetPageHandler<Remove> {
        @Override
        protected Result run0(
                long pageId,
                long page,
                long pageAddr,
                BplusIo<L> io,
                Remove r,
                int lvl
        ) {
            r.addTail(pageId, page, pageAddr, io, lvl, Tail.FORWARD);

            return FOUND;
        }
    }

    private final PageHandler<Update, Result> lockTailExact;

    /**
     * Page handler that adds the page to the tail of {@link Update} object. Results in {@link Result#FOUND} if added to tail successfully.
     * Results in {@link Result#RETRY} if triangle invariant is violated.
     */
    private class LockTailExact extends GetPageHandler<Update> {
        @Override
        protected Result run0(
                long pageId,
                long page,
                long pageAddr,
                BplusIo<L> io,
                Update u,
                int lvl
        ) {
            // Check the triangle invariant.
            if (io.getForward(pageAddr, partId) != u.fwdId) {
                return RETRY;
            }

            u.addTail(pageId, page, pageAddr, io, lvl, Tail.EXACT);

            return FOUND;
        }
    }

    private final PageHandler<Remove, Result> lockTail;

    /**
     * Page handler for lock the tail.
     */
    private class LockTail extends GetPageHandler<Remove> {
        @Override
        public Result run0(
                long pageId,
                long page,
                long pageAddr,
                BplusIo<L> io,
                Remove r,
                int lvl
        ) throws IgniteInternalCheckedException {
            assert lvl > 0 : lvl; // We are not at the bottom.

            // Check that we have a correct view of the world.
            if (io.getForward(pageAddr, partId) != r.fwdId) {
                return RETRY;
            }

            // We don't have a back page, need to lock our forward and become a back for it.
            if (r.fwdId != 0 && r.backId == 0) {
                Result res = r.lockForward(lvl);

                if (res != FOUND) {
                    return res; // Retry.
                }
            }

            r.addTail(pageId, page, pageAddr, io, lvl, Tail.EXACT);

            return FOUND;
        }
    }

    private final PageHandler<Void, Bool> cutRoot = new CutRoot();

    /**
     * Page handler for cutting the root.
     */
    private class CutRoot implements PageHandler<Void, Bool> {
        @Override
        public Bool run(
                int groupId,
                long metaId,
                long metaPage,
                long metaAddr,
                PageIo iox,
                Void ignore,
                int lvl
        ) {
            // Safe cast because we should never recycle meta page until the tree is destroyed.
            BplusMetaIo io = (BplusMetaIo) iox;

            assert lvl == io.getRootLevel(metaAddr); // Can drop only root.

            io.cutRoot(metaAddr);

            int newLvl = lvl - 1;

            assert io.getRootLevel(metaAddr) == newLvl;

            treeMeta = new TreeMetaData(newLvl, io.getFirstPageId(metaAddr, newLvl, partId));

            return TRUE;
        }
    }

    private final PageHandler<Long, Bool> addRoot = new AddRoot();

    /**
     * Page handler for adding the root.
     */
    private class AddRoot implements PageHandler<Long, Bool> {
        @Override
        public Bool run(
                int groupId,
                long metaId,
                long metaPage,
                long pageAddr,
                PageIo iox,
                Long rootPageId,
                int lvl
        ) {
            assert rootPageId != null;

            // Safe cast because we should never recycle meta page until the tree is destroyed.
            BplusMetaIo io = (BplusMetaIo) iox;

            assert lvl == io.getLevelsCount(pageAddr);

            io.addRoot(pageAddr, rootPageId);

            assert io.getRootLevel(pageAddr) == lvl;
            assert io.getFirstPageId(pageAddr, lvl, partId) == rootPageId;

            treeMeta = new TreeMetaData(lvl, rootPageId);

            return TRUE;
        }
    }

    private final PageHandler<Long, Bool> initRoot = new InitRoot();

    /**
     * Page handler to initialize the root.
     */
    private class InitRoot implements PageHandler<Long, Bool> {
        @Override
        public Bool run(
                int groupId,
                long metaId,
                long metaPage,
                long pageAddr,
                PageIo iox,
                Long rootId,
                int notUsed
        ) {
            assert rootId != null;

            // Safe cast because we should never recycle meta page until the tree is destroyed.
            BplusMetaIo io = (BplusMetaIo) iox;

            io.initRoot(pageAddr, rootId);

            assert io.getRootLevel(pageAddr) == 0;
            assert io.getFirstPageId(pageAddr, 0, partId) == rootId;

            treeMeta = new TreeMetaData(0, rootId);

            return TRUE;
        }
    }

    /**
     * Constructor.
     *
     * @param treeNamePrefix Tree name prefix (for debugging purposes).
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param partId Partition ID.
     * @param pageMem Page memory.
     * @param globalRmvId Global remove ID, for a tree that was created for the first time it can be {@code 0}, for restored ones it
     *      must be greater than or equal to the previous value.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     * @param innerIos Inner IO versions.
     * @param leafIos Leaf IO versions.
     * @param metaIos Meta IO versions.
     */
    protected BplusTree(
            String treeNamePrefix,
            int grpId,
            @Nullable String grpName,
            int partId,
            PageMemory pageMem,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList,
            IoVersions<? extends BplusInnerIo<L>> innerIos,
            IoVersions<? extends BplusLeafIo<L>> leafIos,
            IoVersions<? extends BplusMetaIo> metaIos
    ) {
        this(treeNamePrefix, grpId, grpName, partId, pageMem, globalRmvId, metaPageId, reuseList);

        setIos(innerIos, leafIos, metaIos);
    }

    /**
     * Constructor.
     *
     * @param treeNamePrefix Tree name prefix (for debugging purposes).
     * @param grpId Group ID.
     * @param grpName Group name.
     * @param pageMem Page memory.
     * @param globalRmvId Global remove ID, for a tree that was created for the first time it can be {@code 0}, for restored ones it
     *      must be greater than or equal to the previous value.
     * @param metaPageId Meta page ID.
     * @param reuseList Reuse list.
     */
    protected BplusTree(
            String treeNamePrefix,
            int grpId,
            @Nullable String grpName,
            int partId,
            PageMemory pageMem,
            AtomicLong globalRmvId,
            long metaPageId,
            @Nullable ReuseList reuseList
    ) {
        super(treeNamePrefix, grpId, grpName, partId, pageMem, FLAG_AUX);

        // TODO: IGNITE-16350 Move to config.
        minFill = 0.0f; // Testing worst case when merge happens only on empty page.
        maxFill = 0.0f; // Avoiding random effects on testing.

        assert metaPageId != 0L;

        this.metaPageId = metaPageId;
        this.reuseList = reuseList;
        this.globalRmvId = globalRmvId;

        // Initialize page handlers.
        askNeighbor = new AskNeighbor();
        search = new Search();
        lockTailExact = new LockTailExact();
        lockTail = new LockTail();
        lockTailForward = new LockTailForward();
        lockBackAndTail = new LockBackAndTail();
        lockBackAndRmvFromLeaf = new LockBackAndRmvFromLeaf();
        rmvFromLeaf = new RemoveFromLeaf();
        insert = new Insert();
        replace = new Replace();
    }

    /**
     * Sets inner and leaf IO versions.
     *
     * @param innerIos Inner IO versions.
     * @param leafIos Leaf IO versions.
     * @param metaIos Meta IO versions.
     */
    public void setIos(
            IoVersions<? extends BplusInnerIo<L>> innerIos,
            IoVersions<? extends BplusLeafIo<L>> leafIos,
            IoVersions<? extends BplusMetaIo> metaIos
    ) {
        // TODO IGNITE-16350 Refactor and make it always true.
        this.canGetRowFromInner = innerIos.latest().canGetRow();
        this.innerIos = innerIos;
        this.leafIos = leafIos;
        this.metaIos = metaIos;
    }

    /** Flag for enabling single-threaded append-only tree creation. */
    public void enableSequentialWriteMode() {
        sequentialWriteOptsEnabled = true;
    }

    /**
     * Initialize new tree.
     *
     * @param initNew {@code True} if new tree should be created.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected final void initTree(boolean initNew) throws IgniteInternalCheckedException {
        if (initNew) {
            // Allocate the first leaf page, it will be our root.
            long rootId = allocatePage(null);

            init(rootId, latestLeafIo());

            // Initialize meta page with new root page.
            Bool res = write(metaPageId, initRoot, latestMetaIo(), rootId, 0, FALSE);

            assert res == TRUE : res;

            assert treeMeta != null;
        }
    }

    /**
     * Returns tree meta data.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    private TreeMetaData treeMeta() throws IgniteInternalCheckedException {
        return treeMeta(0L);
    }

    /**
     * Returns tree meta data.
     *
     * @param metaPageAddr Meta page address. If equals {@code 0}, it means that we should do read lock on meta page and get meta page
     *      address. Otherwise we will not do the lock and will use the given address.
     * @throws IgniteInternalCheckedException If failed.
     */
    private TreeMetaData treeMeta(long metaPageAddr) throws IgniteInternalCheckedException {
        TreeMetaData meta0 = treeMeta;

        if (meta0 != null) {
            return meta0;
        }

        long metaPage = acquirePage(metaPageId);

        try {
            long pageAddr;

            if (metaPageAddr == 0L) {
                pageAddr = readLock(metaPageId, metaPage);

                assert pageAddr != 0 : "Failed to read lock meta page [metaPageId=" + hexLong(metaPageId) + ']';
            } else {
                pageAddr = metaPageAddr;
            }

            try {
                BplusMetaIo io = metaIos.forPage(pageAddr);

                int rootLvl = io.getRootLevel(pageAddr);
                long rootId = io.getFirstPageId(pageAddr, rootLvl, partId);

                meta0 = new TreeMetaData(rootLvl, rootId);

                treeMeta = meta0;
            } finally {
                if (metaPageAddr == 0L) {
                    readUnlock(metaPageId, metaPage, pageAddr);
                }
            }
        } finally {
            releasePage(metaPageId, metaPage);
        }

        return meta0;
    }

    /**
     * Returns root level.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    private int getRootLevel() throws IgniteInternalCheckedException {
        return getRootLevel(0L);
    }

    /**
     * Returns root level.
     *
     * @param metaPageAddr Meta page address. If equals {@code 0}, it means that we should do read lock on meta page and get meta page
     *      address. Otherwise we will not do the lock and will use the given address.
     * @throws IgniteInternalCheckedException If failed.
     */
    private int getRootLevel(long metaPageAddr) throws IgniteInternalCheckedException {
        TreeMetaData meta0 = treeMeta(metaPageAddr);

        assert meta0 != null;

        return meta0.rootLvl;
    }

    /**
     * Gets the id of the first page in the level.
     *
     * @param metaId Meta page ID.
     * @param metaPage Meta page pointer.
     * @param lvl Level, if {@code 0} then it is a bottom level, if negative then root.
     * @return Page ID.
     */
    private long getFirstPageId(long metaId, long metaPage, int lvl) {
        return getFirstPageId(metaId, metaPage, lvl, 0L);
    }

    /**
     * Gets the id of the first page in the level.
     *
     * @param metaId Meta page ID.
     * @param metaPage Meta page pointer.
     * @param lvl Level, if {@code 0} then it is a bottom level, if negative then root.
     * @param metaPageAddr Meta page address. If equals {@code 0}, it means that we should do read lock on meta page and get meta page
     *      address. Otherwise we will not do the lock and will use the given address.
     * @return Page ID.
     */
    private long getFirstPageId(long metaId, long metaPage, int lvl, long metaPageAddr) {
        long pageAddr = metaPageAddr != 0L ? metaPageAddr : readLock(metaId, metaPage); // Meta can't be removed.

        try {
            BplusMetaIo io = metaIos.forPage(pageAddr);

            if (lvl < 0) {
                lvl = io.getRootLevel(pageAddr);
            }

            if (lvl >= io.getLevelsCount(pageAddr)) {
                return 0;
            }

            return io.getFirstPageId(pageAddr, lvl, partId);
        } finally {
            if (metaPageAddr == 0L) {
                readUnlock(metaId, metaPage, pageAddr);
            }
        }
    }

    /**
     * Getting the cursor through the rows of the tree with lower unbounded.
     *
     * @param upper Upper bound.
     * @param upIncl {@code true} if upper bound is inclusive.
     * @param c Tree row closure.
     * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
     * @return Cursor.
     * @throws IgniteInternalCheckedException If failed.
     */
    private <R> Cursor<R> findLowerUnbounded(
            @Nullable L upper,
            boolean upIncl,
            @Nullable TreeRowMapClosure<L, T, R> c,
            @Nullable Object x
    ) throws IgniteInternalCheckedException {
        ForwardCursor<R> cursor = new ForwardCursor<>(upper, upIncl, c, x);

        long firstPageId;

        long metaPage = acquirePage(metaPageId);
        try {
            firstPageId = getFirstPageId(metaPageId, metaPage, 0); // Level 0 is always at the bottom.
        } finally {
            releasePage(metaPageId, metaPage);
        }

        try {
            long firstPage = acquirePage(firstPageId);

            try {
                long pageAddr = readLock(firstPageId, firstPage); // We always merge pages backwards, the first page is never removed.

                try {
                    cursor.init(pageAddr, io(pageAddr), -1);
                } finally {
                    readUnlock(firstPageId, firstPage, pageAddr);
                }
            } finally {
                releasePage(firstPageId, firstPage);
            }
        } catch (RuntimeException | AssertionError e) {
            throw new BplusTreeRuntimeException(e, grpId, metaPageId, firstPageId);
        }

        return cursor;
    }

    /**
     * Check if the tree is getting destroyed.
     *
     * @throws IgniteInternalCheckedException If destroyed.
     */
    protected final void checkDestroyed() throws IgniteInternalCheckedException {
        if (destroyed.get()) {
            throw new IgniteInternalCheckedException(CONC_DESTROY_MSG + name());
        }
    }

    @Override
    public final Cursor<T> find(@Nullable L lowerInclusive, @Nullable L upperInclusive) throws IgniteInternalCheckedException {
        return find(lowerInclusive, upperInclusive, null);
    }

    @Override
    public final Cursor<T> find(@Nullable L lowerInclusive, @Nullable L upperInclusive, @Nullable Object x)
            throws IgniteInternalCheckedException {
        return find(lowerInclusive, upperInclusive, null, x);
    }

    /**
     * Getting the cursor through the rows of the tree.
     *
     * @param lowerInclusive Lower bound inclusive or {@code null} if unbounded.
     * @param upperInclusive Upper bound inclusive or {@code null} if unbounded.
     * @param c Tree row closure.
     * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
     * @return Cursor.
     * @throws IgniteInternalCheckedException If failed.
     */
    public <R> Cursor<R> find(
            @Nullable L lowerInclusive,
            @Nullable L upperInclusive,
            @Nullable TreeRowMapClosure<L, T, R> c,
            @Nullable Object x
    ) throws IgniteInternalCheckedException {
        return find(lowerInclusive, upperInclusive, true, true, c, x);
    }

    /**
     * Getting the cursor through the rows of the tree.
     *
     * @param lower Lower bound or {@code null} if unbounded.
     * @param upper Upper bound or {@code null} if unbounded.
     * @param lowIncl {@code true} if lower bound is inclusive.
     * @param upIncl {@code true} if upper bound is inclusive.
     * @param c Tree row closure.
     * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
     * @return Cursor.
     * @throws CorruptedDataStructureException If the data structure is broken.
     * @throws CorruptedTreeException If there were {@link RuntimeException} or {@link AssertionError}.
     * @throws IgniteInternalCheckedException If other errors occurred.
     */
    public <R> Cursor<R> find(
            @Nullable L lower,
            @Nullable L upper,
            boolean lowIncl,
            boolean upIncl,
            @Nullable TreeRowMapClosure<L, T, R> c,
            @Nullable Object x
    ) throws IgniteInternalCheckedException {
        checkDestroyed();

        ForwardCursor<R> cursor = new ForwardCursor<>(lower, upper, lowIncl, upIncl, c, x);

        try {
            if (lower == null) {
                return findLowerUnbounded(upper, upIncl, c, x);
            }

            cursor.find();

            return cursor;
        } catch (CorruptedDataStructureException e) {
            throw e;
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalCheckedException("Runtime failure on bounds [lower=" + lower + ", upper=" + upper + "]", e);
        } catch (RuntimeException | AssertionError e) {
            long[] pageIds = pages(
                    lower == null || cursor.getCursor == null,
                    () -> new long[]{cursor.getCursor.pageId}
            );

            throw corruptedTreeException(
                    "Runtime failure on bounds [lower=" + lower + ", upper=" + upper + "]",
                    e,
                    grpId,
                    pageIds
            );
        } finally {
            checkDestroyed();
        }
    }

    /**
     * Iterates over the tree.
     *
     * @param lower Lower bound inclusive.
     * @param upper Upper bound inclusive.
     * @param c Closure applied for all found items, iteration is stopped if closure returns {@code false}.
     * @throws IgniteInternalCheckedException If failed.
     */
    public void iterate(L lower, L upper, TreeRowClosure<L, T> c) throws IgniteInternalCheckedException {
        checkDestroyed();

        ClosureCursor cursor = new ClosureCursor(lower, upper, c);

        try {
            cursor.iterate();
        } catch (CorruptedDataStructureException e) {
            throw e;
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalCheckedException("Runtime failure on bounds [lower=" + lower + ", upper=" + upper + "]", e);
        } catch (RuntimeException | AssertionError e) {
            throw corruptedTreeException(
                    "Runtime failure on bounds [lower=" + lower + ", upper=" + upper + "]",
                    e,
                    grpId,
                    pages(cursor.getCursor != null, () -> new long[]{cursor.getCursor.pageId})
            );
        } finally {
            checkDestroyed();
        }
    }

    /**
     * Visits tree rows.
     *
     * @param lower Lower bound inclusive.
     * @param upper Upper bound inclusive.
     * @param c Closure applied for all found items.
     * @throws IgniteInternalCheckedException If failed.
     */
    public void visit(L lower, L upper, TreeVisitorClosure<L, T> c) throws IgniteInternalCheckedException {
        checkDestroyed();

        try {
            new TreeVisitor(lower, upper, c).visit();
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalCheckedException("Runtime failure on bounds [lower=" + lower + ", upper=" + upper + "]", e);
        } catch (RuntimeException e) {
            throw new IgniteInternalException("Runtime failure on bounds [lower=" + lower + ", upper=" + upper + "]", e);
        } catch (AssertionError e) {
            throw new AssertionError("Assertion error on bounds [lower=" + lower + ", upper=" + upper + "]", e);
        } finally {
            checkDestroyed();
        }
    }

    @Override
    public T findFirst() throws IgniteInternalCheckedException {
        return findFirst(null);
    }

    /**
     * Returns a value mapped to the lowest key, or {@code null} if tree is empty or no entry matches the passed filter.
     *
     * @param filter Filter closure.
     * @return Value.
     * @throws IgniteInternalCheckedException If failed.
     */
    public @Nullable T findFirst(@Nullable TreeRowClosure<L, T> filter) throws IgniteInternalCheckedException {
        checkDestroyed();

        long curPageId = 0L;
        long nextPageId = 0L;

        try {
            for (; ; ) {

                long metaPage = acquirePage(metaPageId);

                try {
                    curPageId = getFirstPageId(metaPageId, metaPage, 0); // Level 0 is always at the bottom.
                } finally {
                    releasePage(metaPageId, metaPage);
                }

                long curPage = acquirePage(curPageId);
                try {
                    long curPageAddr = readLock(curPageId, curPage);

                    if (curPageAddr == 0) {
                        continue; // The first page has gone: restart scan.
                    }

                    try {
                        BplusIo<L> io = io(curPageAddr);

                        assert io.isLeaf();

                        for (; ; ) {
                            int cnt = io.getCount(curPageAddr);

                            for (int i = 0; i < cnt; ++i) {
                                if (filter == null || filter.apply(this, io, curPageAddr, i)) {
                                    return getRow(io, curPageAddr, i);
                                }
                            }

                            nextPageId = io.getForward(curPageAddr, partId);

                            if (nextPageId == 0) {
                                return null;
                            }

                            long nextPage = acquirePage(nextPageId);

                            try {
                                long nextPageAddr = readLock(nextPageId, nextPage);

                                // In the current implementation the next page can't change when the current page is locked.
                                assert nextPageAddr != 0 : nextPageAddr;

                                try {
                                    long pa = curPageAddr;
                                    curPageAddr = 0; // Set to zero to avoid double unlocking in finalizer.

                                    readUnlock(curPageId, curPage, pa);

                                    long p = curPage;
                                    curPage = 0; // Set to zero to avoid double release in finalizer.

                                    releasePage(curPageId, p);

                                    curPageId = nextPageId;
                                    curPage = nextPage;
                                    curPageAddr = nextPageAddr;

                                    nextPage = 0;
                                    nextPageAddr = 0;
                                } finally {
                                    if (nextPageAddr != 0) {
                                        readUnlock(nextPageId, nextPage, nextPageAddr);
                                    }
                                }
                            } finally {
                                if (nextPage != 0) {
                                    releasePage(nextPageId, nextPage);
                                }
                            }
                        }
                    } finally {
                        if (curPageAddr != 0) {
                            readUnlock(curPageId, curPage, curPageAddr);
                        }
                    }
                } finally {
                    if (curPage != 0) {
                        releasePage(curPageId, curPage);
                    }
                }
            }
        } catch (CorruptedDataStructureException e) {
            throw e;
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalCheckedException("Runtime failure on first row lookup", e);
        } catch (RuntimeException | AssertionError e) {
            throw corruptedTreeException("Runtime failure on first row lookup", e, grpId, curPageId, nextPageId);
        } finally {
            checkDestroyed();
        }
    }

    @Override
    public T findLast() throws IgniteInternalCheckedException {
        return findLast(null);
    }

    /**
     * Returns a value mapped to the greatest key, or {@code null} if tree is empty or no entry matches the passed filter.
     *
     * @param c Filter closure.
     * @return Value.
     * @throws IgniteInternalCheckedException If failed.
     */
    public @Nullable T findLast(@Nullable TreeRowClosure<L, T> c) throws IgniteInternalCheckedException {
        checkDestroyed();

        Get g = null;

        try {
            if (c == null) {
                GetOne<T> getOne = new GetOne<>(null, null, null, true);

                g = getOne;

                doFind(g);

                return getOne.res;
            } else {
                GetLast getLast = new GetLast(c);

                g = getLast;

                return getLast.find();
            }
        } catch (CorruptedDataStructureException e) {
            throw e;
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalCheckedException("Runtime failure on last row lookup", e);
        } catch (RuntimeException | AssertionError e) {
            Get g0 = g;

            long[] pageIds = pages(g == null, () -> new long[]{g0.pageId});

            throw corruptedTreeException("Runtime failure on last row lookup", e, grpId, pageIds);
        } finally {
            checkDestroyed();
        }
    }

    /**
     * Returns found result or {@code null}.
     *
     * @param row Lookup row for exact match.
     * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
     * @throws IgniteInternalCheckedException If failed.
     */
    public final <R> @Nullable R findOne(L row, @Nullable Object x) throws IgniteInternalCheckedException {
        return findOne(row, null, x);
    }

    /**
     * Returns found result or {@code null}.
     *
     * @param row Lookup row for exact match.
     * @param c Tree row closure, if the tree row is not found, then {@code null} will be passed to the {@link TreeRowMapClosure#map}.
     * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
     * @throws CorruptedDataStructureException If the data structure is broken.
     * @throws CorruptedTreeException If there were {@link RuntimeException} or {@link AssertionError}.
     * @throws IgniteInternalCheckedException If other errors occurred.
     */
    public final <R> @Nullable R findOne(
            L row,
            @Nullable TreeRowMapClosure<L, T, R> c,
            @Nullable Object x
    ) throws IgniteInternalCheckedException {
        checkDestroyed();

        GetOne<R> g = new GetOne<>(row, c, x, false);

        try {
            doFind(g);

            return g.res;
        } catch (CorruptedDataStructureException e) {
            throw e;
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalCheckedException("Runtime failure on lookup row: " + row, e);
        } catch (RuntimeException | AssertionError e) {
            throw corruptedTreeException("Runtime failure on lookup [row=" + row + "]", e, grpId, g.pageId);
        } finally {
            checkDestroyed();
        }
    }

    @Override
    public final T findOne(L row) throws IgniteInternalCheckedException {
        return findOne(row, null, null);
    }

    /**
     * Searches for the row that (strictly or loosely, depending on {@code includeRow}) follows the lowerBound passed as an argument.
     *
     * @param lowerBound Lower bound.
     * @param includeRow {@code True} if you include the passed row in the result.
     * @return Next row.
     * @throws IgniteInternalCheckedException If failed.
     */
    public final @Nullable T findNext(L lowerBound, boolean includeRow) throws IgniteInternalCheckedException {
        checkDestroyed();

        GetNext g = new GetNext(lowerBound, includeRow);

        try {
            doFind(g);

            return g.nextRow;
        } catch (CorruptedDataStructureException e) {
            throw e;
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalCheckedException("Runtime failure on lookup next row: " + lowerBound, e);
        } catch (RuntimeException | AssertionError e) {
            throw corruptedTreeException("Runtime failure on lookup next row [lower=" + lowerBound + "]", e, grpId, g.pageId);
        } finally {
            checkDestroyed();
        }
    }

    /**
     * Tries to find.
     *
     * @param g Get.
     * @throws IgniteInternalCheckedException If failed.
     */
    private void doFind(Get g) throws IgniteInternalCheckedException {
        assert !sequentialWriteOptsEnabled;

        for (; ; ) { // Go down with retries.
            g.init();

            switch (findDown(g, g.rootId, 0L, g.rootLvl)) {
                case RETRY:
                case RETRY_ROOT:
                    continue;

                default:
                    return;
            }
        }
    }

    private Result findDown(Get g, long pageId, long fwdId, int lvl) throws IgniteInternalCheckedException {
        long page = acquirePage(pageId);

        try {
            for (; ; ) {
                g.checkLockRetry();

                // Init args.
                g.pageId = pageId;
                g.fwdId = fwdId;

                Result res = read(pageId, page, search, g, lvl, RETRY);

                switch (res) {
                    case GO_DOWN:
                    case GO_DOWN_X:
                        assert g.pageId != pageId;
                        assert g.fwdId != fwdId || fwdId == 0;

                        // Go down recursively.
                        res = findDown(g, g.pageId, g.fwdId, lvl - 1);

                        if (res == RETRY) {
                            continue; // The child page got split, need to reread our page.
                        }

                        return res;

                    case NOT_FOUND:
                        assert lvl == 0 : lvl;

                        g.row = null; // Mark not found result.

                        return res;

                    default:
                        return res;
                }
            }
        } finally {
            if (g.canRelease(pageId, lvl)) {
                releasePage(pageId, page);
            }
        }
    }

    /**
     * Returns tree name.
     *
     * @param instance Instance name.
     * @param type Tree type.
     */
    public static String treeName(String instance, String type) {
        return instance + "##" + type;
    }

    /**
     * For debug.
     *
     * @return Tree as {@link String}.
     * @throws IgniteInternalCheckedException If failed.
     */
    @SuppressWarnings("unused")
    public final String printTree() throws IgniteInternalCheckedException {
        long rootPageId;

        long metaPage = acquirePage(metaPageId);
        try {
            rootPageId = getFirstPageId(metaPageId, metaPage, -1);
        } finally {
            releasePage(metaPageId, metaPage);
        }

        return treePrinter.print(rootPageId);
    }

    /**
     * Validates a tree.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    public final void validateTree() throws IgniteInternalCheckedException {
        long rootPageId;
        int rootLvl;

        long metaPage = acquirePage(metaPageId);
        try {
            rootLvl = getRootLevel();

            if (rootLvl < 0) {
                fail("Root level: " + rootLvl);
            }

            validateFirstPages(metaPageId, metaPage, rootLvl);

            rootPageId = getFirstPageId(metaPageId, metaPage, rootLvl);

            validateDownPages(rootPageId, 0L, rootLvl);

            validateDownKeys(rootPageId, null, rootLvl);
        } finally {
            releasePage(metaPageId, metaPage);
        }
    }

    private void validateDownKeys(long pageId, @Nullable L minRow, int lvl) throws IgniteInternalCheckedException {
        long page = acquirePage(pageId);
        try {
            long pageAddr = readLock(pageId, page); // No correctness guaranties.

            try {
                BplusIo<L> io = io(pageAddr);

                int cnt = io.getCount(pageAddr);

                if (cnt < 0) {
                    fail("Negative count: " + cnt);
                }

                if (io.isLeaf()) {
                    for (int i = 0; i < cnt; i++) {
                        if (minRow != null && compare(lvl, io, pageAddr, i, minRow) <= 0) {
                            fail("Wrong sort order: " + hexLong(pageId) + " , at " + i + " , minRow: " + minRow);
                        }

                        minRow = io.getLookupRow(this, pageAddr, i);
                    }

                    return;
                }

                // To find our inner key we have to go left and then always go to the right.
                for (int i = 0; i < cnt; i++) {
                    L row = io.getLookupRow(this, pageAddr, i);

                    if (minRow != null && compare(lvl, io, pageAddr, i, minRow) <= 0) {
                        fail("Min row violated: " + row + " , minRow: " + minRow);
                    }

                    long leftId = inner(io).getLeft(pageAddr, i, partId);

                    L leafRow = getGreatestRowInSubTree(leftId);

                    int cmp = compare(lvl, io, pageAddr, i, leafRow);

                    if (cmp < 0 || (cmp != 0 && canGetRowFromInner)) {
                        fail("Wrong inner row: " + hexLong(pageId) + " , at: " + i + " , leaf:  " + leafRow + " , inner: " + row);
                    }

                    validateDownKeys(leftId, minRow, lvl - 1);

                    minRow = row;
                }

                // Need to handle the rightmost child subtree separately or handle empty routing page.
                long rightId = inner(io).getLeft(pageAddr, cnt, partId); // The same as getRight(cnt - 1)

                validateDownKeys(rightId, minRow, lvl - 1);
            } finally {
                readUnlock(pageId, page, pageAddr);
            }
        } finally {
            releasePage(pageId, page);
        }
    }

    /**
     * Returns Search row.
     *
     * @param pageId Page ID.
     * @throws IgniteInternalCheckedException If failed.
     */
    private L getGreatestRowInSubTree(long pageId) throws IgniteInternalCheckedException {
        long page = acquirePage(pageId);
        try {
            long pageAddr = readLock(pageId, page); // No correctness guaranties.

            try {
                BplusIo<L> io = io(pageAddr);

                int cnt = io.getCount(pageAddr);

                if (io.isLeaf()) {
                    if (cnt <= 0) {
                        // This code is called only if the tree is not empty, so we can't see empty leaf.
                        fail("Invalid leaf count: " + cnt + " " + hexLong(pageId));
                    }

                    return io.getLookupRow(this, pageAddr, cnt - 1);
                }

                // The same as getRight(cnt - 1), but good for routing pages.
                long rightId = inner(io).getLeft(pageAddr, cnt, partId);

                return getGreatestRowInSubTree(rightId);
            } finally {
                readUnlock(pageId, page, pageAddr);
            }
        } finally {
            releasePage(pageId, page);
        }
    }

    private void validateFirstPages(long metaId, long metaPage, int rootLvl) throws IgniteInternalCheckedException {
        for (int lvl = rootLvl; lvl > 0; lvl--) {
            validateFirstPage(metaId, metaPage, lvl);
        }
    }

    /**
     * Throws an {@link AssertionError}.
     *
     * @param msg Message.
     */
    private static void fail(Object msg) {
        throw new AssertionError(msg);
    }

    private void validateFirstPage(long metaId, long metaPage, int lvl) throws IgniteInternalCheckedException {
        if (lvl == 0) {
            fail("Leaf level: " + lvl);
        }

        long pageId = getFirstPageId(metaId, metaPage, lvl);

        long leftmostChildId;

        long page = acquirePage(pageId);
        try {
            long pageAddr = readLock(pageId, page); // No correctness guaranties.

            try {
                BplusIo<L> io = io(pageAddr);

                if (io.isLeaf()) {
                    fail("Leaf.");
                }

                leftmostChildId = inner(io).getLeft(pageAddr, 0, partId);
            } finally {
                readUnlock(pageId, page, pageAddr);
            }
        } finally {
            releasePage(pageId, page);
        }

        long firstDownPageId = getFirstPageId(metaId, metaPage, lvl - 1);

        if (firstDownPageId != leftmostChildId) {
            fail(new IgniteStringBuilder("First: meta ").appendHex(firstDownPageId).app(", child ").appendHex(leftmostChildId));
        }
    }

    private void validateDownPages(long pageId, long fwdId, int lvl) throws IgniteInternalCheckedException {
        long page = acquirePage(pageId);
        try {
            long pageAddr = readLock(pageId, page); // No correctness guaranties.

            try {
                long realPageId = BplusIo.getPageId(pageAddr);

                if (realPageId != pageId) {
                    fail(new IgniteStringBuilder("ABA on page ID: ref ").appendHex(pageId).app(", buf ").appendHex(realPageId));
                }

                BplusIo<L> io = io(pageAddr);

                if (io.isLeaf() != (lvl == 0)) {
                    // Leaf pages only at the level 0.
                    fail("Leaf level mismatch: " + lvl);
                }

                long actualFwdId = io.getForward(pageAddr, partId);

                if (actualFwdId != fwdId) {
                    fail(new IgniteStringBuilder("Triangle: expected fwd ").appendHex(fwdId).app(", actual fwd ").appendHex(actualFwdId));
                }

                int cnt = io.getCount(pageAddr);

                if (cnt < 0) {
                    fail("Negative count: " + cnt);
                }

                if (io.isLeaf()) {
                    if (cnt == 0 && getRootLevel() != 0) {
                        fail("Empty leaf page.");
                    }
                } else {
                    // Recursively go down if we are on inner level.
                    for (int i = 0; i < cnt; i++) {
                        validateDownPages(
                                inner(io).getLeft(pageAddr, i, partId),
                                inner(io).getRight(pageAddr, i, partId),
                                lvl - 1
                        );
                    }

                    if (fwdId != 0) {
                        // For the rightmost child ask neighbor.
                        long fwdId0 = fwdId;
                        long fwdPage = acquirePage(fwdId0);
                        try {
                            long fwdPageAddr = readLock(fwdId0, fwdPage); // No correctness guaranties.

                            try {
                                if (io(fwdPageAddr) != io) {
                                    fail("IO on the same level must be the same");
                                }

                                fwdId = inner(io).getLeft(fwdPageAddr, 0, partId);
                            } finally {
                                readUnlock(fwdId0, fwdPage, fwdPageAddr);
                            }
                        } finally {
                            releasePage(fwdId0, fwdPage);
                        }
                    }

                    // The same as io.getRight(cnt - 1) but works for routing pages.
                    long leftId = inner(io).getLeft(pageAddr, cnt, partId);

                    validateDownPages(leftId, fwdId, lvl - 1);
                }
            } finally {
                readUnlock(pageId, page, pageAddr);
            }
        } finally {
            releasePage(pageId, page);
        }
    }

    /**
     * Makes a string representation of the page.
     *
     * @param io IO.
     * @param pageAddr Page address.
     * @param keys Keys.
     * @return String.
     * @throws IgniteInternalCheckedException If failed.
     */
    private String printPage(BplusIo<L> io, long pageAddr, boolean keys) throws IgniteInternalCheckedException {
        StringBuilder b = new StringBuilder();

        b.append(formatPageId(PageIo.getPageId(pageAddr)))
                .append(" [ ")
                .append(io.isLeaf() ? "L " : "I ");

        int cnt = io.getCount(pageAddr);
        long fwdId = io.getForward(pageAddr, partId);

        b.append("cnt=").append(cnt).append(' ')
                .append("fwd=").append(formatPageId(fwdId)).append(' ');

        if (!io.isLeaf()) {
            b.append("lm=").append(formatPageId(inner(io).getLeft(pageAddr, 0, partId))).append(' ');

            if (cnt > 0) {
                b.append("rm=").append(formatPageId(inner(io).getRight(pageAddr, cnt - 1, partId))).append(' ');
            }
        }

        if (keys) {
            b.append("keys=").append(printPageKeys(io, pageAddr)).append(' ');
        }

        b.append(']');

        return b.toString();
    }

    /**
     * Returns keys as string.
     *
     * @param io IO.
     * @param pageAddr Page address.
     * @throws IgniteInternalCheckedException If failed.
     */
    private String printPageKeys(BplusIo<L> io, long pageAddr) throws IgniteInternalCheckedException {
        int cnt = io.getCount(pageAddr);

        StringBuilder b = new StringBuilder();

        b.append('[');

        for (int i = 0; i < cnt; i++) {
            if (i != 0) {
                b.append(',');
            }

            b.append(io.isLeaf() || canGetRowFromInner ? getRow(io, pageAddr, i) : io.getLookupRow(this, pageAddr, i));
        }

        b.append(']');

        return b.toString();
    }

    /**
     * Formats a page ID into a string.
     *
     * @param x Long.
     * @return String.
     */
    private static String formatPageId(long x) {
        return hexLong(x);
    }

    /**
     * Corrects the index.
     *
     * @param idx Index after binary search, which can be negative.
     * @return Always positive index.
     */
    private static int fix(int idx) {
        assert checkIndex(idx) : idx;

        if (idx < 0) {
            idx = -idx - 1;
        }

        return idx;
    }

    /**
     * Returns {@code true} if correct.
     *
     * @param idx Index.
     */
    private static boolean checkIndex(int idx) {
        return idx > -Short.MAX_VALUE && idx < Short.MAX_VALUE;
    }

    @Override
    public final T remove(L row) throws IgniteInternalCheckedException {
        return doRemove(row, true);
    }

    /**
     * Removes the mapping for a key from this tree if it is present.
     *
     * @param row Lookup row.
     * @return {@code True} if removed row.
     * @throws IgniteInternalCheckedException If failed.
     */
    public final boolean removex(L row) throws IgniteInternalCheckedException {
        Boolean res = (Boolean) doRemove(row, false);

        return res != null ? res : false;
    }

    @Override
    public void invoke(L row, @Nullable Object z, InvokeClosure<T> c) throws IgniteInternalCheckedException {
        checkDestroyed();

        Invoke x = new Invoke(row, z, c);

        try {
            for (; ; ) {
                x.init();

                Result res = invokeDown(x, x.rootId, 0L, 0L, x.rootLvl);

                switch (res) {
                    case RETRY:
                    case RETRY_ROOT:
                        continue;

                    default:
                        if (!x.isFinished()) {
                            res = x.tryFinish();

                            if (res == RETRY || res == RETRY_ROOT) {
                                continue;
                            }

                            assert x.isFinished() : res;
                        }

                        return;
                }
            }
        } catch (CorruptedDataStructureException e) {
            throw e;
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalCheckedException("Runtime failure on invoke [row=" + row + ", op=" + x.op + "]", e);
        } catch (RuntimeException | AssertionError e) {
            throw corruptedTreeException("Runtime failure on invoke [row=" + row + ", op=" + x.op + "]", e, grpId, x.pageId);
        } finally {
            x.releaseAll();
            checkDestroyed();
        }
    }

    private Result invokeDown(
            Invoke x,
            long pageId,
            long backId,
            long fwdId,
            int lvl
    ) throws IgniteInternalCheckedException {
        assert lvl >= 0 : lvl;

        if (x.isTail(pageId, lvl)) {
            return FOUND; // We've already locked this page, so return that we are ok.
        }

        long page = acquirePage(pageId);

        try {
            for (; ; ) {
                x.checkLockRetry();

                // Init args.
                x.pageId(pageId);
                x.fwdId(fwdId);
                x.backId(backId);

                Result readResult = read(pageId, page, search, x, lvl, RETRY);

                switch (readResult) {
                    case GO_DOWN_X:
                        assert backId != 0;
                        assert x.backId == 0; // We did not setup it yet.

                        x.backId(pageId); // Dirty hack to setup a check inside askNeighbor.

                        // We need to get backId here for our child page, it must be the last child of our back.
                        Result askNeighborResult = askNeighbor(backId, x, true);

                        if (askNeighborResult != FOUND) {
                            return askNeighborResult; // Retry.
                        }

                        assert x.backId != pageId; // It must be updated in askNeighbor.

                        // Intentional fallthrough.
                    case GO_DOWN:
                        // Go down recursively.
                        Result invokeDownResult = invokeDown(x, x.pageId, x.backId, x.fwdId, lvl - 1);

                        if (invokeDownResult == RETRY_ROOT || x.isFinished()) {
                            return invokeDownResult;
                        }

                        if (invokeDownResult == RETRY) {
                            continue;
                        }

                        assert x.op != null; // Guarded by isFinished.

                        return x.op.finishOrLockTail(pageId, page, backId, fwdId, lvl);

                    case NOT_FOUND:
                        if (lvl == 0) {
                            x.invokeClosure();
                        }

                        // Level must be equal to bottom level. This is the place when we would insert values into
                        // parent nodes during splits.
                        assert lvl == (x.isPut() ? ((Put) x.op).btmLvl : 0)
                                : "NOT_FOUND on the wrong level  [lvl=" + lvl + ", x=" + x
                                + ", btmLvl=" + (x.isPut() ? ((Put) x.op).btmLvl : 0) + ']';

                        return x.onNotFound(pageId, page, fwdId, lvl);

                    case FOUND:
                        // Item can only be found in the leaf page.
                        assert lvl == 0 : "Invoke found an item in an inner node instead of going down: lvl=" + lvl;

                        x.invokeClosure();

                        return x.onFound(pageId, page, backId, fwdId, lvl);

                    default:
                        return readResult;
                }
            }
        } finally {
            x.levelExit();

            if (x.canRelease(pageId, lvl)) {
                releasePage(pageId, page);
            }
        }
    }

    /**
     * Does a remove.
     *
     * @param row Lookup row.
     * @param needOld {@code True} if need return removed row.
     * @return Removed row.
     * @throws IgniteInternalCheckedException If failed.
     */
    private T doRemove(L row, boolean needOld) throws IgniteInternalCheckedException {
        assert !sequentialWriteOptsEnabled;

        checkDestroyed();

        Remove r = new Remove(row, needOld);

        try {
            for (; ; ) {
                r.init();

                Result res = removeDown(r, r.rootId, 0L, 0L, r.rootLvl);

                switch (res) {
                    case RETRY:
                    case RETRY_ROOT:
                        continue;

                    default:
                        if (!r.isFinished()) {
                            res = r.finishTail();

                            // If not found, then the tree grew beyond our call stack -> retry from the actual root.
                            if (res == RETRY || res == NOT_FOUND) {
                                assert r.checkTailLevel(getRootLevel()) : "tail=" + r.tail + ", res=" + res;

                                continue;
                            }

                            assert res == FOUND : res;
                        }

                        assert r.isFinished();

                        return r.rmvd;
                }
            }
        } catch (CorruptedDataStructureException e) {
            throw e;
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalCheckedException("Runtime failure on remove [row=" + row + ", op=" + r + "]", e);
        } catch (RuntimeException | AssertionError e) {
            throw corruptedTreeException("Runtime failure on remove [row=" + row + ", op=" + r + "]", e, grpId, r.pageId);
        } finally {
            r.releaseAll();
            checkDestroyed();
        }
    }

    private Result removeDown(
            Remove r,
            long pageId,
            long backId,
            long fwdId,
            int lvl
    ) throws IgniteInternalCheckedException {
        assert lvl >= 0 : lvl;

        if (r.isTail(pageId, lvl)) {
            return FOUND; // We've already locked this page, so return that we are ok.
        }

        long page = acquirePage(pageId);

        try {
            for (; ; ) {
                r.checkLockRetry();

                // Init args.
                r.pageId = pageId;
                r.fwdId = fwdId;
                r.backId = backId;

                Result res = read(pageId, page, search, r, lvl, RETRY);

                switch (res) {
                    case GO_DOWN_X:
                        assert backId != 0;
                        assert r.backId == 0; // We did not setup it yet.

                        r.backId = pageId; // Dirty hack to setup a check inside askNeighbor.

                        // We need to get backId here for our child page, it must be the last child of our back.
                        res = askNeighbor(backId, r, true);

                        if (res != FOUND) {
                            return res; // Retry.
                        }

                        assert r.backId != pageId; // It must be updated in askNeighbor.

                        // Intentional fallthrough.
                    case GO_DOWN:
                        res = removeDown(r, r.pageId, r.backId, r.fwdId, lvl - 1);

                        if (res == RETRY) {
                            continue;
                        }

                        if (res == RETRY_ROOT || r.isFinished()) {
                            return res;
                        }

                        res = r.finishOrLockTail(pageId, page, backId, fwdId, lvl);

                        return res;

                    case NOT_FOUND:
                        // We are at the bottom.
                        assert lvl == 0 : lvl;

                        r.finish();

                        return res;

                    case FOUND:
                        return r.tryRemoveFromLeaf(pageId, page, backId, fwdId, lvl);

                    default:
                        return res;
                }
            }
        } finally {
            r.page = 0L;

            if (r.canRelease(pageId, lvl)) {
                releasePage(pageId, page);
            }
        }
    }

    /**
     * Returns {@code true} if may merge.
     *
     * @param cnt Count.
     * @param cap Capacity.
     */
    private boolean mayMerge(int cnt, int cap) {
        int minCnt = (int) (minFill * cap);

        if (cnt <= minCnt) {
            assert cnt == 0; // TODO remove

            return true;
        }

        assert cnt > 0;

        int maxCnt = (int) (maxFill * cap);

        if (cnt > maxCnt) {
            return false;
        }

        assert false; // TODO remove

        // Randomization is for smoothing worst case scenarios. Probability of merge attempt
        // is proportional to free space in our page (discounted on fill factor).
        return randomInt(maxCnt - minCnt) >= cnt - minCnt;
    }

    /**
     * Returns root level.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    public final int rootLevel() throws IgniteInternalCheckedException {
        checkDestroyed();

        return getRootLevel();
    }

    /**
     * Returns {@code true} in case the tree is empty.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    public final boolean isEmpty() throws IgniteInternalCheckedException {
        checkDestroyed();

        for (; ; ) {
            TreeMetaData treeMeta = treeMeta();

            long rootId = treeMeta.rootId;
            long rootPage = acquirePage(rootId);

            try {
                long rootAddr = readLock(rootId, rootPage);

                if (rootAddr == 0) {
                    checkDestroyed();

                    continue;
                }

                try {
                    BplusIo<L> io = io(rootAddr);

                    return io.getCount(rootAddr) == 0;
                } finally {
                    readUnlock(rootId, rootPage, rootAddr);
                }
            } finally {
                releasePage(rootId, rootPage);
            }
        }
    }

    /**
     * Returns number of elements in the tree by scanning pages of the bottom (leaf) level. Since a concurrent access is permitted, there is
     * no guarantee about momentary consistency: the method may miss updates made in already scanned pages.
     *
     * @throws IgniteInternalCheckedException If failed.
     */
    @Override
    public final long size() throws IgniteInternalCheckedException {
        return size(null);
    }

    /**
     * Returns number of elements in the tree that match the filter by scanning through the pages of the leaf level. Since a concurrent
     * access to the tree is permitted, there is no guarantee about momentary consistency: the method may not see updates made in already
     * scanned pages.
     *
     * @param filter The filter to use or null to count all elements.
     * @return Number of either all elements in the tree or the elements that match the filter.
     * @throws IgniteInternalCheckedException If failed.
     */
    public long size(@Nullable TreeRowClosure<L, T> filter) throws IgniteInternalCheckedException {
        checkDestroyed();

        for (; ; ) {
            long curPageId;

            long metaPage = acquirePage(metaPageId);

            try {
                curPageId = getFirstPageId(metaPageId, metaPage, 0); // Level 0 is always at the bottom.
            } finally {
                releasePage(metaPageId, metaPage);
            }

            long cnt = 0;

            long curPage = acquirePage(curPageId);

            try {
                long curPageAddr = readLock(curPageId, curPage);

                if (curPageAddr == 0) {
                    continue; // The first page has gone: restart scan.
                }

                try {
                    BplusIo<L> io = io(curPageAddr);

                    assert io.isLeaf();

                    for (; ; ) {
                        int curPageSize = io.getCount(curPageAddr);

                        if (filter == null) {
                            cnt += curPageSize;
                        } else {
                            for (int i = 0; i < curPageSize; ++i) {
                                if (filter.apply(this, io, curPageAddr, i)) {
                                    cnt++;
                                }
                            }
                        }

                        long nextPageId = io.getForward(curPageAddr, partId);

                        if (nextPageId == 0) {
                            checkDestroyed();

                            return cnt;
                        }

                        long nextPage = acquirePage(nextPageId);

                        try {
                            long nextPageAddr = readLock(nextPageId, nextPage);

                            // In the current implementation the next page can't change when the current page is locked.
                            assert nextPageAddr != 0 : nextPageAddr;

                            try {
                                long pa = curPageAddr;
                                curPageAddr = 0; // Set to zero to avoid double unlocking in finalizer.

                                readUnlock(curPageId, curPage, pa);

                                long p = curPage;
                                curPage = 0; // Set to zero to avoid double release in finalizer.

                                releasePage(curPageId, p);

                                curPageId = nextPageId;
                                curPage = nextPage;
                                curPageAddr = nextPageAddr;

                                nextPage = 0;
                                nextPageAddr = 0;
                            } finally {
                                if (nextPageAddr != 0) {
                                    readUnlock(nextPageId, nextPage, nextPageAddr);
                                }
                            }
                        } finally {
                            if (nextPage != 0) {
                                releasePage(nextPageId, nextPage);
                            }
                        }
                    }
                } finally {
                    if (curPageAddr != 0) {
                        readUnlock(curPageId, curPage, curPageAddr);
                    }
                }
            } finally {
                if (curPage != 0) {
                    releasePage(curPageId, curPage);
                }
            }
        }
    }

    @Override
    public final T put(T row) throws IgniteInternalCheckedException {
        return doPut(row, true);
    }

    /**
     * Does a put.
     *
     * @param row New value.
     * @return {@code True} if replaced existing row.
     * @throws IgniteInternalCheckedException If failed.
     */
    public boolean putx(T row) throws IgniteInternalCheckedException {
        Boolean res = (Boolean) doPut(row, false);

        return res != null ? res : false;
    }

    /**
     * Does a put.
     *
     * @param row New value.
     * @param needOld {@code True} If need return old value.
     * @return Old row.
     * @throws IgniteInternalCheckedException If failed.
     */
    private T doPut(T row, boolean needOld) throws IgniteInternalCheckedException {
        checkDestroyed();

        Put p = new Put(row, needOld);

        try {
            for (; ; ) { // Go down with retries.
                p.init();

                Result res = putDown(p, p.rootId, 0L, p.rootLvl);

                switch (res) {
                    case RETRY:
                    case RETRY_ROOT:
                        continue;

                    case FOUND:
                        // We may need to perform an inner replace on the upper level.
                        if (!p.isFinished()) {
                            res = p.finishTail();

                            // If not found, then the root split has happened and operation should be retried from the actual root.
                            if (res == RETRY || res == NOT_FOUND) {
                                p.releaseTail();

                                assert p.checkTailLevel(getRootLevel()) : "tail=" + p.tail + ", res=" + res;

                                continue;
                            }
                        }

                        return p.oldRow;

                    default:
                        throw new IllegalStateException("Result: " + res);
                }
            }
        } catch (CorruptedDataStructureException e) {
            throw e;
        } catch (IgniteInternalCheckedException e) {
            throw new IgniteInternalCheckedException("Runtime failure on put [row=" + row + ", op=" + p + "]", e);
        } catch (RuntimeException | AssertionError e) {
            throw corruptedTreeException("Runtime failure on put [row=" + row + ", op=" + p + "]", e, grpId, p.pageId);
        } finally {
            checkDestroyed();
        }
    }

    /**
     * Releases the lock that is held by long tree destroy process for a short period of time and acquires it again, allowing other
     * processes to acquire it.
     */
    protected void temporaryReleaseLock() {
        // No-op.
    }

    /**
     * Releases the lock that is held by long tree destroy process for a short period of time and acquires it again, allowing other
     * processes to acquire it.
     *
     * @param lockedPages Deque of locked pages. {@link IgniteTuple3} contains page id, page pointer and page address. Pages are ordered in
     *      that order as they were locked by destroy method. We unlock them in reverse order and unlock in direct order.
     */
    private void temporaryReleaseLock(Deque<IgniteTuple3<Long, Long, Long>> lockedPages) {
        lockedPages.iterator().forEachRemaining(t -> writeUnlock(t.get1(), t.get2(), t.get3(), true));

        temporaryReleaseLock();

        lockedPages.descendingIterator().forEachRemaining(t -> writeLock(t.get1(), t.get2()));
    }

    /**
     * Maximum time for which tree destroy process is allowed to hold the lock, after this time exceeds, {@link
     * BplusTree#temporaryReleaseLock()} is called and hold time is reset.
     *
     * @return Time, in milliseconds.
     */
    protected long maxLockHoldTime() {
        return Long.MAX_VALUE;
    }

    /**
     * Destroys tree. This method is allowed to be invoked only when the tree is out of use (no concurrent operations are trying to read or
     * update the tree after destroy beginning).
     *
     * @return Number of pages recycled from this tree. If the tree was destroyed by someone else concurrently returns {@code 0}, otherwise
     *      it should return at least {@code 2} (for meta page and root page), unless this tree is used as metadata storage, or {@code -1}
     *      if we don't have a reuse list and did not do recycling at all.
     * @throws IgniteInternalCheckedException If failed.
     */
    public final long destroy() throws IgniteInternalCheckedException {
        return destroy(null, false);
    }

    /**
     * Destroys tree. This method is allowed to be invoked only when the tree is out of use (no concurrent operations are trying to read or
     * update the tree after destroy beginning).
     *
     * @param c Visitor closure. Visits only leaf pages.
     * @param forceDestroy Whether to proceed with destroying, even if tree is already marked as destroyed (see {@link #markDestroyed()}).
     * @return Number of pages recycled from this tree. If the tree was destroyed by someone else concurrently returns {@code 0}, otherwise
     *      it should return at least {@code 2} (for meta page and root page), unless this tree is used as metadata storage, or {@code -1}
     *      if we don't have a reuse list and did not do recycling at all.
     * @throws IgniteInternalCheckedException If failed.
     */
    public final long destroy(@Nullable Consumer<L> c, boolean forceDestroy) throws IgniteInternalCheckedException {
        close();

        if (!markDestroyed() && !forceDestroy) {
            return 0;
        }

        if (reuseList == null) {
            return -1;
        }

        LongListReuseBag bag = new LongListReuseBag();

        long pagesCnt = 0;

        AtomicLong lockHoldStartTime = new AtomicLong(FastTimestamps.coarseCurrentTimeMillis());

        Deque<IgniteTuple3<Long, Long, Long>> lockedPages = new LinkedList<>();

        long lockMaxTime = maxLockHoldTime();

        long metaPage = acquirePage(metaPageId);

        try {
            long metaPageAddr = writeLock(metaPageId, metaPage); // No checks, we must be out of use.

            lockedPages.push(new IgniteTuple3<>(metaPageId, metaPage, metaPageAddr));

            try {
                assert metaPageAddr != 0L;

                int rootLvl = getRootLevel(metaPageAddr);

                if (rootLvl < 0) {
                    fail("Root level: " + rootLvl);
                }

                long rootPageId = getFirstPageId(metaPageId, metaPage, rootLvl, metaPageAddr);

                pagesCnt += destroyDownPages(bag, rootPageId, rootLvl, c, lockHoldStartTime, lockMaxTime, lockedPages);

                bag.addFreePage(recyclePage(metaPageId, metaPageAddr));

                pagesCnt++;
            } finally {
                writeUnlock(metaPageId, metaPage, metaPageAddr, true);

                lockedPages.pop();
            }
        } finally {
            releasePage(metaPageId, metaPage);
        }

        addForRecycle(bag);

        return pagesCnt;
    }

    private void addForRecycle(LongListReuseBag bag) throws IgniteInternalCheckedException {
        reuseList.addForRecycle(bag);

        assert bag.isEmpty() : bag.size();
    }

    /**
     * Recursively destroys tree pages. Should be initially called with id of root page as {@code pageId} and root level as {@code lvl}.
     *
     * @param bag Reuse bag.
     * @param pageId Page id.
     * @param lvl Current level of tree.
     * @param c Visitor consumer. Visits only leaf pages.
     * @param lockHoldStartTime When lock has been aquired last time.
     * @param lockMaxTime Maximum time to hold the lock.
     * @param lockedPages Deque of locked pages. Is used to release write-locked pages when temporary releasing checkpoint read lock.
     * @return Count of destroyed pages.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected long destroyDownPages(
            LongListReuseBag bag,
            long pageId,
            int lvl,
            @Nullable Consumer<L> c,
            AtomicLong lockHoldStartTime,
            long lockMaxTime,
            Deque<IgniteTuple3<Long, Long, Long>> lockedPages
    ) throws IgniteInternalCheckedException {
        if (pageId == 0) {
            return 0;
        }

        long pagesCnt = 0;

        long page = acquirePage(pageId);

        try {
            long pageAddr = writeLock(pageId, page);

            if (pageAddr == 0L) {
                return 0; // This page was possibly recycled, but we still need to destroy the rest of the tree.
            }

            lockedPages.push(new IgniteTuple3<>(pageId, page, pageAddr));

            try {
                BplusIo<L> io = io(pageAddr);

                if (io.isLeaf() != (lvl == 0)) {
                    // Leaf pages only at the level 0.
                    fail("Leaf level mismatch: " + lvl);
                }

                int cnt = io.getCount(pageAddr);

                if (cnt < 0) {
                    fail("Negative count: " + cnt);
                }

                if (!io.isLeaf()) {
                    // Recursively go down if we are on inner level.
                    // When i == cnt it is the same as io.getRight(cnt - 1) but works for routing pages.
                    for (int i = 0; i <= cnt; i++) {
                        long leftId = inner(io).getLeft(pageAddr, i, partId);

                        inner(io).setLeft(pageAddr, i, 0);

                        pagesCnt += destroyDownPages(
                                bag,
                                leftId,
                                lvl - 1,
                                c,
                                lockHoldStartTime,
                                lockMaxTime,
                                lockedPages
                        );
                    }
                }

                if (c != null && io.isLeaf()) {
                    io.visit(this, pageAddr, c);
                }

                bag.addFreePage(recyclePage(pageId, pageAddr));

                pagesCnt++;
            } finally {
                writeUnlock(pageId, page, pageAddr, true);

                lockedPages.pop();
            }

            if (FastTimestamps.coarseCurrentTimeMillis() - lockHoldStartTime.get() > lockMaxTime) {
                temporaryReleaseLock(lockedPages);

                lockHoldStartTime.set(FastTimestamps.coarseCurrentTimeMillis());
            }
        } finally {
            releasePage(pageId, page);
        }

        if (bag.size() == 128) {
            addForRecycle(bag);
        }

        return pagesCnt;
    }

    /**
     * Starts gradual destruction, that is, closes the tree, recycles its meta page, and returns a {@link GradualTask}
     * that, when executed by a {@link org.apache.ignite.internal.pagememory.util.GradualTaskExecutor}, gradually destroys
     * the tree.
     *
     * <p>This method is allowed to be invoked only when the tree is out of use (no concurrent operations are trying to read or
     * update the tree after destroy beginning).
     *
     * @param c Visitor closure. Visits only leaf pages.
     * @param forceDestroy Whether to proceed with destroying, even if tree is already marked as destroyed (see {@link #markDestroyed()}).
     * @param maxWorkUnits Maximum amount of allowed "work units" per every {@link GradualTask} step. Recycling of a node counts as 1
     *     work unit; also, visiting an item using a Consumer also counts as 1 work unit per item.
     * @return GradualTask that will destroy the tree; it is the responsibility of a caller to pass this task for
     *     execution to a {@link org.apache.ignite.internal.pagememory.util.GradualTaskExecutor}.
     * @throws IgniteInternalCheckedException If failed.
     */
    public final GradualTask startGradualDestruction(
            @Nullable Consumer<L> c, boolean forceDestroy, int maxWorkUnits
    ) throws IgniteInternalCheckedException {
        close();

        if (!markDestroyed() && !forceDestroy) {
            return GradualTask.completed();
        }

        if (reuseList == null) {
            return GradualTask.completed();
        }

        LongListReuseBag bag = new LongListReuseBag();

        RootPageIdAndLevel rootPageIdAndLevel = detachMetaPage(bag);

        return new DestroyTreeTask(bag, c, rootPageIdAndLevel.level, rootPageIdAndLevel.pageId, maxWorkUnits);
    }

    private RootPageIdAndLevel detachMetaPage(LongListReuseBag bag) throws IgniteInternalCheckedException {
        long metaPage = acquirePage(metaPageId);

        try {
            long metaPageAddr = writeLock(metaPageId, metaPage); // No checks, we must be out of use.

            try {
                assert metaPageAddr != 0L;

                int rootLvl = getRootLevel(metaPageAddr);

                if (rootLvl < 0) {
                    fail("Root level: " + rootLvl);
                }

                long rootPageId = getFirstPageId(metaPageId, metaPage, rootLvl, metaPageAddr);

                bag.addFreePage(recyclePage(metaPageId, metaPageAddr));

                return new RootPageIdAndLevel(rootPageId, rootLvl);
            } finally {
                writeUnlock(metaPageId, metaPage, metaPageAddr, true);
            }
        } finally {
            releasePage(metaPageId, metaPage);
        }
    }

    /**
     * Marks the tree as destroyed.
     *
     * @return {@code True} if state was changed.
     */
    public boolean markDestroyed() {
        return destroyed.compareAndSet(false, true);
    }

    /**
     * Returns {@code true} if marked as destroyed.
     */
    public boolean destroyed() {
        return destroyed.get();
    }

    /**
     * Returns first page IDs.
     *
     * @param pageAddr Meta page address.
     */
    protected Iterable<Long> getFirstPageIds(long pageAddr) {
        List<Long> res = new ArrayList<>();

        BplusMetaIo mio = metaIos.forPage(pageAddr);

        for (int lvl = mio.getRootLevel(pageAddr); lvl >= 0; lvl--) {
            res.add(mio.getFirstPageId(pageAddr, lvl, partId));
        }

        return res;
    }

    /**
     * Returns {@code true} the middle index was shifted to the right.
     *
     * @param pageAddr Page address
     * @param io IO.
     * @param fwdId Forward page ID.
     * @param fwdBuf Forward buffer.
     * @param idx Insertion index.
     * @throws IgniteInternalCheckedException If failed.
     */
    private boolean splitPage(long pageAddr, BplusIo<L> io, long fwdId, long fwdBuf, int idx) throws IgniteInternalCheckedException {
        int cnt = io.getCount(pageAddr);

        int mid = sequentialWriteOptsEnabled ? (int) (cnt * 0.85) : cnt >>> 1;

        boolean res = false;

        if (idx > mid) { // If insertion is going to be to the forward page, keep more in the back page.
            mid++;

            res = true;
        }

        // Update forward page.
        io.splitForwardPage(pageAddr, fwdId, fwdBuf, mid, cnt, pageSize(), partId);

        // Update existing page.
        io.splitExistingPage(pageAddr, mid, fwdId);

        return res;
    }

    private void writeUnlockAndClose(long pageId, long page, long pageAddr) {
        try {
            writeUnlock(pageId, page, pageAddr, true);
        } finally {
            releasePage(pageId, page);
        }
    }

    /**
     * Asking neighbors.
     *
     * @param pageId Inner page ID.
     * @param g Get.
     * @param back Get back (if {@code true}) or forward page (if {@code false}).
     * @return Operation result.
     * @throws IgniteInternalCheckedException If failed.
     */
    private Result askNeighbor(long pageId, Get g, boolean back) throws IgniteInternalCheckedException {
        return read(pageId, askNeighbor, g, back ? TRUE.ordinal() : FALSE.ordinal(), RETRY);
    }

    private Result putDown(Put p, long pageId, long fwdId, int lvl) throws IgniteInternalCheckedException {
        assert lvl >= 0 : lvl;

        long page = acquirePage(pageId);

        try {
            for (; ; ) {
                p.checkLockRetry();

                // Init args.
                p.pageId = pageId;
                p.fwdId = fwdId;

                Result res = read(pageId, page, search, p, lvl, RETRY);

                switch (res) {
                    case GO_DOWN:
                    case GO_DOWN_X:
                        assert lvl > 0 : lvl;
                        assert p.pageId != pageId;
                        assert p.fwdId != fwdId || fwdId == 0;

                        // Go down recursively.
                        res = putDown(p, p.pageId, p.fwdId, lvl - 1);

                        if (res == RETRY_ROOT || p.isFinished()) {
                            return res;
                        }

                        if (res == RETRY) {
                            continue;
                        }

                        // We have to either insert split row to this level,
                        // perform inner replace, lock the tail or retry.
                        res = p.finishOrLockTail(pageId, page, 0L, fwdId, lvl);

                        return res;

                    case FOUND: // Do replace.
                        assert lvl == 0 : "This replace can happen only at the bottom level.";

                        return p.tryReplace(pageId, page, fwdId, lvl);

                    case NOT_FOUND: // Do insert.
                        assert lvl == p.btmLvl : "must insert at the bottom level";

                        return p.tryInsert(pageId, page, fwdId, lvl);

                    default:
                        return res;
                }
            }
        } finally {
            if (p.canRelease(pageId, lvl)) {
                releasePage(pageId, page);
            }
        }
    }

    private void doVisit(TreeVisitor c) throws IgniteInternalCheckedException {
        for (; ; ) { // Go down with retries.
            c.init();

            switch (visitDown(c, c.rootId, 0L, c.rootLvl)) {
                case RETRY:
                case RETRY_ROOT:
                    continue;

                default:
                    return;
            }
        }
    }

    private Result visitDown(
            TreeVisitor v,
            long pageId,
            long fwdId,
            int lvl
    ) throws IgniteInternalCheckedException {
        long page = acquirePage(pageId);

        try {
            for (; ; ) {
                v.checkLockRetry();

                // Init args.
                v.pageId = pageId;
                v.fwdId = fwdId;

                Result res = read(pageId, page, search, v, lvl, RETRY);

                switch (res) {
                    case GO_DOWN:
                    case GO_DOWN_X:
                        assert v.pageId != pageId;
                        assert v.fwdId != fwdId || fwdId == 0;

                        // Go down recursively.
                        res = visitDown(v, v.pageId, v.fwdId, lvl - 1);

                        if (res == RETRY) {
                            continue; // The child page got split, need to reread our page.
                        }

                        return res;

                    case NOT_FOUND:
                        assert lvl == 0 : lvl;

                        return v.init(pageId, page, fwdId);

                    case FOUND:
                        throw new IllegalStateException(); // Must never be called because we always have a shift.

                    default:
                        return res;
                }
            }
        } finally {
            if (v.canRelease(pageId, lvl)) {
                releasePage(pageId, page);
            }
        }
    }

    /**
     * Asking neighbors.
     *
     * @param io IO.
     * @param pageAddr Page address.
     * @param back Backward page.
     * @return Page ID.
     */
    private long doAskNeighbor(BplusIo<L> io, long pageAddr, boolean back) {
        long res;

        if (back) {
            // Count can be 0 here if it is a routing page, in this case we have a single child.
            int cnt = io.getCount(pageAddr);

            // We need to do get the rightmost child: io.getRight(cnt - 1),
            // here io.getLeft(cnt) is the same, but handles negative index if count is 0.
            res = inner(io).getLeft(pageAddr, cnt, partId);
        } else {
            // Leftmost child.
            res = inner(io).getLeft(pageAddr, 0, partId);
        }

        assert res != 0 : "inner page with no route down: " + hexLong(PageIo.getPageId(pageAddr));

        return res;
    }

    @Override
    public String toString() {
        return S.toString(BplusTree.class, this);
    }

    /**
     * Get operation.
     */
    public abstract class Get {
        long rmvId;

        /** Starting point root level. May be outdated. Must be modified only in {@link Get#init()}. */
        int rootLvl;

        /** Starting point root ID. May be outdated. Must be modified only in {@link Get#init()}. */
        long rootId;

        @Nullable L row;

        /** In/Out parameter: Page ID. */
        long pageId;

        /** In/Out parameter: expected forward page ID. */
        long fwdId;

        /** In/Out parameter: in case of right turn this field will contain backward page ID for the child. */
        long backId;

        int shift;

        /** If this operation is a part of invoke. */
        Invoke invoke;

        /** Ignore row passed, find last row. */
        boolean findLast;

        /** Number of repetitions to capture a lock in the B+Tree (countdown). */
        int lockRetriesCnt = getLockRetries();

        /**
         * Constructor.
         *
         * @param row Row.
         * @param findLast find last row.
         */
        Get(@Nullable L row, boolean findLast) {
            assert findLast ^ row != null;

            this.row = row;
            this.findLast = findLast;
        }

        /**
         * Copies an operation.
         *
         * @param g Other operation to copy from.
         */
        final void copyFrom(Get g) {
            rmvId = g.rmvId;
            rootLvl = g.rootLvl;
            pageId = g.pageId;
            fwdId = g.fwdId;
            backId = g.backId;
            shift = g.shift;
            findLast = g.findLast;
        }

        final void init() throws IgniteInternalCheckedException {
            TreeMetaData meta0 = treeMeta();

            assert meta0 != null;

            restartFromRoot(meta0.rootId, meta0.rootLvl, globalRmvId.get());
        }

        /**
         * Restarts from root.
         *
         * @param rootId Root page ID.
         * @param rootLvl Root level.
         * @param rmvId Remove ID to be afraid of.
         */
        void restartFromRoot(long rootId, int rootLvl, long rmvId) {
            this.rootId = rootId;
            this.rootLvl = rootLvl;
            this.rmvId = rmvId;
        }

        /**
         * Returns {@code true} if we need to stop.
         *
         * @param io IO.
         * @param pageAddr Page address.
         * @param idx Index of found entry.
         * @param lvl Level.
         * @throws IgniteInternalCheckedException If failed.
         */
        boolean found(BplusIo<L> io, long pageAddr, int idx, int lvl) throws IgniteInternalCheckedException {
            assert lvl >= 0;

            return lvl == 0; // Stop if we are at the bottom.
        }

        /**
         * Returns {@code true} if we need to stop.
         *
         * @param io IO.
         * @param pageAddr Page address.
         * @param idx Insertion point.
         * @param lvl Level.
         * @throws IgniteInternalCheckedException If failed.
         */
        boolean notFound(BplusIo<L> io, long pageAddr, int idx, int lvl) throws IgniteInternalCheckedException {
            assert lvl >= 0;

            return lvl == 0; // Stop if we are at the bottom.
        }

        /**
         * Returns {@code true} If we can release the given page.
         *
         * @param pageId Page.
         * @param lvl Level.
         */
        public boolean canRelease(long pageId, int lvl) {
            return pageId != 0L;
        }

        /**
         * Sets back page ID.
         *
         * @param backId Back page ID.
         */
        void backId(long backId) {
            this.backId = backId;
        }

        /**
         * Sets page ID.
         *
         * @param pageId Page ID.
         */
        void pageId(long pageId) {
            this.pageId = pageId;
        }

        /**
         * Sets forward page ID.
         *
         * @param fwdId Forward page ID.
         */
        void fwdId(long fwdId) {
            this.fwdId = fwdId;
        }

        /**
         * Returns {@code true} if the operation is finished.
         */
        boolean isFinished() {
            throw new IllegalStateException();
        }

        /**
         * Checks that there has not been an excess of attempts to get the lock.
         *
         * @throws IgniteInternalCheckedException If the operation can not be retried.
         */
        void checkLockRetry() throws IgniteInternalCheckedException {
            if (lockRetriesCnt == 0) {
                String errMsg = lockRetryErrorMessage(getClass().getSimpleName());

                throw new IgniteInternalCheckedException(errMsg);
            }

            lockRetriesCnt--;
        }

        /**
         * Returns operation row.
         */
        public L row() {
            return row;
        }
    }

    /**
     * Get a single entry.
     */
    private final class GetOne<R> extends Get {
        private final @Nullable Object arg;

        private final @Nullable TreeRowMapClosure<L, T, R> treeRowClosure;

        private @Nullable R res;

        /**
         * Constructor.
         *
         * @param row Row.
         * @param treeRowClosure Tree row closure, if the tree row is not found, then {@code null} will be passed to the
         *      {@link TreeRowMapClosure#map}.
         * @param arg Implementation specific argument.
         * @param findLast Ignore row passed, find last row
         */
        private GetOne(
                @Nullable L row,
                @Nullable TreeRowMapClosure<L, T, R> treeRowClosure,
                @Nullable Object arg,
                boolean findLast
        ) {
            super(row, findLast);

            this.treeRowClosure = treeRowClosure;
            this.arg = arg;
        }

        @Override
        boolean found(BplusIo<L> io, long pageAddr, int idx, int lvl) throws IgniteInternalCheckedException {
            // Check if we are on an inner page and can't get row from it.
            if (lvl != 0 && !canGetRowFromInner) {
                return false;
            }

            if (treeRowClosure == null || treeRowClosure.apply(BplusTree.this, io, pageAddr, idx)) {
                T treeRow = getRow(io, pageAddr, idx, arg);

                res = treeRowClosure != null ? treeRowClosure.map(treeRow) : (R) treeRow;
            }

            return true;
        }

        @Override
        boolean notFound(BplusIo<L> io, long pageAddr, int idx, int lvl) {
            assert lvl >= 0 : lvl;

            if (lvl == 0) {
                res = treeRowClosure == null ? null : treeRowClosure.map(null);

                return true;
            }

            return false;
        }
    }

    /**
     * Get a cursor for range.
     */
    private final class GetCursor extends Get {
        /** Cursor. */
        AbstractForwardCursor cursor;

        /**
         * Constructor.
         *
         * @param lower Lower bound.
         * @param shift Shift.
         * @param cursor Cursor.
         */
        GetCursor(L lower, int shift, AbstractForwardCursor cursor) {
            super(lower, false);

            assert shift != 0; // Either handle range of equal rows or find a greater row after concurrent merge.

            this.shift = shift;
            this.cursor = cursor;
        }

        @Override
        boolean found(BplusIo<L> io, long pageAddr, int idx, int lvl) {
            throw new IllegalStateException(); // Must never be called because we always have a shift.
        }

        @Override
        boolean notFound(BplusIo<L> io, long pageAddr, int idx, int lvl) throws IgniteInternalCheckedException {
            if (lvl != 0) {
                return false;
            }

            cursor.init(pageAddr, io, idx);

            return true;
        }
    }

    /**
     * Get a cursor for range.
     */
    private final class TreeVisitor extends Get {
        long nextPageId;

        L upper;

        TreeVisitorClosure<L, T> pred;

        private boolean dirty;

        private boolean writing;

        TreeVisitor(L lower, L upper, TreeVisitorClosure<L, T> pred) {
            super(lower, false);

            this.shift = -1;
            this.upper = upper;
            this.pred = pred;
        }

        @Override
        boolean found(BplusIo<L> io, long pageAddr, int idx, int lvl) {
            throw new IllegalStateException(); // Must never be called because we always have a shift.
        }

        @Override
        boolean notFound(BplusIo<L> io, long pageAddr, int idx, int lvl) throws IgniteInternalCheckedException {
            if (lvl != 0) {
                return false;
            }

            writing = (pred.state() & TreeVisitorClosure.CAN_WRITE) != 0;

            if (!writing) {
                init(pageAddr, io, idx);
            }

            return true;
        }

        Result init(long pageId, long page, long fwdId) throws IgniteInternalCheckedException {
            // Init args.
            this.pageId = pageId;
            this.fwdId = fwdId;

            if (writing) {
                long pageAddr = writeLock(pageId, page);

                if (pageAddr == 0) {
                    return RETRY;
                }

                try {
                    BplusIo<L> io = io(pageAddr);

                    // Check triangle invariant.
                    if (io.getForward(pageAddr, partId) != fwdId) {
                        return RETRY;
                    }

                    init(pageAddr, io, -1);
                } finally {
                    unlock(pageId, page, pageAddr);
                }
            }

            return NOT_FOUND;
        }

        private void init(long pageAddr, BplusIo<L> io, int startIdx) throws IgniteInternalCheckedException {
            nextPageId = 0;

            int cnt = io.getCount(pageAddr);

            if (cnt != 0) {
                visit(pageAddr, io, startIdx, cnt);
            }
        }

        /**
         * Does a visit.
         *
         * @param pageAddr Page address.
         * @param io IO.
         * @param startIdx Start index.
         * @param cnt Number of rows in the buffer.
         * @throws IgniteInternalCheckedException If failed.
         */
        private void visit(long pageAddr, BplusIo<L> io, int startIdx, int cnt) throws IgniteInternalCheckedException {
            assert io.isLeaf() : io;
            assert cnt != 0 : cnt; // We can not see empty pages (empty tree handled in init).
            assert startIdx >= -1 : startIdx;
            assert cnt >= startIdx;

            checkDestroyed();

            nextPageId = io.getForward(pageAddr, partId);

            if (startIdx == -1) {
                startIdx = findLowerBound(pageAddr, io, cnt);
            }

            if (cnt == startIdx) {
                return; // Go to the next page;
            }

            cnt = findUpperBound(pageAddr, io, startIdx, cnt);

            for (int i = startIdx; i < cnt; i++) {
                int state = pred.visit(BplusTree.this, io, pageAddr, i);

                boolean stop = (state & TreeVisitorClosure.STOP) != 0;

                if (writing) {
                    dirty = dirty || (state & TreeVisitorClosure.DIRTY) != 0;
                }

                if (stop) {
                    nextPageId = 0; // The End.

                    return;
                }
            }

            if (nextPageId != 0) {
                row = io.getLookupRow(BplusTree.this, pageAddr, cnt - 1); // Need save last row.

                shift = 1;
            }
        }

        private void visit() throws IgniteInternalCheckedException {
            doVisit(this);

            while (nextPageId != 0) {
                nextPage();
            }
        }

        /**
         * Returns adjusted to lower bound start index.
         *
         * @param pageAddr Page address.
         * @param io IO.
         * @param cnt Count.
         * @throws IgniteInternalCheckedException If failed.
         */
        private int findLowerBound(long pageAddr, BplusIo<L> io, int cnt) throws IgniteInternalCheckedException {
            assert io.isLeaf();

            // Compare with the first row on the page.
            int cmp = compare(0, io, pageAddr, 0, row);

            if (cmp < 0 || (cmp == 0 && shift == 1)) {
                int idx = findInsertionPoint(0, io, pageAddr, 0, cnt, row, shift);

                assert idx < 0;

                return fix(idx);
            }

            return 0;
        }

        /**
         * Returns corrected number of rows with respect to upper bound.
         *
         * @param pageAddr Page address.
         * @param io IO.
         * @param low Start index.
         * @param cnt Number of rows in the buffer.
         * @throws IgniteInternalCheckedException If failed.
         */
        private int findUpperBound(long pageAddr, BplusIo<L> io, int low, int cnt) throws IgniteInternalCheckedException {
            assert io.isLeaf();

            // Compare with the last row on the page.
            int cmp = compare(0, io, pageAddr, cnt - 1, upper);

            if (cmp > 0) {
                int idx = findInsertionPoint(0, io, pageAddr, low, cnt, upper, 1);

                assert idx < 0;

                cnt = fix(idx);

                nextPageId = 0; // The End.
            }

            return cnt;
        }

        private void nextPage() throws IgniteInternalCheckedException {
            for (; ; ) {
                if (nextPageId == 0) {
                    return;
                }

                long pageId = nextPageId;
                long page = acquirePage(pageId);
                try {
                    long pageAddr = lock(pageId, page); // Doing explicit null check.

                    // If concurrent merge occurred we have to reinitialize cursor from the last returned row.
                    if (pageAddr == 0L) {
                        break;
                    }

                    try {
                        BplusIo<L> io = io(pageAddr);

                        visit(pageAddr, io, -1, io.getCount(pageAddr));
                    } finally {
                        unlock(pageId, page, pageAddr);
                    }
                } finally {
                    releasePage(pageId, page);
                }
            }

            doVisit(this); // restart from last read row
        }

        private void unlock(long pageId, long page, long pageAddr) {
            if (writing) {
                writeUnlock(pageId, page, pageAddr, dirty);

                dirty = false; // reset dirty flag
            } else {
                readUnlock(pageId, page, pageAddr);
            }
        }

        private long lock(long pageId, long page) {
            writing = (pred.state() & TreeVisitorClosure.CAN_WRITE) != 0;

            if (writing) {
                return writeLock(pageId, page);
            } else {
                return readLock(pageId, page);
            }
        }
    }

    /**
     * Get the last item in the tree which matches the passed filter.
     */
    private final class GetLast extends Get {
        private final TreeRowClosure<L, T> filter;

        private boolean retry = true;

        private long lastPageId;

        private T row0;

        /**
         * Constructor.
         *
         * @param filter Filter closure.
         */
        public GetLast(TreeRowClosure<L, T> filter) {
            super(null, true);

            assert filter != null;

            this.filter = filter;
        }

        @Override
        boolean found(BplusIo<L> io, long pageAddr, int idx, int lvl) throws IgniteInternalCheckedException {
            if (lvl != 0) {
                return false;
            }

            for (int i = idx; i >= 0; i--) {
                if (filter.apply(BplusTree.this, io, pageAddr, i)) {
                    retry = false;
                    row0 = getRow(io, pageAddr, i);

                    return true;
                }
            }

            if (pageId == rootId) {
                retry = false; // We are at the root page, there are no other leafs.
            }

            if (retry) {
                findLast = false;

                // Restart from an item before the first item in the leaf (last item on the previous leaf).
                row0 = getRow(io, pageAddr, 0);
                shift = -1;

                lastPageId = pageId; // Track leafs to detect a loop over the first leaf in the tree.
            }

            return true;
        }

        @Override
        boolean notFound(BplusIo<L> io, long pageAddr, int idx, int lvl) throws IgniteInternalCheckedException {
            if (lvl != 0) {
                return false;
            }

            if (io.getCount(pageAddr) == 0) {
                // it's an empty tree
                retry = false;

                return true;
            }

            if (idx == 0 && lastPageId == pageId) {
                // not found
                retry = false;
                row0 = null;

                return true;
            } else {
                for (int i = idx; i >= 0; i--) {
                    if (filter.apply(BplusTree.this, io, pageAddr, i)) {
                        retry = false;
                        row0 = getRow(io, pageAddr, i);

                        break;
                    }
                }
            }

            if (retry) {
                // Restart from an item before the first item in the leaf (last item on the previous leaf).
                row0 = getRow(io, pageAddr, 0);

                lastPageId = pageId; // Track leafs to detect a loop over the first leaf in the tree.
            }

            return true;
        }

        /**
         * Returns last item in the tree.
         *
         * @throws IgniteInternalCheckedException If failure.
         */
        public T find() throws IgniteInternalCheckedException {
            while (retry) {
                row = row0;

                doFind(this);
            }

            return row0;
        }
    }

    /**
     * Put operation.
     */
    public final class Put extends Update {
        /** Right child page ID for split row. */
        long rightId;

        /** Replaced row if any. */
        T oldRow;

        /**
         * Bottom level for insertion (insert can't go deeper). Will be incremented on split on each level.
         */
        short btmLvl;

        final boolean needOld;

        /**
         * Constructor.
         *
         * @param row Row.
         * @param needOld {@code True} If need return old value.
         */
        private Put(T row, boolean needOld) {
            this(row, needOld, null);
        }

        /**
         * Constructor.
         *
         * @param row Row.
         * @param needOld {@code True} If need return old value.
         * @param onUpdateCallback Callback after performing an update of tree row while on a page with that tree row under its write lock.
         */
        private Put(T row, boolean needOld, @Nullable Runnable onUpdateCallback) {
            super(row, onUpdateCallback);

            this.needOld = needOld;
        }

        @Override
        boolean notFound(BplusIo<L> io, long pageAddr, int idx, int lvl) {
            assert btmLvl >= 0 : btmLvl;
            assert lvl >= btmLvl : lvl;

            return lvl == btmLvl;
        }

        @Override
        protected Result finishOrLockTail(long pageId, long page, long backId, long fwdId, int lvl)
                throws IgniteInternalCheckedException {
            if (btmLvl == lvl) {
                // Insert for the split.
                return tryInsert(pageId, page, fwdId, lvl);
            }

            // Finish inner replace.
            Result res = finishTail();

            // Add this page to the tail if inner replace has not happened.
            if (res == NOT_FOUND) {
                // Set forward id to check the triangle invariant under the write-lock.
                fwdId(fwdId);

                res = write(pageId, page, lockTailExact, this, lvl, RETRY);
            }

            // Release tail if retry is required.
            if (res == RETRY) {
                releaseTail();
            }

            return res;
        }

        @Override
        protected Result finishTail() throws IgniteInternalCheckedException {
            // An inner node is required for replacement.
            if (tail.lvl == 0) {
                return NOT_FOUND;
            }

            int idx = insertionPoint(tail);

            // Missing row means that current page must be added to tail.
            if (idx < 0) {
                idx = fix(idx);

                BplusInnerIo<L> io = (BplusInnerIo<L>) tail.io;

                // Release tail in case of broken triangle invariant in locked pages.
                if (io.getLeft(tail.buf, idx, partId) != tail.down.pageId) {
                    releaseTail();

                    return RETRY;
                }

                return NOT_FOUND;
            }

            assert oldRow == null : "The old row must be set only once.";

            // Insertion index is found. Replace must be performed in both tail top and tail bottom.
            replaceRowInPage(tail.io, tail.buf, idx);

            // Unlock everything until the leaf, there's no need to hold these locks anymore.
            while (tail.lvl != 0) {
                writeUnlockAndClose(tail.pageId, tail.page, tail.buf);

                tail = tail.down;
            }

            // Read old row from the leaf to reduce contention in inner pages.
            oldRow = needOld ? getRow(tail.io, tail.buf, tail.idx) : (T) Boolean.TRUE;

            // Replace row in the leaf.
            replaceRowInPage(tail.io, tail.buf, tail.idx);

            finish();

            return FOUND;
        }

        /**
         * Tail page is kept locked after split until insert to the upper level will not be finished. It is needed because split row will be
         * "in flight" and if we'll release tail, remove on split row may fail.
         *
         * @param tailId Tail page ID.
         * @param tailPage Tail page pointer.
         * @param tailPageAddr Tail page address
         * @param io Tail page IO.
         * @param lvl Tail page level.
         */
        private void setTailForSplit(long tailId, long tailPage, long tailPageAddr, BplusIo<L> io, int lvl) {
            assert tailId != 0L && tailPage != 0L && tailPageAddr != 0L;

            // Old tail must be unlocked.
            releaseTail();

            addTail(tailId, tailPage, tailPageAddr, io, lvl, Tail.EXACT);
        }

        /**
         * Finish put.
         */
        private void finish() {
            row = null;
            rightId = 0;

            releaseTail();
        }

        @Override
        boolean isFinished() {
            return row == null;
        }

        /**
         * Inserts a row.
         *
         * @param pageId Page ID.
         * @param pageAddr Page address.
         * @param io IO.
         * @param idx Index.
         * @param lvl Level.
         * @return Move up row.
         * @throws IgniteInternalCheckedException If failed.
         */
        private @Nullable L insert(long pageId, long pageAddr, BplusIo<L> io, int idx, int lvl) throws IgniteInternalCheckedException {
            int maxCnt = io.getMaxCount(pageSize());
            int cnt = io.getCount(pageAddr);

            if (cnt == maxCnt) {
                // Need to split page.
                return insertWithSplit(pageId, pageAddr, io, idx, lvl);
            }

            insertSimple(pageAddr, io, idx);

            return null;
        }

        private void insertSimple(long pageAddr, BplusIo<L> io, int idx) throws IgniteInternalCheckedException {
            io.insert(pageAddr, idx, row, null, rightId, false);

            if (onUpdateCallback != null) {
                onUpdateCallback.run();
            }
        }

        /**
         * Inserts with split.
         *
         * @param pageId Page ID.
         * @param pageAddr Page address.
         * @param io IO.
         * @param idx Index.
         * @param lvl Level.
         * @return Move up row.
         * @throws IgniteInternalCheckedException If failed.
         */
        private L insertWithSplit(
                long pageId,
                long pageAddr,
                BplusIo<L> io,
                int idx,
                int lvl
        ) throws IgniteInternalCheckedException {
            long fwdId = allocatePage(null);
            long fwdPage = acquirePage(fwdId);

            try {
                // Need to check this before the actual split, because after the split we will have new forward page here.
                boolean hadFwd = io.getForward(pageAddr, partId) != 0;

                long fwdPageAddr = writeLock(fwdId, fwdPage); // Initial write, no need to check for concurrent modification.

                assert fwdPageAddr != 0L;

                try {
                    boolean midShift = splitPage(pageAddr, io, fwdId, fwdPageAddr, idx);

                    // Do insert.
                    int cnt = io.getCount(pageAddr);

                    if (idx < cnt || (idx == cnt && !midShift)) { // Insert into back page.
                        insertSimple(pageAddr, io, idx);

                        // Fix leftmost child of forward page, because newly inserted row will go up.
                        if (idx == cnt && !io.isLeaf()) {
                            inner(io).setLeft(fwdPageAddr, 0, rightId);
                        }
                    } else {
                        // Insert into newly allocated forward page.
                        insertSimple(fwdPageAddr, io, idx - cnt);
                    }

                    // Do move up.
                    cnt = io.getCount(pageAddr);

                    // Last item from backward row goes up.
                    L moveUpRow = io.getLookupRow(BplusTree.this, pageAddr, cnt - 1);

                    if (!io.isLeaf()) { // Leaf pages must contain all the links, inner pages remove moveUpLink.
                        io.setCount(pageAddr, cnt - 1);
                    }

                    if (!hadFwd && lvl == getRootLevel()) { // We are splitting root.
                        long newRootId = allocatePage(null);
                        long newRootPage = acquirePage(newRootId);

                        try {
                            if (io.isLeaf()) {
                                io = latestInnerIo();
                            }

                            long newRootAddr = writeLock(newRootId, newRootPage); // Initial write.

                            assert newRootAddr != 0L;

                            try {
                                inner(io).initNewRoot(
                                        newRootAddr,
                                        newRootId,
                                        pageId,
                                        moveUpRow,
                                        null,
                                        fwdId,
                                        pageSize(),
                                        false
                                );
                            } finally {
                                writeUnlock(newRootId, newRootPage, newRootAddr, true);
                            }
                        } finally {
                            releasePage(newRootId, newRootPage);
                        }

                        Bool res = write(metaPageId, addRoot, newRootId, lvl + 1, FALSE);

                        assert res == TRUE : res;

                        return null; // We've just moved link up to root, nothing to return here.
                    }

                    // Regular split.
                    return moveUpRow;
                } finally {
                    writeUnlock(fwdId, fwdPage, fwdPageAddr, true);
                }
            } finally {
                releasePage(fwdId, fwdPage);
            }
        }

        /**
         * Tries to insert.
         *
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteInternalCheckedException If failed.
         */
        private Result tryInsert(long pageId, long page, long fwdId, int lvl) throws IgniteInternalCheckedException {
            // Init args.
            this.pageId = pageId;
            this.fwdId = fwdId;

            return write(pageId, page, insert, this, lvl, RETRY);
        }

        /**
         * Tries to replace.
         *
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteInternalCheckedException If failed.
         */
        private Result tryReplace(long pageId, long page, long fwdId, int lvl) throws IgniteInternalCheckedException {
            // Init args.
            this.pageId = pageId;
            this.fwdId = fwdId;

            return write(pageId, page, replace, this, lvl, RETRY);
        }

        /**
         * Replaces a row in the page with a new one.
         *
         * @param io Page IO for the page.
         * @param pageAddr Page address.
         * @param idx Replacement index.
         */
        private void replaceRowInPage(BplusIo<L> io, long pageAddr, int idx) throws IgniteInternalCheckedException {
            io.store(pageAddr, idx, row, null, false);

            if (onUpdateCallback != null) {
                onUpdateCallback.run();
            }
        }

        @Override
        void checkLockRetry() throws IgniteInternalCheckedException {
            // Non-null tail means that lock on the tail page is still being held, and we can't fail with exception.
            if (tail == null) {
                super.checkLockRetry();
            }
        }

        @Override
        public String toString() {
            return "Put [super=" + super.toString() + ", needOld=" + needOld + "]";
        }
    }

    /**
     * Invoke operation.
     */
    public final class Invoke extends Get {
        Object arg;

        InvokeClosure<T> clo;

        Bool closureInvoked = FALSE;

        T foundRow;

        Update op;

        /**
         * Constructor.
         *
         * @param row Row.
         * @param arg Implementation specific argument.
         * @param clo Closure.
         */
        private Invoke(L row, Object arg, InvokeClosure<T> clo) {
            super(row, false);

            assert clo != null;

            this.clo = clo;
            this.arg = arg;
        }

        @Override
        void pageId(long pageId) {
            this.pageId = pageId;

            if (op != null) {
                op.pageId = pageId;
            }
        }

        @Override
        void fwdId(long fwdId) {
            this.fwdId = fwdId;

            if (op != null) {
                op.fwdId = fwdId;
            }
        }

        @Override
        void backId(long backId) {
            this.backId = backId;

            if (op != null) {
                op.backId = backId;
            }
        }

        @Override
        void restartFromRoot(long rootId, int rootLvl, long rmvId) {
            super.restartFromRoot(rootId, rootLvl, rmvId);

            if (op != null) {
                op.restartFromRoot(rootId, rootLvl, rmvId);
            }
        }

        @Override
        boolean found(BplusIo<L> io, long pageAddr, int idx, int lvl) throws IgniteInternalCheckedException {
            // If the operation is initialized, then the closure has been called already.
            if (op != null) {
                return op.found(io, pageAddr, idx, lvl);
            }

            if (lvl == 0) {
                if (closureInvoked == FALSE) {
                    closureInvoked = READY;

                    foundRow = getRow(io, pageAddr, idx, arg);
                }

                return true;
            }

            return false;
        }

        @Override
        boolean notFound(BplusIo<L> io, long pageAddr, int idx, int lvl) throws IgniteInternalCheckedException {
            // If the operation is initialized, then the closure has been called already.
            if (op != null) {
                return op.notFound(io, pageAddr, idx, lvl);
            }

            if (lvl == 0) {
                if (closureInvoked == FALSE) {
                    closureInvoked = READY;
                }

                return true;
            }

            return false;
        }

        private void invokeClosure() throws IgniteInternalCheckedException {
            if (closureInvoked != READY) {
                return;
            }

            closureInvoked = DONE;

            clo.call(foundRow);

            switch (clo.operationType()) {
                case PUT:
                    T newRow = clo.newRow();

                    assert newRow != null;

                    op = new Put(newRow, false, clo::onUpdate);

                    break;

                case REMOVE:
                    assert foundRow != null;

                    op = new Remove(row, false, clo::onUpdate);

                    break;

                case NOOP:
                case IN_PLACE:
                    return;

                default:
                    throw new IllegalStateException();
            }

            op.copyFrom(this);

            op.invoke = this;
        }

        @Override
        public boolean canRelease(long pageId, int lvl) {
            return pageId != 0L && (op == null || op.canRelease(pageId, lvl));
        }

        /**
         * Returns {@code true} If it is a {@link Put} operation internally.
         */
        private boolean isPut() {
            return op != null && op.getClass() == Put.class;
        }

        /**
         * Returns {@code true} If it is a {@link Remove} operation internally.
         */
        private boolean isRemove() {
            return op != null && op.getClass() == Remove.class;
        }

        /**
         * Returns {@code true} if it is a {@link Remove} and the page is in tail.
         *
         * @param pageId Page ID.
         * @param lvl Level.
         */
        private boolean isTail(long pageId, int lvl) {
            return op != null && op.isTail(pageId, lvl);
        }

        private void levelExit() {
            if (isRemove()) {
                ((Remove) op).page = 0L;
            }
        }

        /**
         * Release all the resources by the end of operation.
         *
         * @throws IgniteInternalCheckedException if failed.
         */
        private void releaseAll() throws IgniteInternalCheckedException {
            if (isRemove()) {
                ((Remove) op).releaseAll();
            }
        }

        /**
         * Callback when the element is not found in the leaf.
         *
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteInternalCheckedException If failed.
         */
        private Result onNotFound(long pageId, long page, long fwdId, int lvl)
                throws IgniteInternalCheckedException {
            if (op == null) {
                return NOT_FOUND;
            }

            if (isRemove()) {
                assert lvl == 0;

                ((Remove) op).finish();

                return NOT_FOUND;
            }

            return ((Put) op).tryInsert(pageId, page, fwdId, lvl);
        }

        /**
         * Callback when an element is found in the leaf.
         *
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param backId Back page ID.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteInternalCheckedException If failed.
         */
        private Result onFound(long pageId, long page, long backId, long fwdId, int lvl) throws IgniteInternalCheckedException {
            if (op == null) {
                return FOUND;
            }

            if (isRemove()) {
                return ((Remove) op).tryRemoveFromLeaf(pageId, page, backId, fwdId, lvl);
            }

            return ((Put) op).tryReplace(pageId, page, fwdId, lvl);
        }

        private Result tryFinish() throws IgniteInternalCheckedException {
            assert op != null; // Must be guarded by isFinished.

            Result res = op.finishTail();

            if (res == NOT_FOUND) {
                res = RETRY;
            }

            if (res == RETRY && isPut()) {
                op.releaseTail();
            }

            assert res == FOUND || res == RETRY : res;

            return res;
        }

        @Override
        boolean isFinished() {
            return closureInvoked == DONE && (op == null || op.isFinished());
        }
    }

    /**
     * Update operation. Has basic operations for {@link Tail} support.
     */
    private abstract class Update extends Get {
        /** We may need to lock part of the tree branch from the bottom to up for multiple levels. */
        @Nullable Tail<L> tail;

        /**
         * Callback after performing an {@link Put put} or {@link Remove remove} of a tree row while on a page with that tree row under its
         * write lock.
         */
        final @Nullable Runnable onUpdateCallback;

        /**
         * Constructor.
         *
         * @param row Row.
         */
        private Update(L row) {
            this(row, null);
        }

        /**
         * Constructor.
         *
         * @param row Row.
         * @param onUpdateCallback Callback after performing an {@link Put put} or {@link Remove remove} of a tree row while on a page with
         *      that tree row under its write lock.
         */
        private Update(L row, @Nullable Runnable onUpdateCallback) {
            super(row, false);

            this.onUpdateCallback = onUpdateCallback;
        }

        /**
         * Method that's invoked when operation goes up from the recursion and {@link #isFinished()} returns false. Either finishes the
         * operation or locks the page for further processing on another level.
         * <p/>
         * Returns {@link Result#FOUND} if operation has finished and {@link #isFinished()} returns {@code true} now.
         * <p/>
         * Returns {@link Result#RETRY} if operation should be retried.
         * <p/>
         * Returns {@link Result#NOT_FOUND} if operation has added the page to tail, meaning that operation can't be finished on current
         * level.
         *
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param backId Back page ID.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteInternalCheckedException If failed.
         */
        protected abstract Result finishOrLockTail(
                long pageId,
                long page,
                long backId,
                long fwdId,
                int lvl
        ) throws IgniteInternalCheckedException;

        /**
         * Process tail and finish. Same as {@link #finishOrLockTail(long, long, long, long, int)} but doesn't add the page to the tail.
         *
         * @return Result.
         * @throws IgniteInternalCheckedException If failed.
         */
        protected abstract Result finishTail() throws IgniteInternalCheckedException;

        /**
         * Release pages for all locked levels at the tail.
         */
        protected final void releaseTail() {
            doReleaseTail(tail);

            tail = null;
        }

        /**
         * Returns {@code true} if tail level is correct.
         *
         * @param rootLvl Actual root level.
         */
        protected final boolean checkTailLevel(int rootLvl) {
            return tail == null || tail.lvl < rootLvl;
        }

        /**
         * Releases the tail.
         *
         * @param t Tail.
         */
        protected final void doReleaseTail(@Nullable Tail<L> t) {
            while (t != null) {
                writeUnlockAndClose(t.pageId, t.page, t.buf);

                Tail<L> s = t.sibling;

                if (s != null) {
                    writeUnlockAndClose(s.pageId, s.page, s.buf);
                }

                t = t.down;
            }
        }

        @Override
        public final boolean canRelease(long pageId, int lvl) {
            return pageId != 0L && !isTail(pageId, lvl);
        }

        /**
         * Returns {@code true} If the given page is in tail.
         *
         * @param pageId Page ID.
         * @param lvl Level.
         */
        protected final boolean isTail(long pageId, int lvl) {
            Tail<L> t = tail;

            while (t != null) {
                if (t.lvl < lvl) {
                    return false;
                }

                if (t.lvl == lvl) {
                    if (t.pageId == pageId) {
                        return true;
                    }

                    t = t.sibling;

                    return t != null && t.pageId == pageId;
                }

                t = t.down;
            }

            return false;
        }

        /**
         * Adds tail.
         *
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param io IO.
         * @param lvl Level.
         * @param type Type.
         * @return Added tail.
         */
        protected final Tail<L> addTail(long pageId, long page, long pageAddr, BplusIo<L> io, int lvl, byte type) {
            Tail<L> t = new Tail<>(pageId, page, pageAddr, io, type, lvl);

            if (tail == null) {
                tail = t;
            } else if (tail.lvl == lvl) { // Add on the same level.
                assert tail.sibling == null; // Only two siblings on a single level.

                if (type == Tail.EXACT) {
                    assert tail.type != Tail.EXACT;

                    if (tail.down != null) { // Take down from sibling, EXACT must own down link.
                        t.down = tail.down;
                        tail.down = null;
                    }

                    t.sibling = tail;
                    tail = t;
                } else {
                    assert tail.type == Tail.EXACT : tail.type;

                    tail.sibling = t;
                }
            } else { // Add on top of existing level.
                assert tail.lvl == lvl - 1 : "tail=" + tail + ", lvl=" + lvl;

                t.down = tail;
                tail = t;
            }

            return t;
        }

        /**
         * Returns tail of {@link Tail#EXACT} type at the given level.
         *
         * @param tail Tail to start with.
         * @param lvl Level.
         */
        protected final Tail<L> getTail(Tail<L> tail, int lvl) {
            assert tail != null;
            assert lvl >= 0 && lvl <= tail.lvl : lvl;

            Tail<L> t = tail;

            while (t.lvl != lvl) {
                t = t.down;
            }

            assert t.type == Tail.EXACT : t.type; // All the down links must be of EXACT type.

            return t;
        }

        /**
         * Returns insertion point. May be negative.
         *
         * @param tail Tail.
         * @throws IgniteInternalCheckedException If failed.
         */
        protected final int insertionPoint(Tail<L> tail) throws IgniteInternalCheckedException {
            assert tail.type == Tail.EXACT : tail.type;

            if (tail.idx == Short.MIN_VALUE) {
                int idx = findInsertionPoint(tail.lvl, tail.io, tail.buf, 0, tail.getCount(), row, 0);

                assert checkIndex(idx) : idx;

                tail.idx = (short) idx;
            }

            return tail.idx;
        }

        /**
         * Returns tail as a String.
         *
         * @param keys If we have to show keys.
         * @throws IgniteInternalCheckedException If failed.
         */
        protected final String printTail(boolean keys) throws IgniteInternalCheckedException {
            IgniteStringBuilder sb = new IgniteStringBuilder("");

            Tail<L> t = tail;

            while (t != null) {
                sb.app(t.lvl).app(": ").app(printPage(t.io, t.buf, keys));

                Tail<L> d = t.down;

                t = t.sibling;

                if (t != null) {
                    sb.app(" -> ").app(t.type == Tail.FORWARD ? "F" : "B").app(' ').app(printPage(t.io, t.buf, keys));
                }

                sb.app('\n');

                t = d;
            }

            return sb.toString();
        }

        @Override
        public String toString() {
            try {
                return "Update [tail=" + printTail(false) + ']';
            } catch (IgniteInternalCheckedException ignore) {
                // Should be impossible if "keys == false" in "printTail".
                return null;
            }
        }
    }

    /**
     * Remove operation.
     */
    public final class Remove extends Update implements ReuseBag {
        boolean needReplaceInner;

        Bool needMergeEmptyBranch = FALSE;

        /** Removed row. */
        T rmvd;

        /** Current page absolute pointer. */
        long page;

        @Nullable Object freePages;

        final boolean needOld;

        /**
         * Constructor.
         *
         * @param row Row.
         * @param needOld {@code True} If need return old value.
         */
        private Remove(L row, boolean needOld) {
            this(row, needOld, null);
        }

        /**
         * Constructor.
         *
         * @param row Row.
         * @param needOld {@code True} If need return old value.
         * @param onRemoveCallback Callback after performing an remove of tree row while on a page with that tree row under its write lock.
         */
        private Remove(L row, boolean needOld, @Nullable Runnable onRemoveCallback) {
            super(row, onRemoveCallback);

            this.needOld = needOld;
        }

        @Override
        public long pollFreePage() {
            if (freePages == null) {
                return 0L;
            } else if (freePages instanceof LongArrayFIFOQueue) {
                LongArrayFIFOQueue pages = ((LongArrayFIFOQueue) freePages);

                return pages.isEmpty() ? 0L : pages.dequeueLastLong();
            }

            long res = (long) freePages;

            freePages = null;

            return res;
        }

        @Override
        public void addFreePage(long pageId) {
            assert pageId != 0L;

            if (freePages == null) {
                freePages = pageId;
            } else {
                LongArrayFIFOQueue pages;

                if (freePages instanceof LongArrayFIFOQueue) {
                    pages = (LongArrayFIFOQueue) freePages;
                } else {
                    pages = new LongArrayFIFOQueue(4);

                    pages.enqueue((long) freePages);

                    freePages = pages;
                }

                pages.enqueue(pageId);
            }
        }

        @Override
        public boolean isEmpty() {
            if (freePages == null) {
                return true;
            } else if (freePages instanceof LongArrayFIFOQueue) {
                return ((LongArrayFIFOQueue) freePages).isEmpty();
            }

            return false;
        }

        @Override
        boolean notFound(BplusIo<L> io, long pageAddr, int idx, int lvl) {
            if (lvl == 0) {
                assert tail == null;

                return true;
            }

            return false;
        }

        /**
         * Finish the operation.
         */
        private void finish() {
            assert tail == null;

            row = null;
        }

        /**
         * Merges empty branches.
         *
         * @return Tail to release if an empty branch was not merged.
         * @throws IgniteInternalCheckedException If failed.
         */
        private @Nullable Tail<L> mergeEmptyBranch() throws IgniteInternalCheckedException {
            assert needMergeEmptyBranch == TRUE : needMergeEmptyBranch;

            Tail<L> t = tail;

            // Find empty branch beginning.
            for (Tail<L> t0 = t.down; t0.lvl != 0; t0 = t0.down) {
                assert t0.type == Tail.EXACT : t0.type;

                if (t0.getCount() != 0) {
                    t = t0; // Here we correctly handle empty windows in the middle of branch.
                }
            }

            int cnt = t.getCount();
            int idx = fix(insertionPoint(t));

            assert cnt > 0 : cnt;

            if (idx == cnt) {
                idx--;
            }

            // If at the join point we will not be able to merge, we need to retry.
            if (!checkChildren(t, t.getLeftChild(), t.getRightChild(), idx)) {
                return t;
            }

            // Make the branch really empty before the merge.
            removeDataRowFromLeafTail(t);

            while (t.lvl != 0) { // If we've found empty branch, merge it top-down.
                boolean res = merge(t);

                // All the merges must succeed because we have only empty pages from one side.
                assert res : needMergeEmptyBranch + "\n" + printTail(true);

                if (needMergeEmptyBranch == TRUE) {
                    needMergeEmptyBranch = READY; // Need to mark that we've already done the first (top) merge.
                }

                t = t.down;
            }

            return null; // Succeeded.
        }

        private void mergeBottomUp(Tail<L> t) throws IgniteInternalCheckedException {
            assert needMergeEmptyBranch == FALSE || needMergeEmptyBranch == DONE : needMergeEmptyBranch;

            if (t.down == null || t.down.sibling == null) {
                // Do remove if we did not yet.
                if (t.lvl == 0 && !isRemoved()) {
                    removeDataRowFromLeafTail(t);
                }

                return; // Nothing to merge.
            }

            mergeBottomUp(t.down);
            merge(t);
        }

        /**
         * Checks if inner key in tail.
         *
         * @return {@code true} If found.
         * @throws IgniteInternalCheckedException If failed.
         */
        private boolean isInnerKeyInTail() throws IgniteInternalCheckedException {
            assert tail.lvl > 0 : tail.lvl;

            return insertionPoint(tail) >= 0;
        }

        /**
         * Returns {@code true} If already removed from leaf.
         */
        private boolean isRemoved() {
            return rmvd != null;
        }

        /**
         * Returns {@code true} if we need to retry or {@code false} to exit.
         *
         * @param t Tail to release.
         */
        private boolean releaseForRetry(Tail<L> t) {
            // Try to simply release all first.
            if (t.lvl <= 1) {
                // We've just locked leaf and did not do the remove, can safely release all and retry.
                assert !isRemoved() : "removed";

                // These fields will be setup again on remove from leaf.
                needReplaceInner = false;
                needMergeEmptyBranch = FALSE;

                releaseTail();

                return true;
            }

            // Release all up to the given tail with its direct children.
            if (t.down != null) {
                Tail<L> newTail = t.down.down;

                if (newTail != null) {
                    t.down.down = null;

                    releaseTail();

                    tail = newTail;

                    return true;
                }
            }

            // Here we wanted to do a regular merge after all the important operations,
            // so we can leave this invalid tail as is. We have no other choice here
            // because our tail is not long enough for retry. Exiting.
            assert isRemoved() && !needReplaceInner && needMergeEmptyBranch != TRUE
                    : "isRemoved=" + isRemoved() + ", needReplaceInner=" + needReplaceInner
                    + ", needMergeEmptyBranch=" + needMergeEmptyBranch;

            return false;
        }

        @Override
        protected Result finishTail() throws IgniteInternalCheckedException {
            assert !isFinished() && tail.type == Tail.EXACT && tail.lvl >= 0 && needMergeEmptyBranch != READY
                    : "isFinished=" + isFinished() + ", tail=" + tail + ", needMergeEmptyBranch=" + needMergeEmptyBranch;

            if (tail.lvl == 0) {
                // At the bottom level we can't have a tail without a sibling, it means we have higher levels.
                assert tail.sibling != null : tail;

                return NOT_FOUND; // Lock upper level, we are at the bottom now.
            } else {
                // We may lock wrong triangle because of concurrent operations.
                if (!validateTail()) {
                    if (releaseForRetry(tail)) {
                        return RETRY;
                    }

                    // It was a regular merge, leave as is and exit.
                } else {
                    // Try to find inner key on inner level.
                    if (needReplaceInner && !isInnerKeyInTail()) {
                        // Since we setup needReplaceInner in leaf page write lock and do not release it,
                        // we should not be able to miss the inner key. Even if concurrent merge
                        // happened the inner key must still exist.
                        return NOT_FOUND; // Lock the whole branch up to the inner key.
                    }

                    // Try to merge an empty branch.
                    if (needMergeEmptyBranch == TRUE) {
                        // We can't merge empty branch if tail is a routing page.
                        if (tail.getCount() == 0) {
                            return NOT_FOUND; // Lock the whole branch up to the first non-routing.
                        }

                        // Top-down merge for empty branch. The actual row remove will happen here if everything is ok.
                        Tail<L> t = mergeEmptyBranch();

                        if (t != null) {
                            // We were not able to merge empty branch, need to release and retry.
                            boolean ok = releaseForRetry(t);

                            assert ok; // Here we must always retry because it is not a regular merge.

                            return RETRY;
                        }

                        needMergeEmptyBranch = DONE;
                    }

                    // The actual row remove may happen here as well.
                    mergeBottomUp(tail);

                    if (needReplaceInner) {
                        replaceInner(); // Replace inner key with new max key for the left subtree.

                        needReplaceInner = false;
                    }

                    // Loop is needed to prevent the rare case when, after parallel remove of keys, empty root remains.
                    // B+tree after removes key: [empty_root] - [empty_inner_node] - [5] ==>
                    // B+tree after cutting empty root: [5]
                    while (tail.getCount() == 0 && tail.lvl != 0 && getRootLevel() == tail.lvl) {
                        // Free root if it became empty after merge.

                        cutRoot(tail.lvl);
                        freePage(tail.pageId, tail.page, tail.buf, true);

                        tail = tail.down;

                        assert tail.sibling == null : tail;

                        // Exit: we are done.
                    }

                    if (tail.sibling != null && tail.getCount() + tail.sibling.getCount() < tail.io.getMaxCount(pageSize())) {
                        // Release everything lower than tail, we've already merged this path.
                        doReleaseTail(tail.down);
                        tail.down = null;

                        return NOT_FOUND; // Lock and merge one level more.
                    }

                    // We don't want to merge anything more, exiting.
                }
            }

            // If we've found nothing in the tree, we should not do any modifications or take tail locks.
            assert isRemoved();

            releaseTail();
            finish();

            return FOUND;
        }

        /**
         * Removes data page from leaf tail.
         *
         * @param t Tail.
         * @throws IgniteInternalCheckedException If failed.
         */
        private void removeDataRowFromLeafTail(Tail<L> t) throws IgniteInternalCheckedException {
            assert !isRemoved();

            Tail<L> leaf = getTail(t, 0);

            removeDataRowFromLeaf(leaf.buf, leaf.io, leaf.getCount(), insertionPoint(leaf));
        }

        /**
         * Removes form leaf.
         *
         * @param leafId Leaf page ID.
         * @param leafPage Leaf page pointer.
         * @param backId Back page ID.
         * @param fwdId Forward ID.
         * @return Result code.
         * @throws IgniteInternalCheckedException If failed.
         */
        private Result removeFromLeaf(long leafId, long leafPage, long backId, long fwdId) throws IgniteInternalCheckedException {
            // Init parameters.
            pageId = leafId;
            page = leafPage;
            this.backId = backId;
            this.fwdId = fwdId;

            // Usually this will be true, so in most cases we should not lock any extra pages.
            if (backId == 0) {
                return doRemoveFromLeaf();
            }

            // Lock back page before the remove, we'll need it for merges.
            long backPage = acquirePage(backId);

            try {
                return write(backId, backPage, lockBackAndRmvFromLeaf, this, 0, RETRY);
            } finally {
                if (canRelease(backId, 0)) {
                    releasePage(backId, backPage);
                }
            }
        }

        /**
         * Does a remove from leaf.
         *
         * @return Result code.
         * @throws IgniteInternalCheckedException If failed.
         */
        private Result doRemoveFromLeaf() throws IgniteInternalCheckedException {
            assert page != 0L;

            return write(pageId, page, rmvFromLeaf, this, 0, RETRY);
        }

        /**
         * Does a lock tail.
         *
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteInternalCheckedException If failed.
         */
        private Result doLockTail(int lvl) throws IgniteInternalCheckedException {
            assert page != 0L;

            return write(pageId, page, lockTail, this, lvl, RETRY);
        }

        /**
         * Locks the tail.
         *
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param backId Back page ID.
         * @param fwdId Expected forward page ID.
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteInternalCheckedException If failed.
         */
        private Result lockTail(long pageId, long page, long backId, long fwdId, int lvl) throws IgniteInternalCheckedException {
            assert tail != null;

            // Init parameters for the handlers.
            this.pageId = pageId;
            this.page = page;
            this.fwdId = fwdId;
            this.backId = backId;

            if (backId == 0) {
                // Back page ID is provided only when the last move was to the right.
                return doLockTail(lvl);
            }

            long backPage = acquirePage(backId);

            try {
                return write(backId, backPage, lockBackAndTail, this, lvl, RETRY);
            } finally {
                if (canRelease(backId, lvl)) {
                    releasePage(backId, backPage);
                }
            }
        }

        /**
         * Locks the forward page.
         *
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteInternalCheckedException If failed.
         */
        private Result lockForward(int lvl) throws IgniteInternalCheckedException {
            assert fwdId != 0 : fwdId;
            assert backId == 0 : backId;

            long fwdId = this.fwdId;
            long fwdPage = acquirePage(fwdId);

            try {
                return write(fwdId, fwdPage, lockTailForward, this, lvl, RETRY);
            } finally {
                // If we were not able to lock forward page as tail, release the page.
                if (canRelease(fwdId, lvl)) {
                    releasePage(fwdId, fwdPage);
                }
            }
        }

        /**
         * Deletes a data page from a leaf.
         *
         * @param pageAddr Page address.
         * @param io IO.
         * @param cnt Count.
         * @param idx Index to remove.
         * @throws IgniteInternalCheckedException If failed.
         */
        private void removeDataRowFromLeaf(long pageAddr, BplusIo<L> io, int cnt, int idx) throws IgniteInternalCheckedException {
            assert idx >= 0 && idx < cnt : idx;
            assert io.isLeaf() : "inner";
            assert !isRemoved() : "already removed";

            // Detach the row.
            rmvd = needOld ? getRow(io, pageAddr, idx) : (T) Boolean.TRUE;

            doRemove(pageAddr, io, cnt, idx);

            assert isRemoved();

            if (onUpdateCallback != null) {
                onUpdateCallback.run();
            }
        }

        /**
         * Performs the removal.
         *
         * @param pageAddr Page address.
         * @param io IO.
         * @param cnt Count.
         * @param idx Index to remove.
         * @throws IgniteInternalCheckedException If failed.
         */
        private void doRemove(long pageAddr, BplusIo<L> io, int cnt, int idx) throws IgniteInternalCheckedException {
            assert cnt > 0 : cnt;
            assert idx >= 0 && idx < cnt : idx + " " + cnt;

            io.remove(pageAddr, idx, cnt);
        }

        /**
         * Returns {@code true} if the currently locked tail is valid.
         *
         * @throws IgniteInternalCheckedException If failed.
         */
        private boolean validateTail() throws IgniteInternalCheckedException {
            Tail<L> t = tail;

            if (t.down == null) {
                // It is just a regular merge in progress.
                assert needMergeEmptyBranch != TRUE && !needReplaceInner
                        : "needMergeEmptyBranch=" + needMergeEmptyBranch + ", needReplaceInner" + needReplaceInner;

                return true;
            }

            Tail<L> left = t.getLeftChild();
            Tail<L> right = t.getRightChild();

            assert left.pageId != right.pageId;

            int cnt = t.getCount();

            if (cnt != 0) {
                int idx = fix(insertionPoint(t));

                if (idx == cnt) {
                    idx--;
                }

                // The locked left and right pages allowed to be children of the tail.
                if (isChild(t, left, idx, cnt, false) && isChild(t, right, idx, cnt, true)) {
                    return true;
                }
            }

            // Otherwise they must correctly reside with respect to tail sibling.
            Tail<L> s = t.sibling;

            if (s == null) {
                return false;
            }

            // It must be the rightmost element.
            int idx = cnt == 0 ? 0 : cnt - 1;

            if (s.type == Tail.FORWARD) {
                return isChild(t, left, idx, cnt, true) && isChild(s, right, 0, 0, false);
            }

            assert s.type == Tail.BACK;

            if (!isChild(t, right, 0, 0, false)) {
                return false;
            }

            cnt = s.getCount();
            idx = cnt == 0 ? 0 : cnt - 1;

            return isChild(s, left, idx, cnt, true);
        }

        /**
         * Returns {@code true} if they are really parent and child.
         *
         * @param prnt Parent.
         * @param child Child.
         * @param idx Index.
         * @param cnt Count.
         * @param right Right or left.
         */
        private boolean isChild(Tail<L> prnt, Tail<L> child, int idx, int cnt, boolean right) {
            if (right && cnt != 0) {
                idx++;
            }

            return inner(prnt.io).getLeft(prnt.buf, idx, partId) == child.pageId;
        }

        /**
         * Checks the children.
         *
         * @param prnt Parent.
         * @param left Left.
         * @param right Right.
         * @param idx Index.
         * @return {@code true} if children are correct.
         */
        private boolean checkChildren(Tail<L> prnt, Tail<L> left, Tail<L> right, int idx) {
            assert idx >= 0 && idx < prnt.getCount() : idx;

            return inner(prnt.io).getLeft(prnt.buf, idx, partId) == left.pageId
                    && inner(prnt.io).getRight(prnt.buf, idx, partId) == right.pageId;
        }

        /**
         * Tries to merge the left and right child.
         *
         * @param prnt Parent tail.
         * @param left Left child tail.
         * @param right Right child tail.
         * @return {@code true} If merged successfully.
         * @throws IgniteInternalCheckedException If failed.
         */
        private boolean doMerge(Tail<L> prnt, Tail<L> left, Tail<L> right) throws IgniteInternalCheckedException {
            assert right.io == left.io; // Otherwise incompatible.
            assert left.io.getForward(left.buf, partId) == right.pageId;

            int prntCnt = prnt.getCount();
            int prntIdx = fix(insertionPoint(prnt));

            // Fix index for the right move: remove the last item.
            if (prntIdx == prntCnt) {
                prntIdx--;
            }

            // The only case when the siblings can have different parents is when we are merging
            // top-down an empty branch and we already merged the join point with non-empty branch.
            // This happens because when merging empty page we do not update parent link to a lower
            // empty page in the branch since it will be dropped anyway.
            if (needMergeEmptyBranch == READY) {
                assert left.getCount() == 0 || right.getCount() == 0; // Empty branch check.
            } else if (!checkChildren(prnt, left, right, prntIdx)) {
                return false;
            }

            boolean emptyBranch = needMergeEmptyBranch == TRUE || needMergeEmptyBranch == READY;

            if (!left.io.merge(prnt.io, prnt.buf, prntIdx, left.buf, right.buf, emptyBranch, pageSize())) {
                return false;
            }

            // Invalidate indexes after successful merge.
            prnt.idx = Short.MIN_VALUE;
            left.idx = Short.MIN_VALUE;

            // Remove split key from parent. If we are merging empty branch then remove only on the top iteration.
            if (needMergeEmptyBranch != READY) {
                doRemove(prnt.buf, prnt.io, prntCnt, prntIdx);
            }

            // Forward page is now empty and has no links, can free and release it right away.
            freePage(right.pageId, right.page, right.buf, true);

            return true;
        }

        /**
         * Frees the page.
         *
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param release Release write lock and release page.
         */
        private void freePage(long pageId, long page, long pageAddr, boolean release) {
            long effectivePageId = effectivePageId(pageId);

            long recycled = recyclePage(pageId, pageAddr);

            if (effectivePageId != effectivePageId(pageId)) {
                throw new IllegalStateException("Effective page ID must stay the same.");
            }

            if (release) {
                writeUnlockAndClose(pageId, page, pageAddr);
            }

            addFreePage(recycled);
        }

        /**
         * Cuts the root.
         *
         * @param lvl Expected root level.
         * @throws IgniteInternalCheckedException If failed.
         */
        private void cutRoot(int lvl) throws IgniteInternalCheckedException {
            Bool res = write(metaPageId, cutRoot, lvl, FALSE);

            assert res == TRUE : res;
        }

        private void reuseFreePages() throws IgniteInternalCheckedException {
            // If we have a bag, then it will be processed at the upper level.
            if (reuseList != null && freePages != null) {
                reuseList.addForRecycle(this);
            }
        }

        /**
         * Replacing the key in the inner node.
         *
         * @throws IgniteInternalCheckedException If failed.
         */
        private void replaceInner() throws IgniteInternalCheckedException {
            assert needReplaceInner;

            int innerIdx;

            Tail<L> inner = tail;

            for (; ; ) { // Find inner key to replace.
                assert inner.type == Tail.EXACT : inner.type;
                assert inner.lvl > 0 : "leaf " + tail.lvl;

                innerIdx = insertionPoint(inner);

                if (innerIdx >= 0) {
                    break; // Successfully found the inner key.
                }

                // We did not find the inner key to replace.
                if (inner.lvl == 1) {
                    return; // After leaf merge inner page lost inner key, nothing to do here.
                }

                // Go level down.
                inner = inner.down;
            }

            Tail<L> leaf = getTail(inner, 0);

            int leafCnt = leaf.getCount();

            assert leafCnt > 0 : leafCnt; // Leaf must be merged at this point already if it was empty.

            int leafIdx = leafCnt - 1; // Last leaf item.

            // We increment remove ID in write lock on inner page, thus it is guaranteed that
            // any successor, who already passed the inner page, will get greater value at leaf
            // than he had read at the beginning of the operation and will retry operation from root.
            long rmvId = globalRmvId.incrementAndGet();

            // Update inner key with the new biggest key of left subtree.
            inner.io.store(inner.buf, innerIdx, leaf.io, leaf.buf, leafIdx);
            inner.io.setRemoveId(inner.buf, rmvId);

            // Update remove ID for the leaf page.
            leaf.io.setRemoveId(leaf.buf, rmvId);
        }

        /**
         * Tries to merge the left and right child.
         *
         * @param prnt Parent for merge.
         * @return {@code true} If merged, {@code false} if not (because of insufficient space or empty parent).
         * @throws IgniteInternalCheckedException If failed.
         */
        private boolean merge(Tail<L> prnt) throws IgniteInternalCheckedException {
            // If we are merging empty branch this is acceptable because even if we merge
            // two routing pages, one of them is effectively dropped in this merge, so just
            // keep a single routing page.
            if (prnt.getCount() == 0 && needMergeEmptyBranch != READY) {
                return false; // Parent is an empty routing page, child forward page will have another parent.
            }

            Tail<L> left = prnt.getLeftChild();
            Tail<L> right = prnt.getRightChild();

            if (!doMerge(prnt, left, right)) {
                return false;
            }

            // left from BACK becomes EXACT.
            if (left.type == Tail.BACK) {
                assert left.sibling == null;

                left.down = right.down;
                left.type = Tail.EXACT;
                prnt.down = left;
            } else { // left is already EXACT.
                assert left.type == Tail.EXACT : left.type;
                assert left.sibling != null;

                left.sibling = null;
            }

            return true;
        }

        @Override
        boolean isFinished() {
            return row == null;
        }

        /**
         * Releases all.
         *
         * @throws IgniteInternalCheckedException If failed.
         */
        private void releaseAll() throws IgniteInternalCheckedException {
            releaseTail();
            reuseFreePages();
        }

        @Override
        protected Result finishOrLockTail(long pageId, long page, long backId, long fwdId, int lvl)
                throws IgniteInternalCheckedException {
            Result res = finishTail();

            if (res == NOT_FOUND) {
                res = lockTail(pageId, page, backId, fwdId, lvl);
            }

            return res;
        }

        /**
         * Tries to remove from the leaf.
         *
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param backId Back page ID.
         * @param fwdId Forward ID.
         * @param lvl Level.
         * @return Result.
         * @throws IgniteInternalCheckedException If failed.
         */
        private Result tryRemoveFromLeaf(long pageId, long page, long backId, long fwdId, int lvl) throws IgniteInternalCheckedException {
            // We must be at the bottom here, just need to remove row from the current page.
            assert lvl == 0 : lvl;

            Result res = removeFromLeaf(pageId, page, backId, fwdId);

            if (res == FOUND && tail == null) {
                // Finish if we don't need to do any merges.
                finish();
            }

            return res;
        }

        @Override
        public String toString() {
            return "Remove [super=" + super.toString() + ']';
        }
    }

    /**
     * Tail for remove.
     */
    private static final class Tail<L> {
        static final byte BACK = 0;

        static final byte EXACT = 1;

        static final byte FORWARD = 2;

        private final long pageId;

        private final long page;

        private final long buf;

        private final BplusIo<L> io;

        private byte type;

        private final int lvl;

        private short idx = Short.MIN_VALUE;

        /** Only {@link #EXACT} tail can have either {@link #BACK} or {@link #FORWARD} sibling. */
        private @Nullable Tail<L> sibling;

        /** Only {@link #EXACT} tail can point to {@link #EXACT} tail of lower level. */
        private @Nullable Tail<L> down;

        /**
         * Constructor.
         *
         * @param pageId Page ID.
         * @param page Page absolute pointer.
         * @param buf Buffer.
         * @param io IO.
         * @param type Type.
         * @param lvl Level.
         */
        private Tail(long pageId, long page, long buf, BplusIo<L> io, byte type, int lvl) {
            assert type == BACK || type == EXACT || type == FORWARD : type;
            assert lvl >= 0 && lvl <= Byte.MAX_VALUE : lvl;
            assert pageId != 0L;
            assert page != 0L;
            assert buf != 0L;

            this.pageId = pageId;
            this.page = page;
            this.buf = buf;
            this.io = io;
            this.type = type;
            this.lvl = (byte) lvl;
        }

        /**
         * Returns count.
         */
        private int getCount() {
            return io.getCount(buf);
        }

        @Override
        public String toString() {
            return new IgniteStringBuilder("Tail[").app("pageId=").appendHex(pageId).app(", cnt= ").app(getCount())
                    .app(", lvl=" + lvl).app(", sibling=").app(sibling).app("]").toString();
        }

        /**
         * Returns left child.
         */
        private Tail<L> getLeftChild() {
            Tail<L> s = down.sibling;

            return s.type == BACK ? s : down;
        }

        /**
         * Returns right child.
         */
        private Tail<L> getRightChild() {
            Tail<L> s = down.sibling;

            return s.type == FORWARD ? s : down;
        }
    }

    /**
     * Returns insertion point as in {@link Arrays#binarySearch(Object[], Object, Comparator)}.
     *
     * @param io IO.
     * @param buf Buffer.
     * @param low Start index.
     * @param cnt Row count.
     * @param row Lookup row.
     * @param shift Shift if equal.
     * @throws IgniteInternalCheckedException If failed.
     */
    private int findInsertionPoint(
            int lvl,
            BplusIo<L> io,
            long buf,
            int low,
            int cnt,
            @Nullable L row,
            int shift
    ) throws IgniteInternalCheckedException {
        assert row != null;

        if (sequentialWriteOptsEnabled) {
            assert io.getForward(buf, partId) == 0L;

            return -cnt - 1;
        }

        int high = cnt - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;

            int cmp = compare(lvl, io, buf, mid, row);

            if (cmp == 0) {
                cmp = -shift; // We need to fix the case when search row matches multiple data rows.
            }

            //noinspection Duplicates
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid; // Found.
            }
        }

        return -(low + 1);  // Not found.
    }

    /**
     * Returns IO.
     *
     * @param pageAddr Page address.
     * @throws IllegalStateException If failed.
     */
    private BplusIo<L> io(long pageAddr) {
        assert pageAddr != 0;

        int type = PageIo.getType(pageAddr);
        int ver = PageIo.getVersion(pageAddr);

        if (innerIos.getType() == type) {
            return innerIos.forVersion(ver);
        }

        if (leafIos.getType() == type) {
            return leafIos.forVersion(ver);
        }

        throw new IllegalStateException("Unknown page type: " + type + " pageId: " + hexLong(PageIo.getPageId(pageAddr)));
    }

    /**
     * Returns inner page IO.
     *
     * @param io IO.
     */
    private static <L> BplusInnerIo<L> inner(BplusIo<L> io) {
        assert !io.isLeaf();

        return (BplusInnerIo<L>) io;
    }

    /**
     * Returns latest version of inner page IO.
     */
    public final BplusInnerIo<L> latestInnerIo() {
        return innerIos.latest();
    }

    /**
     * Returns the latest version of leaf page IO.
     */
    public final BplusLeafIo<L> latestLeafIo() {
        return leafIos.latest();
    }

    /**
     * Returns the latest version of leaf page IO.
     */
    public final BplusMetaIo latestMetaIo() {
        return metaIos.latest();
    }

    /**
     * Returns comparison result as in {@link Comparator#compare(Object, Object)}.
     *
     * @param io IO.
     * @param pageAddr Page address.
     * @param idx Index of row in the given buffer.
     * @param row Lookup row.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected abstract int compare(BplusIo<L> io, long pageAddr, int idx, @Nullable L row) throws IgniteInternalCheckedException;

    /**
     * Returns comparison result as in {@link Comparator#compare(Object, Object)}.
     *
     * @param lvl Level.
     * @param io IO.
     * @param pageAddr Page address.
     * @param idx Index of row in the given buffer.
     * @param row Lookup row.
     * @throws IgniteInternalCheckedException If failed.
     */
    protected int compare(int lvl, BplusIo<L> io, long pageAddr, int idx, @Nullable L row) throws IgniteInternalCheckedException {
        return compare(io, pageAddr, idx, row);
    }

    /**
     * Get a full detached data row.
     *
     * @param io IO.
     * @param pageAddr Page address.
     * @param idx Index.
     * @return Full detached data row.
     * @throws IgniteInternalCheckedException If failed.
     */
    public final T getRow(BplusIo<L> io, long pageAddr, int idx) throws IgniteInternalCheckedException {
        return getRow(io, pageAddr, idx, null);
    }

    /**
     * Get data row. Can be called on inner page only if {@link #canGetRowFromInner} is {@code true}.
     *
     * @param io IO.
     * @param pageAddr Page address.
     * @param idx Index.
     * @param x Implementation specific argument, {@code null} always means that we need to return full detached data row.
     * @return Data row.
     * @throws IgniteInternalCheckedException If failed.
     */
    public abstract T getRow(BplusIo<L> io, long pageAddr, int idx, @Nullable Object x) throws IgniteInternalCheckedException;

    /**
     * Abstract forward cursor.
     */
    private abstract class AbstractForwardCursor {
        /** Next page ID. */
        long nextPageId;

        /** Lower bound. */
        @Nullable L lowerBound;

        /** Handle multiple equal rows on lower side. */
        private int lowerShift;

        /** Upper bound. */
        @Nullable final L upperBound;

        /** Handle multiple equal rows on upper side. */
        private final int upperShift;

        /** Cached value for retrieving diagnosing info in case of failure. */
        public GetCursor getCursor;

        /**
         * Constructor.
         *
         * @param lowerBound Lower bound.
         * @param upperBound Upper bound.
         * @param lowIncl {@code true} if lower bound is inclusive.
         * @param upIncl {@code true} if upper bound is inclusive.
         */
        AbstractForwardCursor(@Nullable L lowerBound, @Nullable L upperBound, boolean lowIncl, boolean upIncl) {
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;

            lowerShift = lowIncl ? -1 : 1;
            upperShift = upIncl ? 1 : -1;
        }

        /**
         * Initializes a cursor.
         */
        abstract void init0();

        /**
         * Gets rows from page.
         *
         * @param pageAddr Page address.
         * @param io IO.
         * @param startIdx Start index.
         * @param cnt Number of rows in the buffer.
         * @return {@code true} If we were able to fetch rows from this page.
         * @throws IgniteInternalCheckedException If failed.
         */
        abstract boolean fillFromBuffer0(long pageAddr, BplusIo<L> io, int startIdx, int cnt) throws IgniteInternalCheckedException;

        /**
         * Reinitializes the cursor.
         *
         * @return {@code True} If we have rows to return after reading the next page.
         * @throws IgniteInternalCheckedException If failed.
         */
        abstract boolean reinitialize0() throws IgniteInternalCheckedException;

        /**
         * Callback when the rows are not found.
         *
         * @param readDone {@code True} if traversed all rows.
         */
        abstract void onNotFound(boolean readDone);

        /**
         * Initializes a cursor.
         *
         * @param pageAddr Page address.
         * @param io IO.
         * @param startIdx Start index.
         * @throws IgniteInternalCheckedException If failed.
         */
        final void init(long pageAddr, BplusIo<L> io, int startIdx) throws IgniteInternalCheckedException {
            nextPageId = 0;

            init0();

            int cnt = io.getCount(pageAddr);

            // If we see an empty page here, it means that it is an empty tree.
            if (cnt == 0) {
                assert io.getForward(pageAddr, partId) == 0L;

                onNotFound(true);
            } else if (!fillFromBuffer(pageAddr, io, startIdx, cnt)) {
                onNotFound(false);
            }
        }

        /**
         * Returns adjusted to lower bound start index.
         *
         * @param pageAddr Page address.
         * @param io IO.
         * @param cnt Count.
         * @throws IgniteInternalCheckedException If failed.
         */
        final int findLowerBound(long pageAddr, BplusIo<L> io, int cnt) throws IgniteInternalCheckedException {
            assert io.isLeaf();

            // Compare with the first row on the page.
            int cmp = compare(0, io, pageAddr, 0, lowerBound);

            if (cmp < 0 || (cmp == 0 && lowerShift == 1)) {
                int idx = findInsertionPoint(0, io, pageAddr, 0, cnt, lowerBound, lowerShift);

                assert idx < 0;

                return fix(idx);
            }

            return 0;
        }

        /**
         * Returns corrected number of rows with respect to upper bound.
         *
         * @param pageAddr Page address.
         * @param io IO.
         * @param low Start index.
         * @param cnt Number of rows in the buffer.
         * @throws IgniteInternalCheckedException If failed.
         */
        final int findUpperBound(long pageAddr, BplusIo<L> io, int low, int cnt) throws IgniteInternalCheckedException {
            assert io.isLeaf();

            // Compare with the last row on the page.
            int cmp = compare(0, io, pageAddr, cnt - 1, upperBound);

            if (cmp > 0 || (cmp == 0 && upperShift == -1)) {
                int idx = findInsertionPoint(0, io, pageAddr, low, cnt, upperBound, upperShift);

                assert idx < 0;

                cnt = fix(idx);

                nextPageId = 0; // The End.
            }

            return cnt;
        }

        /**
         * Gets rows from page.
         *
         * @param pageAddr Page address.
         * @param io IO.
         * @param startIdx Start index.
         * @param cnt Number of rows in the buffer.
         * @return {@code true} If we were able to fetch rows from this page.
         * @throws IgniteInternalCheckedException If failed.
         */
        private boolean fillFromBuffer(long pageAddr, BplusIo<L> io, int startIdx, int cnt) throws IgniteInternalCheckedException {
            assert io.isLeaf() : io;
            assert cnt != 0 : cnt; // We can not see empty pages (empty tree handled in init).
            assert startIdx >= 0 || startIdx == -1 : startIdx;
            assert cnt >= startIdx;

            checkDestroyed();

            nextPageId = io.getForward(pageAddr, partId);

            return fillFromBuffer0(pageAddr, io, startIdx, cnt);
        }

        /**
         * Tries to find.
         *
         * @throws IgniteInternalCheckedException If failed.
         */
        final void find() throws IgniteInternalCheckedException {
            assert lowerBound != null;

            getCursor = new GetCursor(lowerBound, lowerShift, this);

            doFind(getCursor);
        }

        /**
         * Reinitializes the cursor.
         *
         * @return {@code True} If we have rows to return after reading the next page.
         * @throws IgniteInternalCheckedException If failed.
         */
        private boolean reinitialize() throws IgniteInternalCheckedException {
            // If initially we had no lower bound, then we have to have non-null lastRow argument here
            // (if the tree is empty we never call this method), otherwise we always fallback
            // to the previous lower bound.
            find();

            return reinitialize0();
        }

        /**
         * Reads next page.
         *
         * @param lastRow Last read row (to be used as new lower bound).
         * @return {@code true} If we have rows to return after reading the next page.
         * @throws IgniteInternalCheckedException If failed.
         */
        final boolean nextPage(@Nullable L lastRow) throws IgniteInternalCheckedException {
            checkDestroyed();

            updateLowerBound(lastRow);

            for (; ; ) {
                if (nextPageId == 0) {
                    onNotFound(true);

                    return false; // Done.
                }

                long pageId = nextPageId;
                long page = acquirePage(pageId);
                try {
                    long pageAddr = readLock(pageId, page); // Doing explicit null check.

                    // If concurrent merge occurred we have to reinitialize cursor from the last returned row.
                    if (pageAddr == 0L) {
                        break;
                    }

                    try {
                        BplusIo<L> io = io(pageAddr);

                        if (fillFromBuffer(pageAddr, io, -1, io.getCount(pageAddr))) {
                            return true;
                        }

                        // Continue fetching forward.
                    } finally {
                        readUnlock(pageId, page, pageAddr);
                    }
                } catch (CorruptedDataStructureException e) {
                    throw e;
                } catch (RuntimeException | AssertionError e) {
                    throw corruptedTreeException("Runtime failure on cursor iteration", e, grpId, pageId);
                } finally {
                    releasePage(pageId, page);
                }
            }

            // Reinitialize when `next` is released.
            return reinitialize();
        }

        /**
         * Updates lower bound.
         *
         * @param lower New exact lower bound.
         */
        private void updateLowerBound(@Nullable L lower) {
            if (lower != null) {
                lowerShift = 1; // Now we have the full row an need to avoid duplicates.
                lowerBound = lower; // Move the lower bound forward for further concurrent merge retries.
            }
        }
    }

    /**
     * Closure cursor.
     */
    private final class ClosureCursor extends AbstractForwardCursor {
        /** Row predicate. */
        private final TreeRowClosure<L, T> predicate;

        /** Last row. */
        @Nullable
        private L lastRow;

        /**
         * Constructor.
         *
         * @param lowerBound Lower bound inclusive.
         * @param upperBound Upper bound inclusive.
         * @param predicate Row predicate.
         */
        ClosureCursor(L lowerBound, L upperBound, TreeRowClosure<L, T> predicate) {
            super(lowerBound, upperBound, true, true);

            this.predicate = predicate;
        }

        @Override
        void init0() {
            // No-op.
        }

        @Override
        boolean fillFromBuffer0(long pageAddr, BplusIo<L> io, int startIdx, int cnt) throws IgniteInternalCheckedException {
            if (startIdx == -1) {
                startIdx = findLowerBound(pageAddr, io, cnt);
            }

            if (cnt == startIdx) {
                return false;
            }

            for (int i = startIdx; i < cnt; i++) {
                int cmp = compare(0, io, pageAddr, i, upperBound);

                if (cmp > 0) {
                    nextPageId = 0; // The End.

                    return false;
                }

                boolean stop = !predicate.apply(BplusTree.this, io, pageAddr, i);

                if (stop) {
                    nextPageId = 0; // The End.

                    return true;
                }
            }

            if (nextPageId != 0) {
                lastRow = io.getLookupRow(BplusTree.this, pageAddr, cnt - 1); // Need save last row.
            }

            return true;
        }

        @Override
        boolean reinitialize0() {
            return true;
        }

        @Override
        void onNotFound(boolean readDone) {
            nextPageId = 0;
        }

        /**
         * Iterates over the tree.
         *
         * @throws IgniteInternalCheckedException If failed.
         */
        private void iterate() throws IgniteInternalCheckedException {
            find();

            if (nextPageId == 0) {
                return;
            }

            for (; ; ) {
                L lastRow0 = lastRow;

                lastRow = null;

                nextPage(lastRow0);

                if (nextPageId == 0) {
                    return;
                }
            }
        }
    }

    /**
     * Forward cursor.
     */
    private final class ForwardCursor<R> extends AbstractForwardCursor implements Cursor<R> {
        /** Implementation specific argument. */
        private final @Nullable Object arg;

        /** {@code null} array means the end of iteration over the cursor. */
        private @Nullable R @Nullable [] results = (R[]) OBJECT_EMPTY_ARRAY;

        private @Nullable T lastRow;

        /** Row index. */
        private int row = -1;

        /** Filter closure. */
        private final @Nullable TreeRowMapClosure<L, T, R> treeRowClosure;

        private @Nullable Boolean hasNext = null;

        /**
         * Lower unbound cursor.
         *
         * @param upperBound Upper bound.
         * @param upIncl {@code true} if upper bound is inclusive.
         * @param treeRowClosure Tree row closure.
         * @param arg Implementation specific argument, {@code null} always means that we need to return full detached data row.
         */
        ForwardCursor(
                @Nullable L upperBound,
                boolean upIncl,
                @Nullable TreeRowMapClosure<L, T, R> treeRowClosure,
                @Nullable Object arg
        ) {
            this(null, upperBound, true, upIncl, treeRowClosure, arg);
        }

        /**
         * Constructor.
         *
         * @param lowerBound Lower bound.
         * @param upperBound Upper bound.
         * @param lowIncl {@code true} if lower bound is inclusive.
         * @param upIncl {@code true} if upper bound is inclusive.
         * @param treeRowClosure Tree row closure.
         * @param arg Implementation specific argument, {@code null} always means that we need to return full detached data row.
         */
        ForwardCursor(
                @Nullable L lowerBound,
                @Nullable L upperBound,
                boolean lowIncl,
                boolean upIncl,
                @Nullable TreeRowMapClosure<L, T, R> treeRowClosure,
                @Nullable Object arg
        ) {
            super(lowerBound, upperBound, lowIncl, upIncl);

            this.treeRowClosure = treeRowClosure;
            this.arg = arg;
        }

        @Override
        boolean fillFromBuffer0(long pageAddr, BplusIo<L> io, int startIdx, int cnt) throws IgniteInternalCheckedException {
            if (startIdx == -1) {
                if (lowerBound != null) {
                    startIdx = findLowerBound(pageAddr, io, cnt);
                } else {
                    startIdx = 0;
                }
            }

            if (upperBound != null && cnt != startIdx) {
                cnt = findUpperBound(pageAddr, io, startIdx, cnt);
            }

            int cnt0 = cnt - startIdx;

            if (cnt0 == 0) {
                return false;
            }

            if (results == OBJECT_EMPTY_ARRAY) {
                results = (R[]) new Object[cnt0];
            }

            int resCnt = 0;

            for (int idx = startIdx; idx < cnt; idx++) {
                if (treeRowClosure == null || treeRowClosure.apply(BplusTree.this, io, pageAddr, idx)) {
                    T treeRow = getRow(io, pageAddr, idx, arg);

                    R result = treeRowClosure != null ? treeRowClosure.map(treeRow) : (R) treeRow;

                    results = set(results, resCnt++, result);

                    lastRow = treeRow;
                }
            }

            if (resCnt == 0) {
                results = (R[]) OBJECT_EMPTY_ARRAY;

                return false;
            }

            clearTail(results, resCnt);

            return true;
        }

        @Override
        boolean reinitialize0() {
            hasNext = null;

            return hasNext();
        }

        @Override
        void onNotFound(boolean readDone) {
            if (readDone) {
                results = null;
            } else {
                if (results != OBJECT_EMPTY_ARRAY) {
                    assert results.length > 0; // Otherwise it makes no sense to create an array.

                    // Fake clear.
                    results[0] = null;
                }
            }
        }

        @Override
        void init0() {
            row = -1;
        }

        @Override
        public boolean hasNext() {
            if (results == null) {
                return false;
            }

            if (hasNext == null) {
                hasNext = advance();
            }

            return hasNext;
        }

        /**
         * Returns cleared last row.
         */
        private void clearLastResult() {
            if (row == 0) {
                return;
            }

            int last = row - 1;

            assert results[last] != null;

            results[last] = null;
        }

        @Override
        public R next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            R r = results[row];

            assert r != null;

            hasNext = null;

            return r;
        }

        private boolean advance() {
            if (++row < results.length && results[row] != null) {
                clearLastResult(); // Allow to GC the last returned row.

                return true;
            }

            clearLastResult();

            row = 0;

            T lastRow = this.lastRow;

            this.lastRow = null;

            try {
                return nextPage(lastRow);
            } catch (IgniteInternalCheckedException e) {
                // Can't throw checked exceptions from existing iterator API.
                throw new IgniteInternalException(Common.INTERNAL_ERR, "Unable to read the next page", e);
            }
        }

        @Override
        public void close() {
            // No-op.
        }
    }

    /**
     * Class for getting the next row.
     */
    private final class GetNext extends Get {
        @Nullable
        private T nextRow;

        private GetNext(L row, boolean includeRow) {
            super(row, false);

            shift = includeRow ? -1 : 1;
        }

        @Override
        boolean found(BplusIo<L> io, long pageAddr, int idx, int lvl) {
            // Must never be called because we always have a shift.
            throw new IllegalStateException();
        }

        @Override
        boolean notFound(BplusIo<L> io, long pageAddr, int idx, int lvl) throws IgniteInternalCheckedException {
            if (lvl != 0) {
                return false;
            }

            int cnt = io.getCount(pageAddr);

            if (cnt == 0) {
                // Empty tree.
                assert io.getForward(pageAddr, partId) == 0L;
            } else {
                assert io.isLeaf() : io;
                assert cnt > 0 : cnt;
                assert idx >= 0 : idx;
                assert cnt >= idx : "cnt=" + cnt + ", idx=" + idx;

                checkDestroyed();

                if (idx < cnt) {
                    nextRow = getRow(io, pageAddr, idx);
                }
            }

            return true;
        }
    }

    /**
     * Page handler for basic {@link Get} operation.
     */
    private abstract class GetPageHandler<G extends Get> implements PageHandler<G, Result> {
        @Override
        public Result run(
                int groupId,
                long pageId,
                long page,
                long pageAddr,
                PageIo iox,
                G g,
                int lvl
        ) throws IgniteInternalCheckedException {
            assert PageIo.getPageId(pageAddr) == pageId
                    : "pageId mismatch [requested=" + pageId + ", stored=" + PageIo.getPageId(pageAddr) + "]";

            // If we've passed the check for correct page ID, we can safely cast.
            BplusIo<L> io = (BplusIo<L>) iox;

            // In case of intersection with inner replace in remove operation
            // we need to restart our operation from the tree root.
            if (lvl == 0 && g.rmvId < io.getRemoveId(pageAddr)) {
                return RETRY_ROOT;
            }

            return run0(pageId, page, pageAddr, io, g, lvl);
        }

        /**
         * Handles the page.
         *
         * @param pageId Page ID.
         * @param page Page pointer.
         * @param pageAddr Page address.
         * @param io IO.
         * @param g Operation.
         * @param lvl Level.
         * @return Result code.
         * @throws IgniteInternalCheckedException If failed.
         */
        protected abstract Result run0(
                long pageId,
                long page,
                long pageAddr,
                BplusIo<L> io,
                G g,
                int lvl
        ) throws IgniteInternalCheckedException;

        @Override
        public boolean releaseAfterWrite(int cacheId, long pageId, long page, long pageAddr, G g, int lvl) {
            return g.canRelease(pageId, lvl);
        }
    }

    /**
     * Tree meta data.
     */
    private static class TreeMetaData {
        /** Root level. */
        final int rootLvl;

        /** Root page ID. */
        final long rootId;

        /**
         * Constructor.
         *
         * @param rootLvl Root level.
         * @param rootId Root page ID.
         */
        TreeMetaData(int rootLvl, long rootId) {
            this.rootLvl = rootLvl;
            this.rootId = rootId;
        }

        @Override
        public String toString() {
            return S.toString(TreeMetaData.class, this);
        }
    }

    /**
     * Operation result.
     */
    public enum Result {
        GO_DOWN,

        GO_DOWN_X,

        FOUND,

        NOT_FOUND,

        RETRY,

        RETRY_ROOT
    }

    /**
     * Four state boolean.
     */
    enum Bool {
        FALSE,

        TRUE,

        READY,

        DONE
    }

    /**
     * A generic visitor-style interface for performing filtering/modifications/miscellaneous operations on the tree.
     */
    @FunctionalInterface
    public interface TreeRowClosure<L, T extends L> {
        /**
         * Performs inspection or operation on a specified row and returns true if this row is
         * required or matches or /operation successful (depending on the context).
         *
         * @param tree The tree.
         * @param io Th tree IO object.
         * @param pageAddr The page address.
         * @param idx The item index.
         * @return {@code True} if the item passes the predicate.
         * @throws IgniteInternalCheckedException If failed.
         */
        boolean apply(BplusTree<L, T> tree, BplusIo<L> io, long pageAddr, int idx) throws IgniteInternalCheckedException;
    }

    /**
     * Extension of the {@link TreeRowClosure} with the ability to {@link #map(Object) convert} tree row to some object.
     */
    public interface TreeRowMapClosure<L, T extends L, R> extends TreeRowClosure<L, T> {
        @Override
        default boolean apply(BplusTree<L, T> tree, BplusIo<L> io, long pageAddr, int idx) throws IgniteInternalCheckedException {
            return true;
        }

        /**
         * Converts a tree row to some object.
         *
         * <p>Executed after {@link #apply} has returned {@code true}, and also under read lock of page on which the tree row is located.
         *
         * @param treeRow Tree row.
         */
        default @Nullable R map(@Nullable T treeRow) {
            return (R) treeRow;
        }
    }

    /**
     * A generic visitor-style interface for performing inspection/modification operations on the tree.
     */
    public interface TreeVisitorClosure<L, T extends L> {
        int STOP = 0x01;

        int CAN_WRITE = STOP << 1;

        int DIRTY = CAN_WRITE << 1;

        /**
         * Performs inspection or operation on a specified row.
         *
         * @param tree The tree.
         * @param io Th tree IO object.
         * @param pageAddr The page address.
         * @param idx The item index.
         * @return state bitset.
         * @throws IgniteInternalCheckedException If failed.
         */
        int visit(BplusTree<L, T> tree, BplusIo<L> io, long pageAddr, int idx) throws IgniteInternalCheckedException;

        /**
         * Returns state bitset.
         */
        int state();
    }

    /**
     * Returns number of retries.
     */
    protected int getLockRetries() {
        return LOCK_RETRIES;
    }

    /**
     * PageIds converter with empty check.
     *
     * @param empty Flag for empty array result.
     * @param pages Pages supplier.
     * @return Array of page ids.
     */
    private static long[] pages(boolean empty, Supplier<long[]> pages) {
        return empty ? LongArrays.EMPTY_ARRAY : pages.get();
    }

    /**
     * Construct the exception and invoke failure processor.
     *
     * @param msg Message.
     * @param cause Cause.
     * @param grpId Group id.
     * @param pageIds Pages ids.
     * @return New CorruptedTreeException instance.
     */
    protected CorruptedTreeException corruptedTreeException(String msg, Throwable cause, int grpId, long... pageIds) {
        return new CorruptedTreeException(msg, cause, grpName, grpId, pageIds);
    }

    /**
     * Returns meta page id.
     */
    public long getMetaPageId() {
        return metaPageId;
    }

    /**
     * Create an error message when reaching the maximum number of repetitions to capture a lock in the B+Tree.
     *
     * @param op Operation name, for example: GET, PUT.
     * @return Error message.
     */
    protected String lockRetryErrorMessage(String op) {
        return "Maximum number of retries "
                + getLockRetries() + " reached for " + op + " operation "
                + "(the tree may be corrupted). Increase " + IGNITE_BPLUS_TREE_LOCK_RETRIES + " system property "
                + "if you regularly see this message (current value is " + getLockRetries() + "). "
                + getClass().getSimpleName() + " [grpName=" + grpName + ", treeName=" + name() + ", metaPageId="
                + hexLong(metaPageId) + "].";
    }

    private static class RootPageIdAndLevel {
        private final long pageId;
        private final int level;

        private RootPageIdAndLevel(long pageId, int level) {
            this.pageId = pageId;
            this.level = level;
        }
    }

    private class DestroyTreeTask implements GradualTask {
        private final LongListReuseBag bag;
        private final @Nullable Consumer<L> actOnEachElement;
        private final int rootLevel;
        private final int maxWorkUnits;

        /** IDs of pages contained in inner pages on each level. First index is level. */
        private final long[][] childrenPageIds;

        /** Indices of current page ID (among {@link #childrenPageIds}) on each level. Indexed by level. */
        private final int[] currentChildIndices;

        /** Level on which we currently are. */
        private int currentLevel;

        /** ID of the page that we are going to process (that is, recycle it after reading its contents) next. */
        private long currentPageId;

        private boolean finished = false;

        private DestroyTreeTask(
                LongListReuseBag bag,
                @Nullable Consumer<L> actOnEachElement,
                int rootLevel,
                long rootPageId,
                int maxWorkUnits
        ) {
            this.bag = bag;
            this.actOnEachElement = actOnEachElement;
            this.rootLevel = rootLevel;
            this.maxWorkUnits = maxWorkUnits;

            childrenPageIds = new long[rootLevel + 1][];
            currentChildIndices = new int[rootLevel + 1];

            currentLevel = rootLevel;
            currentPageId = rootPageId;
        }

        @Override
        public void runStep() throws Exception {
            destroyNextBatch();

            if (finished) {
                addForRecycle(bag);
            }
        }

        private void destroyNextBatch() throws IgniteInternalCheckedException {
            // Recycling of a node counts as 1 work unit; also, visiting an item
            // using a Consumer also counts as 1 work unit per item.
            int workDone = 0;

            while (!finished && workDone < maxWorkUnits) {
                long pageId = currentPageId;

                long page = acquirePage(pageId);

                try {
                    long pageAddr = writeLock(pageId, page);

                    if (pageAddr == 0L) {
                        // This page was possibly recycled, but we still need to destroy the rest of the tree.
                        workDone++;

                        positionToNextPageId();

                        continue;
                    }

                    try {
                        BplusIo<L> io = io(pageAddr);

                        if (io.isLeaf() != (currentLevel == 0)) {
                            // Leaf pages only at the level 0.
                            fail("Leaf level mismatch: " + currentLevel);
                        }

                        int cnt = io.getCount(pageAddr);

                        if (cnt < 0) {
                            fail("Negative count: " + cnt);
                        }

                        if (!io.isLeaf()) {
                            readChildrenPageIdsAndDescend(pageAddr, io, cnt);
                        } else {
                            if (actOnEachElement != null) {
                                io.visit(BplusTree.this, pageAddr, actOnEachElement);

                                workDone += io.getCount(pageAddr);
                            }

                            positionToNextPageId();
                        }

                        bag.addFreePage(recyclePage(pageId, pageAddr));

                    } finally {
                        writeUnlock(pageId, page, pageAddr, true);
                    }
                } finally {
                    releasePage(pageId, page);
                }

                workDone++;

                if (bag.size() >= 128) {
                    addForRecycle(bag);
                }
            }
        }

        private void readChildrenPageIdsAndDescend(long pageAddr, BplusIo<L> io, int cnt) {
            long[] pageIds = new long[cnt + 1];

            // When i == cnt it is the same as io.getRight(cnt - 1) but works for routing pages.
            for (int i = 0; i <= cnt; i++) {
                long leftId = inner(io).getLeft(pageAddr, i, partId);

                inner(io).setLeft(pageAddr, i, 0);

                pageIds[i] = leftId;
            }

            currentLevel--;
            childrenPageIds[currentLevel] = pageIds;
            currentChildIndices[currentLevel] = 0;
            currentPageId = childrenPageIds[currentLevel][currentChildIndices[currentLevel]];
        }

        /**
         * Either positions {@link #currentPageId} to next ID that should be processed, or set {@link #finished} to {@code true}.
         */
        private void positionToNextPageId() {
            while (currentLevel < rootLevel) {
                if (currentChildIndices[currentLevel] + 1 < childrenPageIds[currentLevel].length) {
                    // We can go right.
                    currentChildIndices[currentLevel]++;
                    currentPageId = childrenPageIds[currentLevel][currentChildIndices[currentLevel]];

                    return;
                } else {
                    // Go up and try going right again.
                    currentLevel++;
                }
            }

            // We were not able to find any more tree nodes, so our work is over.
            finished = true;
        }

        @Override
        public boolean isCompleted() {
            return finished;
        }
    }
}
