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

import static org.apache.ignite.internal.lang.IgniteSystemProperties.getFloat;
import static org.apache.ignite.internal.util.GridUnsafe.getInt;
import static org.apache.ignite.internal.util.GridUnsafe.getLong;
import static org.apache.ignite.internal.util.GridUnsafe.putInt;
import static org.apache.ignite.internal.util.GridUnsafe.putLong;
import static org.apache.ignite.internal.util.IgniteUtils.safeAbs;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.mem.IgniteOutOfMemoryException;
import org.apache.ignite.internal.util.GridUnsafe;

/**
 * Loaded pages mapping to relative pointer based on Robin Hood hashing: backward shift deletion algorithm.
 *
 * <p>Performance of initial Robin Hood hashing could be greatly improved with only a little change to the removal method.
 *
 * <p>Instead of replacing entries with 'Removed' fake entries on deletion, backward shift deletion variant for the Robin Hood hashing
 * algorithm does shift backward all the entries following the entry to delete until either an empty bucket, or a bucket with a DIB of 0
 * (distance to initial bucket).
 *
 * <p>Every deletion will shift backwards entries and therefore decrease their respective DIBs by 1 (all their initial DIB values would be
 * >=1).
 *
 * <p>This implementation stores ideal bucket with entry value itself.
 */
public class RobinHoodBackwardShiftHashMap implements LoadedPagesMap {
    /** Long-long offheap map load factor. */
    // TODO: IGNITE-16350 Move to config or something else.
    public static final String IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR = "IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR";

    /** Load factor. */
    private static final float LOAD_FACTOR = getFloat(IGNITE_LONG_LONG_HASH_MAP_LOAD_FACTOR, 2.5f);

    /** Size of count of entries (value returned by size() method). */
    private static final int MAPSIZE_SIZE = 4;

    /** Padding to provide read/write from word beginning for each cell. Change this to 0 if padding is not required. */
    private static final int CELL_PADDING = 4;

    /** Padding to provide read/write from word beginning. Change this to 0 if padding is not required. */
    private static final int MAPSIZE_PADDING = 4;

    /** Count of entries offset starting from base address. */
    private static final int MAPSIZE_OFFSET = 0;

    /** Size of initial/ideal bucket (cell to store value to avoid probing other cells followed). */
    private static final int IDEAL_BUCKET_SIZE = 4;

    /** Offset of initial/ideal bucket starting from entry base. */
    private static final int IDEAL_BUCKET_OFFSET = 0;

    /** Group ID size. */
    private static final int GRP_ID_SIZE = 4;

    /** Group ID offset from entry base. */
    private static final int GRP_ID_OFFSET = IDEAL_BUCKET_OFFSET + IDEAL_BUCKET_SIZE;

    /** Page ID size. */
    private static final int PAGE_ID_SIZE = 8;

    /** Page ID offset from entry base. */
    private static final int PAGE_ID_OFFSET = GRP_ID_OFFSET + GRP_ID_SIZE;

    /** Value size. */
    private static final int VALUE_SIZE = 8;

    /** Value offset from entry base. */
    private static final int VALUE_OFFSET = PAGE_ID_OFFSET + PAGE_ID_SIZE;

    /** Version (tag/generation) offset from entry base. */
    private static final int VERSION_SIZE = 4;

    /** Version (tag/generation) offset from entry base. */
    private static final int VERSION_OFFSET = VALUE_OFFSET + VALUE_SIZE;

    /** Page ID used for empty bucket. */
    private static final long EMPTY_PAGE_ID = 0;

    /** Group ID used for empty bucket. */
    private static final int EMPTY_GRP_ID = 0;

    /** Bytes required for storing one entry (cell). */
    private static final int BYTES_PER_CELL = IDEAL_BUCKET_SIZE
            + GRP_ID_SIZE + PAGE_ID_SIZE
            + VALUE_SIZE + VERSION_SIZE
            + CELL_PADDING;

    /** Number of buckets, indicates range of scan memory, max probe count and maximum map size. */
    private final int numBuckets;

    /** Base address of map content. */
    private long baseAddr;

    /**
     * Returns estimated memory size required for this map to store the given number of elements.
     *
     * @param elementsCnt Maximum elements can be stored in map, its maximum size.
     */
    public static long requiredMemory(long elementsCnt) {
        float loadFactor = LOAD_FACTOR;

        assert loadFactor != 0;

        return requiredMemoryByBuckets((long) (elementsCnt * loadFactor));
    }

    /**
     * Returns required size to allocate, based on number of buckets (cells) to store in map, its capacity.
     *
     * @param numBuckets Number of buckets (cells) to store, capacity.
     */
    static long requiredMemoryByBuckets(long numBuckets) {
        return numBuckets * BYTES_PER_CELL + MAPSIZE_SIZE + MAPSIZE_PADDING;
    }

    /**
     * Creates map in preallocated unsafe memory segment.
     *
     * @param baseAddr Base buffer address.
     * @param size Size available for map, number of buckets (cells) to store will be determined accordingly.
     */
    public RobinHoodBackwardShiftHashMap(long baseAddr, long size) {
        this.numBuckets = (int) ((size - MAPSIZE_SIZE - MAPSIZE_PADDING) / BYTES_PER_CELL);
        this.baseAddr = baseAddr;

        GridUnsafe.setMemory(baseAddr, size, (byte) 0);
    }

    /**
     * Returns base cell (bucket) address in buffer.
     *
     * @param idx cell index.
     */
    private long entryBase(int idx) {
        assert idx >= 0 && idx < numBuckets : "idx=" + idx + ", numBuckets=" + numBuckets;

        return baseAddr + MAPSIZE_SIZE + MAPSIZE_PADDING + (long) idx * BYTES_PER_CELL;
    }

    /** {@inheritDoc} */
    @Override
    public long get(int grpId, long pageId, int reqVer, long absent, long outdated) {
        assert grpId != EMPTY_GRP_ID;

        // initial index is also ideal for searhed element
        int idxInit = safeAbs(FullPageId.hashCode(grpId, pageId)) % numBuckets;

        for (int i = 0; i < numBuckets; i++) {
            int idxCurr = (idxInit + i) % numBuckets;

            final long base = entryBase(idxCurr);
            final int distanceFromInit = distance(idxCurr, idxInit);

            final int curGrpId = getGrpId(base);
            final long curPageId = getPageId(base);

            final int dibCurEntry = distance(idxCurr, getIdealBucket(base));

            if (isEmpty(curGrpId, curPageId)) {
                return absent;
            } else if (curGrpId == grpId && curPageId == pageId) {
                // Equal value found
                long actualVer = getVersion(base);
                boolean freshVal = actualVer >= reqVer;

                return freshVal ? getValue(base) : outdated;
            } else if (dibCurEntry < distanceFromInit) {
                // Current entry has quite good position, it would be swapped at hypothetical insert of current value
                return absent;
            }
        }

        return absent;
    }

    /** {@inheritDoc} */
    @Override
    public void put(int grpId, long pageId, long val, int ver) {
        assert grpId != 0;

        // initial index is also ideal for inserted element
        int idxInit = safeAbs(FullPageId.hashCode(grpId, pageId)) % numBuckets;

        int swapCnt = 0;

        int grpIdToInsert = grpId;
        long pageIdToInsert = pageId;
        long valToInsert = val;
        int verToInsert = ver;
        int idxIdealToInsert = idxInit;

        for (int i = 0; i < numBuckets; i++) {
            int idxCurr = (idxInit + i) % numBuckets;

            final long base = entryBase(idxCurr);
            final int dibEntryToInsert = distance(idxCurr, idxInit);

            final int curGrpId = getGrpId(base);
            final long curPageId = getPageId(base);
            final int curIdealBucket = getIdealBucket(base);
            final long curVal = getValue(base);
            final int curVer = getVersion(base);
            final int dibCurEntry = distance(idxCurr, curIdealBucket);

            if (isEmpty(curGrpId, curPageId)) {
                setCellValue(base, idxIdealToInsert, grpIdToInsert, pageIdToInsert, valToInsert, verToInsert);

                setSize(size() + 1);

                return;
            } else if (curGrpId == grpIdToInsert && curPageId == pageIdToInsert) {
                if (swapCnt != 0) {
                    throw new IllegalStateException("Swapped " + swapCnt + " times. Entry: " + dumpEntry(idxCurr));
                }

                setValue(base, valToInsert);

                return; // Equal value found
            } else if (dibCurEntry < dibEntryToInsert) {
                // Swapping *toInsert and state in bucket: save cur state to bucket
                setCellValue(base, idxIdealToInsert, grpIdToInsert, pageIdToInsert, valToInsert, verToInsert);

                idxIdealToInsert = curIdealBucket;
                pageIdToInsert = curPageId;
                grpIdToInsert = curGrpId;
                valToInsert = curVal;
                verToInsert = curVer;

                swapCnt++;
            }
        }

        // no free space left
        throw new IgniteOutOfMemoryException("No room for a new key");
    }

    /** {@inheritDoc} */
    @Override
    public boolean remove(int grpId, long pageId) {
        assert grpId != EMPTY_GRP_ID;

        int idxInit = safeAbs(FullPageId.hashCode(grpId, pageId)) % numBuckets;

        int idxEqualValFound = -1;
        for (int i = 0; i < numBuckets; i++) {
            int idxCurr = (idxInit + i) % numBuckets;

            final long base = entryBase(idxCurr);
            final int dibEntryToInsert = distance(idxCurr, idxInit);

            final int curGrpId = getGrpId(base);
            final long curPageId = getPageId(base);
            final int curIdealBucket = getIdealBucket(base);
            final int dibCurEntry = distance(idxCurr, curIdealBucket);

            if (isEmpty(curGrpId, curPageId)) {
                return false;
            } else if (curGrpId == grpId && curPageId == pageId) {
                idxEqualValFound = idxCurr;

                break; // Equal value found.
            } else if (dibCurEntry < dibEntryToInsert) {
                // If our value was present in map we had already found it.
                return false;
            }
        }

        setSize(size() - 1);

        doBackwardShift(idxEqualValFound);

        return true;
    }

    /**
     * Runs backward shifts from current index to .
     *
     * @param idxRmv removed index.
     */
    private void doBackwardShift(int idxRmv) {
        assert idxRmv >= 0;

        // Scanning rest of map to perform backward shifts.
        for (int i = 0; i < numBuckets - 1; i++) {
            int idxCurr = (idxRmv + i) % numBuckets;
            int idxNext = (idxRmv + i + 1) % numBuckets;

            long baseCurr = entryBase(idxCurr);

            long baseNext = entryBase(idxNext);
            final int nextGrpId = getGrpId(baseNext);
            final long nextPageId = getPageId(baseNext);
            final int nextIdealBucket = getIdealBucket(baseNext);
            final int nextEntryVer = getVersion(baseNext);

            if (isEmpty(nextGrpId, nextPageId)
                    || distance(idxNext, nextIdealBucket) == 0) {
                setEmpty(baseCurr);

                return;
            } else {
                setCellValue(baseCurr, nextIdealBucket, nextGrpId, nextPageId, getValue(baseNext), nextEntryVer);
            }
        }

        int lastShiftedIdx = (idxRmv - 1) % numBuckets;

        if (lastShiftedIdx < 0) {
            lastShiftedIdx += numBuckets;
        }

        setEmpty(entryBase(lastShiftedIdx));
    }

    /** {@inheritDoc} */
    @Override
    public ReplaceCandidate getNearestAt(final int idxStart) {
        for (int i = 0; i < numBuckets; i++) {
            int idxCurr = (idxStart + i) % numBuckets;

            if (isEmptyAt(idxCurr)) {
                continue;
            }

            long base = entryBase(idxCurr);

            return new ReplaceCandidate(getVersion(base), getValue(base), getFullPageId(base));
        }

        return null;
    }

    /**
     * Returns {@code true} if value is not provided in cell having index.
     *
     * @param idx Index to test.
     */
    private boolean isEmptyAt(int idx) {
        long base = entryBase(idx);

        return isEmpty(getGrpId(base), getPageId(base));
    }

    /** {@inheritDoc} */
    @Override
    public long refresh(int grpId, long pageId, int ver) {
        assert grpId != EMPTY_GRP_ID;

        int idxInit = safeAbs(FullPageId.hashCode(grpId, pageId)) % numBuckets;

        for (int i = 0; i < numBuckets; i++) {
            int idxCurr = (idxInit + i) % numBuckets;

            final long base = entryBase(idxCurr);
            final int distanceFromInit = distance(idxCurr, idxInit);

            final int curGrpId = getGrpId(base);
            final long curPageId = getPageId(base);
            final int curIdealBucket = getIdealBucket(base);
            final int dibCurEntry = distance(idxCurr, curIdealBucket);

            if (isEmpty(curGrpId, curPageId)) {
                break; // break to fail
            } else if (curGrpId == grpId && curPageId == pageId) {
                // Equal value found.
                long actualVer = getVersion(base);

                boolean freshVal = actualVer >= ver;

                if (freshVal) {
                    throw new IllegalArgumentException("Fresh element found at "
                            + dumpEntry(idxCurr) + " during search of cell to refresh. "
                            + "Refresh should be called for existent outdated element. ");
                }

                setVersion(base, ver);

                return getValue(base);
            } else if (dibCurEntry < distanceFromInit) {
                // Current entry has quite good position, it would be swapped at hypothetical insert of current value.

                break;
            }
        }

        throw new IllegalArgumentException("Element not found group ID: " + grpId + ", page ID: " + pageId
                + " during search of cell to refresh. Refresh should be called for existent outdated element. ");
    }

    /** {@inheritDoc} */
    @Override
    public LongArrayList removeIf(int startIdxToClear, int endIdxToClear, KeyPredicate keyPred) {
        assert endIdxToClear >= startIdxToClear
                : "Start and end indexes are not consistent: {" + startIdxToClear + ", " + endIdxToClear + "}";

        int sz = endIdxToClear - startIdxToClear;

        LongArrayList list = new LongArrayList(sz);

        for (int idx = startIdxToClear; idx < endIdxToClear; idx++) {
            long base = entryBase(idx);

            int grpId = getGrpId(base);
            long pageId = getPageId(base);

            if (isEmpty(grpId, pageId)) {
                continue; // absent value, no removal required
            }

            if (!keyPred.test(grpId, pageId)) {
                continue; // not matched value, no removal required
            }

            long valAt = getValue(base);

            setSize(size() - 1);

            doBackwardShift(idx);

            list.add(valAt);

            idx--; // Need recheck current cell because of backward shift.
        }

        return list;
    }

    /** {@inheritDoc} */
    @Override
    public int capacity() {
        return numBuckets;
    }

    /**
     * Returns distance between cells, or 0 if cell is ideal.
     *
     * @param curr current selected index to store value.
     * @param baseIdx base or ideal bucket to store entry value to avoid probing.
     */
    private int distance(int curr, int baseIdx) {
        int diff = curr - baseIdx;

        if (diff < 0) {
            return diff + numBuckets;
        }

        return diff;
    }

    /**
     * Returns {@code true} if group & page id indicates cell has state 'Empty'.
     *
     * @param grpId Group ID.
     * @param pageId Page ID.
     */
    private boolean isEmpty(int grpId, long pageId) {
        return pageId == EMPTY_PAGE_ID && grpId == EMPTY_GRP_ID;
    }

    /**
     * Sets cell value to be empty.
     *
     * @param addr entry base address.
     */
    private void setEmpty(long addr) {
        setPageId(addr, EMPTY_PAGE_ID);
        setGrpId(addr, EMPTY_GRP_ID);
        setValue(addr, 0);
        setIdealBucket(addr, 0);
        setVersion(addr, 0);
    }

    /**
     * Sets number of ideal bucket (cell) to store this value.
     *
     * @param base Entry base, address in buffer of the entry start.
     * @param idxIdeal Number of ideal bucket (cell) to insert this value.
     */
    private void setIdealBucket(long base, int idxIdeal) {
        assert idxIdeal >= 0 && idxIdeal < numBuckets;

        putInt(base + IDEAL_BUCKET_OFFSET, idxIdeal);
    }

    /**
     * Returns printable dump with all buckets state.
     */
    public String dump() {
        StringBuilder sb = new StringBuilder();

        for (int idx = 0; idx < numBuckets; idx++) {
            dumpEntry(sb, idx);
        }

        return sb.toString();
    }

    /**
     * Returns string representation of bucket content.
     *
     * @param idx index of entry to dump
     */
    private String dumpEntry(int idx) {
        StringBuilder sb = new StringBuilder();

        dumpEntry(sb, idx);

        return sb.toString();
    }

    /**
     * Dumps entry.
     *
     * @param sb Destination string builder to dump entry to.
     * @param idx Bucket index.
     */
    private void dumpEntry(StringBuilder sb, int idx) {
        long base = entryBase(idx);
        int curGrpId = getGrpId(base);
        long curPageId = getPageId(base);
        long curVal = getValue(base);
        long ver = getVersion(base);

        sb.append("slot [").append(idx).append("]:");

        if (isEmpty(curGrpId, curPageId)) {
            sb.append("Empty: ");
        }

        sb.append("i.buc=").append(getIdealBucket(base)).append(',')
                .append("(grp=").append(curGrpId).append(',')
                .append("page=").append(curPageId).append(')')
                .append("->")
                .append("(val=").append(curVal).append(',')
                .append("ver=").append(ver).append(')')
                .append('\n');
    }

    /**
     * Sets cell value.
     *
     * @param base Entry base, address in buffer of the entry start.
     * @param idealBucket Number of ideal bucket (cell) to insert this value.
     * @param grpId Entry key. Group ID to be stored in entry.
     * @param pageId Entry key. Page ID to be stored.
     * @param val Entry value associated with key.
     * @param ver Entry version.
     */
    private void setCellValue(long base, int idealBucket, int grpId, long pageId, long val, int ver) {
        setIdealBucket(base, idealBucket);
        setGrpId(base, grpId);
        setPageId(base, pageId);
        setValue(base, val);
        setVersion(base, ver);
    }

    /**
     * Returns number of ideal bucket (cell) to store this value.
     *
     * @param base address of current cell.
     */
    private int getIdealBucket(long base) {
        return getInt(base + IDEAL_BUCKET_OFFSET);
    }

    /**
     * Returns page ID saved in cell.
     *
     * @param base Address of current cell.
     */
    private long getPageId(long base) {
        return getLong(base + PAGE_ID_OFFSET);
    }

    /**
     * Sets page ID.
     *
     * @param base Address of cell.
     * @param pageId Page ID to set in current cell.
     */
    private void setPageId(long base, long pageId) {
        putLong(base + PAGE_ID_OFFSET, pageId);
    }

    /**
     * Returns group ID stored in entry.
     *
     * @param base Entry base address.
     */
    private int getGrpId(long base) {
        return getInt(base + GRP_ID_OFFSET);
    }

    /**
     * Sets group ID.
     *
     * @param base Entry base address.
     * @param grpId Group ID to be stored in entry.
     */
    private void setGrpId(long base, int grpId) {
        putInt(base + GRP_ID_OFFSET, grpId);
    }

    /**
     * Returns value stored in bucket.
     *
     * @param base Bucket base address.
     */
    private long getValue(long base) {
        return getLong(base + VALUE_OFFSET);
    }

    /**
     * Sets value to the bucket.
     *
     * @param base Bucket base address.
     * @param val Value to store in bucket.
     */
    private void setValue(long base, long val) {
        putLong(base + VALUE_OFFSET, val);
    }

    /**
     * Returns entry version associated with bucket.
     *
     * @param base Bucket base address.
     */
    private int getVersion(long base) {
        return getInt(base + VERSION_OFFSET);
    }

    /**
     * Sets version to the bucket.
     *
     * @param base Bucket base address.
     * @param ver Entry version to set in bucket.
     */
    private void setVersion(long base, int ver) {
        putInt(base + VERSION_OFFSET, ver);
    }

    /** {@inheritDoc} */
    @Override
    public final int size() {
        return getInt(baseAddr + MAPSIZE_OFFSET);
    }

    /**
     * Changes collection size.
     *
     * @param sz new size to set.
     */
    private void setSize(int sz) {
        putInt(baseAddr + MAPSIZE_OFFSET, sz);
    }

    /** {@inheritDoc} */
    @Override
    public void forEach(BiConsumer<FullPageId, Long> act) {
        for (int i = 0; i < numBuckets; i++) {
            if (isEmptyAt(i)) {
                continue;
            }

            long base = entryBase(i);

            act.accept(getFullPageId(base), getValue(base));
        }
    }

    /**
     * Return full page ID from bucket (key).
     *
     * @param base bucket base address.
     */
    private FullPageId getFullPageId(long base) {
        return new FullPageId(getPageId(base), getGrpId(base));
    }
}
