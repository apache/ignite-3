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

import static org.apache.ignite.internal.failure.FailureType.CRITICAL_ERROR;
import static org.apache.ignite.internal.raft.storage.segstore.SegmentFileManager.SWITCH_SEGMENT_RECORD;
import static org.apache.ignite.internal.util.IgniteUtils.closeAllManually;
import static org.apache.ignite.lang.ErrorGroups.Marshalling.COMMON_ERR;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.Iterator;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.raft.storage.segstore.CheckpointQueue.Entry;
import org.apache.ignite.internal.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;

/**
 * Class responsible for running periodic checkpoint tasks.
 *
 * <p>A checkpoint is triggered by the {@link SegmentFileManager} when segment file rollover occurs. Upon rollover, the following happens:
 *
 * <ol>
 *     <li>{@code SegmentFileManager} notifies the Checkpointer by providing the segment file that got full and the
 *     {@link ReadModeIndexMemTable} for this file;</li>
 *     <li>Checkpointer adds a task to the tail of the {@link CheckpointQueue}. If the queue is full, the caller thread is blocked until
 *     free space is available in the queue.</li>
 *     <li>Checkpoint thread polls the head of the queue and performs the following actions:
 *          <ol>
 *              <li>Syncs the segment file;</li>
 *              <li>Persists the memtable into an index file;</li>
 *              <li>Creates in-memory metadata structures for the index;</li>
 *              <li>Removes the mem table from the queue and therefore disposes of it.</li>
 *          </ol>
 *     </li>
 * </ol>
 */
class RaftLogCheckpointer {
    // TODO: Move to configuration, see https://issues.apache.org/jira/browse/IGNITE-26476.
    static final int MAX_QUEUE_SIZE = 10;

    private final CheckpointQueue queue = new CheckpointQueue(MAX_QUEUE_SIZE);

    private final Thread checkpointThread;

    private final IndexFileManager indexFileManager;

    private final FailureProcessor failureProcessor;

    RaftLogCheckpointer(String nodeName, IndexFileManager indexFileManager, FailureProcessor failureProcessor) {
        this.indexFileManager = indexFileManager;
        this.failureProcessor = failureProcessor;

        checkpointThread = new IgniteThread(nodeName, "segstore-checkpoint", new CheckpointTask());
    }

    void start() {
        checkpointThread.start();
    }

    void stop() throws Exception {
        closeAllManually(this::stopCheckpointThread, queue);
    }

    private void stopCheckpointThread() {
        checkpointThread.interrupt();

        try {
            checkpointThread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInternalException(COMMON_ERR, "Interrupted while waiting for the checkpoint thread to finish.", e);
        }
    }

    void onRollover(SegmentFile segmentFile, ReadModeIndexMemTable indexMemTable) {
        try {
            queue.add(segmentFile, indexMemTable);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            throw new IgniteInternalException(COMMON_ERR, "Interrupted while adding an entry to the checkpoint queue.", e);
        }
    }

    /**
     * Searches for the segment payload corresponding to the given Raft Group ID and Raft Log Index in the checkpoint queue.
     *
     * @return {@code ByteBuffer} which position is set to the start of the corresponding segment payload or {@code null} if the payload has
     *         not been found in all files currently present in the queue.
     */
    @Nullable ByteBuffer findSegmentPayloadInQueue(long groupId, long logIndex) {
        Iterator<Entry> it = queue.tailIterator();

        while (it.hasNext()) {
            Entry e = it.next();

            SegmentInfo segmentInfo = e.memTable().segmentInfo(groupId);

            int segmentPayloadOffset = segmentInfo == null ? 0 : segmentInfo.getOffset(logIndex);

            if (segmentPayloadOffset != 0) {
                return e.segmentFile().buffer().position(segmentPayloadOffset);
            }
        }

        return null;
    }

    /**
     * Returns the lowest log index for the given group present in the checkpoint queue or {@code -1} if no such index exists.
     */
    long firstLogIndexInclusive(long groupId) {
        Iterator<Entry> it = queue.tailIterator();

        long firstIndex = -1;

        while (it.hasNext()) {
            SegmentInfo segmentInfo = it.next().memTable().segmentInfo(groupId);

            // Segment Info can be empty if the log suffix was truncated.
            if (segmentInfo != null && segmentInfo.size() > 0) {
                firstIndex = segmentInfo.firstLogIndexInclusive();
            }
        }

        return firstIndex;
    }

    /**
     * Returns the highest possible log index for the given group present in the checkpoint queue or {@code -1} if no such index exists.
     */
    long lastLogIndexExclusive(long groupId) {
        Iterator<Entry> it = queue.tailIterator();

        while (it.hasNext()) {
            SegmentInfo segmentInfo = it.next().memTable().segmentInfo(groupId);

            if (segmentInfo != null) {
                return segmentInfo.lastLogIndexExclusive();
            }
        }

        return -1;
    }

    private class CheckpointTask implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    CheckpointQueue.Entry entry = queue.peekHead();

                    SegmentFile segmentFile = entry.segmentFile();

                    // This will block until all ongoing writes have been completed.
                    segmentFile.closeForRollover(SWITCH_SEGMENT_RECORD);

                    segmentFile.sync();

                    indexFileManager.saveIndexMemtable(entry.memTable());

                    queue.removeHead();
                } catch (InterruptedException | ClosedByInterruptException e) {
                    // Interrupt is called on stop.
                    return;
                } catch (IOException e) {
                    failureProcessor.process(new FailureContext(CRITICAL_ERROR, e));
                }
            }
        }
    }
}
