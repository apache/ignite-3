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
import static org.apache.ignite.lang.ErrorGroups.Marshalling.COMMON_ERR;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.thread.IgniteThread;

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
    static final int MEM_TABLE_QUEUE_SIZE = 10;

    private final CheckpointQueue queue = new CheckpointQueue(MEM_TABLE_QUEUE_SIZE);

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

    void stop() {
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

    private class CheckpointTask implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    CheckpointQueue.Entry entry = queue.peek();

                    entry.segmentFile().sync();

                    IndexFile indexFile = indexFileManager.saveIndexMemtable(entry.memTable());

                    queue.removeHead();

                    indexFile.syncAndRename();
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
