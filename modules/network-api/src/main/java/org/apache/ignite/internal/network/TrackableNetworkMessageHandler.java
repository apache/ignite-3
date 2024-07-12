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

package org.apache.ignite.internal.network;

import static org.apache.ignite.internal.tostring.IgniteToStringBuilder.includeSensitive;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.thread.IgniteThread;
import org.apache.ignite.internal.thread.ThreadOperation;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Trackable message handler that will log long-running messages.
 */
public class TrackableNetworkMessageHandler implements NetworkMessageHandler {
    private static final IgniteLogger LOG = Loggers.forClass(TrackableNetworkMessageHandler.class);

    /**
     * If message handling takes more time, than this constant, we will log warning message with some information.
     */
    private static final int MESSAGING_PROCESSING_LOG_THRESHOLD_MILLIS = 5;

    private final NetworkMessageHandler targetHandler;

    TrackableNetworkMessageHandler(NetworkMessageHandler targetHandler) {
        this.targetHandler = targetHandler;
    }

    @Override
    public void onReceived(NetworkMessage message, ClusterNode sender, @Nullable Long correlationId) {
        long startTimeNanos = System.nanoTime();

        targetHandler.onReceived(message, sender, correlationId);

        if (!storageThread()) {
            maybeLogLongProcessing(message, startTimeNanos);
        }
    }

    private static boolean storageThread() {
        IgniteThread current = IgniteThread.current();

        if (current == null) {
            return false;
        }

        Set<ThreadOperation> allowedOperations = current.allowedOperations();

        return allowedOperations.contains(ThreadOperation.STORAGE_READ) || allowedOperations.contains(ThreadOperation.STORAGE_WRITE);
    }

    private static void maybeLogLongProcessing(NetworkMessage message, long startTimeNanos) {
        long durationMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTimeNanos);

        if (durationMillis > MESSAGING_PROCESSING_LOG_THRESHOLD_MILLIS) {
            LOG.warn(
                    "Message handling has been too long [duration={}ms, message=[{}]]",
                    durationMillis,
                    // Message may include sensitive data, however it seems useful to print full message content while testing.
                    includeSensitive() ? message : message.getClass()
            );
        }
    }
}
