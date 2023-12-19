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

package org.apache.ignite.network;

import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.jetbrains.annotations.Nullable;

/**
 * Trackable message handler that will log long-running messages.
 */
public class TrackableNetworkMessageHandler implements NetworkMessageHandler {
    private static final IgniteLogger LOG = Loggers.forClass(TrackableNetworkMessageHandler.class);

    /**
     * If message handling takes more time, than this constant, we will log warning message with some information.
     */
    // TODO: I don't know which value to use here, let's discuss in the PR. On the one hand every IO should be highlighted and thus
    // TODO: the value should be rather small, on the other hand, currently we have lots of slow messages and we will just flood the log
    // TODO: will log running ones. So for now, I believe we should set rather big value here in order to ease test failure analysis.
    private static final int WATCH_EVENT_PROCESSING_LOG_THRESHOLD_NANOS = 1_000_000;

    private final NetworkMessageHandler targetHandler;

    TrackableNetworkMessageHandler(NetworkMessageHandler targetHandler) {
        this.targetHandler = targetHandler;
    }

    @Override
    public void onReceived(NetworkMessage message, String senderConsistentId, @Nullable Long correlationId) {
        long startTimeNanos = System.nanoTime();

        targetHandler.onReceived(message, senderConsistentId, correlationId);

        maybeLogLongProcessing(message, startTimeNanos);
    }

    private static void maybeLogLongProcessing(NetworkMessage message, long startTimeNanos) {
        long durationNanos = System.nanoTime() - startTimeNanos;

        if (durationNanos > WATCH_EVENT_PROCESSING_LOG_THRESHOLD_NANOS) {
            LOG.warn(
                    "Message handling has been too long [duration={}ns, message=[{}]]",
                    durationNanos,
                    message
            );
        }
    }
}
