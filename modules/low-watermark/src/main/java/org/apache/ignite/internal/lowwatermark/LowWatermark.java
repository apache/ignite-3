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

package org.apache.ignite.internal.lowwatermark;

import java.util.UUID;
import java.util.function.Consumer;
import org.apache.ignite.internal.event.EventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEventParameters;
import org.jetbrains.annotations.Nullable;

/**
 * Low watermark is the node's local time, meaning that data (obsolete versions of table rows, remote indexes, remote tables, etc) that was
 * deleted before (less than or equal to) this time will be destroyed on the node, i.e. not available for reading.
 *
 * <p>Low watermark increases monotonically only after all {@link org.apache.ignite.internal.event.EventListener} have completed processing
 * the new value.</p>
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-91%3A+Transaction+protocol">IEP-91</a>
 */
public interface LowWatermark extends EventProducer<LowWatermarkEvent, LowWatermarkEventParameters> {
    /** Returns the current low watermark, {@code null} means no low watermark has been assigned yet. */
    @Nullable HybridTimestamp getLowWatermark();

    /**
     * Runs the provided {@code consumer} under the {@code lock} preventing concurrent LWM update, {@code null} means no low watermark has
     * been assigned yet.
     */
    void getLowWatermarkSafe(Consumer<@Nullable HybridTimestamp> consumer);

    /**
     * Updates the low watermark if it is higher than the current one.
     *
     * @param newLowWatermark Candidate for update.
     */
    void updateLowWatermark(HybridTimestamp newLowWatermark);

    /**
     * Sets the low watermark during node recovery.
     *
     * <p>This method sets the low watermark immediately and unconditionally, without checking if the new value is higher than
     * the current one. Listeners are not notified about the change; even more, they must not be registered at this point.
     * It is intended to be used only during node recovery.
     *
     * @param newLowWatermark New low watermark.
     */
    void setLowWatermarkOnRecovery(HybridTimestamp newLowWatermark);

    /**
     * Locks the low watermark at the provided timestamp (prevents it from being updated to a value higher than the provided one).
     *
     * @param lockId ID of the transaction for which the low watermark should be locked.
     * @param ts Timestamp to lock.
     * @return True if the lock was acquired,
     *     false if the lock was not acquired due to the low watermark being higher than the provided timestamp.
     */
    boolean tryLock(UUID lockId, HybridTimestamp ts);

    /**
     * Releases the lock created by {@link #tryLock(UUID, HybridTimestamp)}.
     *
     * @param lockId ID of the transaction that locks the low watermark.
     */
    void unlock(UUID lockId);
}
