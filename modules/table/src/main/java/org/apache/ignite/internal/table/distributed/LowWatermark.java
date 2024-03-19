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

package org.apache.ignite.internal.table.distributed;

import java.util.function.Consumer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.jetbrains.annotations.Nullable;

/**
 * Low watermark is the node's local time, which ensures that read-only transactions have completed by this time, and new read-only
 * transactions will only be created after this time, and we can safely delete obsolete/garbage data such as: obsolete versions of table
 * rows, remote indexes, remote tables, etc.
 *
 * @see <a href="https://cwiki.apache.org/confluence/display/IGNITE/IEP-91%3A+Transaction+protocol">IEP-91</a>
 */
public interface LowWatermark {
    /** Returns the current low watermark, {@code null} means no low watermark has been assigned yet. */
    @Nullable HybridTimestamp getLowWatermark();

    /** Subscribes on watermark changes. */
    void addUpdateListener(LowWatermarkChangedListener listener);

    /** Unsubscribes on watermark changes. */
    void removeUpdateListener(LowWatermarkChangedListener listener);

    /**
     * Runs the provided {@code consumer} under the {@code lock} preventing concurrent LWM update, {@code null} means no low watermark has
     * been assigned yet.
     */
    void getLowWatermarkSafe(Consumer<@Nullable HybridTimestamp> consumer);
}
