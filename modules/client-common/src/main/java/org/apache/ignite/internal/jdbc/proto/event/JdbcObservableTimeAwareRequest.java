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

package org.apache.ignite.internal.jdbc.proto.event;

import org.apache.ignite.internal.hlc.HybridTimestampTracker;
import org.jetbrains.annotations.Nullable;

/**
 * An extension to JDBC request that provides the ability to update and read client observable time.
 */
abstract class JdbcObservableTimeAwareRequest {
    /** Tracker of the latest time observed by client. */
    @SuppressWarnings("TransientFieldInNonSerializableClass")
    private transient @Nullable HybridTimestampTracker timestampTracker;

    /** Returns the tracker of the latest time observed by client. */
    public @Nullable HybridTimestampTracker timestampTracker() {
        return timestampTracker;
    }

    /** Sets the tracker of the latest time observed by client. */
    public void timestampTracker(HybridTimestampTracker tracker) {
        this.timestampTracker = tracker;
    }
}
