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

import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.raft.configuration.LogStorageConfiguration;
import org.apache.ignite.internal.raft.util.SharedLogStorageManagerUtils;

/** Options to create {@link SegmentLogStorageManager} using {@link SharedLogStorageManagerUtils}. */
public class SegmentLogStorageOptions {
    private final int stripes;

    private final LogStorageConfiguration configuration;

    private final FailureProcessor failureProcessor;

    /** Constructor. */
    public SegmentLogStorageOptions(int stripes, LogStorageConfiguration configuration, FailureProcessor failureProcessor) {
        this.stripes = stripes;
        this.configuration = configuration;
        this.failureProcessor = failureProcessor;
    }

    /** Number of log storage disruptor stripes. */
    public int stripes() {
        return stripes;
    }

    /** Log storage configuration. */
    public LogStorageConfiguration configuration() {
        return configuration;
    }

    /** Failure processor. */
    public FailureProcessor failureProcessor() {
        return failureProcessor;
    }
}
