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

package org.apache.ignite.internal.worker;

import org.apache.ignite.internal.metrics.AtomicLongMetric;

/**
 * Metrics for the {@link CriticalSingleThreadExecutor}.
 */
public class CriticalSingleThreadExecutorMetrics {
    private final AtomicLongMetric messageQueueSize;

    public CriticalSingleThreadExecutorMetrics(CriticalSingleThreadExecutorMetricSource source) {
        messageQueueSize = source.addMetric(new AtomicLongMetric("MessageQueueSize", "Size of the message queue."));
    }

    /**
     * Increments message queue size.
     *
     */
    public void incrementMessageQueueSize() {
        this.messageQueueSize.increment();
    }

    /**
     * Decrements message queue size.
     *
     */
    public void decrementMessageQueueSize() {
        this.messageQueueSize.decrement();
    }
}
