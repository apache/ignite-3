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

import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.schema.configuration.LowWatermarkConfigurationSchema;

/**
 * Scheduled task to {@link LowWatermarkImpl#updateLowWatermark} at {@link LowWatermarkConfigurationSchema#updateIntervalMillis}.
 */
class ScheduledUpdateLowWatermarkTask implements Runnable {
    private final LowWatermarkImpl lowWatermarkImpl;

    private final AtomicReference<State> state;

    ScheduledUpdateLowWatermarkTask(LowWatermarkImpl lowWatermarkImpl, State state) {
        this.lowWatermarkImpl = lowWatermarkImpl;

        this.state = new AtomicReference<>(state);
    }

    @Override
    public void run() {
        if (state.compareAndSet(State.NEW, State.IN_PROGRESS)) {
            lowWatermarkImpl.updateLowWatermarkAsync(lowWatermarkImpl.createNewLowWatermarkCandidate())
                    .whenCompleteAsync((unused, throwable) -> {
                        if (state.compareAndSet(State.IN_PROGRESS, State.COMPLETED) && !hasCause(throwable, NodeStoppingException.class)) {
                            lowWatermarkImpl.scheduleUpdates();
                        }
                    });
        }
    }

    boolean tryCancel() {
        return state.compareAndSet(State.NEW, State.CANCELLED);
    }

    State state() {
        return state.get();
    }

    enum State {
        NEW, IN_PROGRESS, COMPLETED, CANCELLED
    }
}
