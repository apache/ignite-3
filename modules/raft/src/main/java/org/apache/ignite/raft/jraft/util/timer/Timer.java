/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util.timer;

import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Schedules {@link TimerTask}s for one-time future execution in a background thread.
 *
 * Forked from <a href="https://github.com/netty/netty">Netty</a>.
 */
public interface Timer {

    /**
     * Schedules the specified {@link TimerTask} for one-time execution after the specified delay.
     *
     * @return a handle which is associated with the specified task
     * @throws IllegalStateException if this timer has been {@linkplain #stop() stopped} already
     * @throws RejectedExecutionException if the pending timeouts are too many and creating new timeout can cause
     * instability in the system.
     */
    Timeout newTimeout(final TimerTask task, final long delay, final TimeUnit unit);

    /**
     * Releases all resources acquired by this {@link Timer} and cancels all tasks which were scheduled but not executed
     * yet.
     *
     * @return the handles associated with the tasks which were canceled by this method
     */
    Set<Timeout> stop();
}
