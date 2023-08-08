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

package org.apache.ignite.internal.cli.core.repl;

import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;

import jakarta.inject.Singleton;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.cli.event.ConnectionEventListener;
import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.jetbrains.annotations.Nullable;

/** Executes tasks periodically while the session is connected. */
@Singleton
public class PeriodicSessionTaskExecutor implements ConnectionEventListener {
    private static final IgniteLogger LOG = CliLoggers.forClass(PeriodicSessionTaskExecutor.class);

    @Nullable
    private ScheduledExecutorService executor;

    private final List<? extends PeriodicSessionTask> tasks;

    public PeriodicSessionTaskExecutor(List<? extends PeriodicSessionTask> tasks) {
        this.tasks = tasks;
    }

    @Override
    public synchronized void onConnect(SessionInfo sessionInfo) {
        if (executor == null) {
            executor = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("SessionTaskExecutor", LOG));
            executor.scheduleWithFixedDelay(() -> runTasks(sessionInfo), 0, 5, TimeUnit.SECONDS);
        }
    }

    @Override
    public synchronized void onDisconnect() {
        if (executor != null) {
            shutdownAndAwaitTermination(executor, 3, TimeUnit.SECONDS);
            tasks.forEach(PeriodicSessionTask::onDisconnect);
            executor = null;
        }
    }

    private void runTasks(SessionInfo sessionInfo) {
        tasks.forEach(periodicSessionTask -> periodicSessionTask.update(sessionInfo));
    }
}
