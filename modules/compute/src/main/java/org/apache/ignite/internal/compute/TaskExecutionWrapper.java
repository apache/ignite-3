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

package org.apache.ignite.internal.compute;

import static org.apache.ignite.internal.lang.IgniteExceptionMapperUtil.convertToPublicFuture;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.task.TaskExecution;
import org.jetbrains.annotations.Nullable;

/**
 * Wraps the {@link TaskExecution} converting exceptions thrown by the delegate to public.
 *
 * @param <R> Result type.
 */
class TaskExecutionWrapper<R> extends JobExecutionWrapper<R> implements TaskExecution<R> {
    private final TaskExecution<R> delegate;

    TaskExecutionWrapper(TaskExecution<R> delegate) {
        super(delegate);
        this.delegate = delegate;
    }

    @Override
    public CompletableFuture<List<@Nullable JobStatus>> statusesAsync() {
        return convertToPublicFuture(delegate.statusesAsync());
    }
}
