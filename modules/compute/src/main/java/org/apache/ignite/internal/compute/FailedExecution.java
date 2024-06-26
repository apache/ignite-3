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

import static java.util.concurrent.CompletableFuture.failedFuture;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.compute.JobExecution;
import org.apache.ignite.compute.JobState;
import org.apache.ignite.marshaling.Marshaler;
import org.jetbrains.annotations.Nullable;

/**
 * Job execution implementation which will return failed future with specified error from all methods.
 *
 * @param <R> Job result type.
 */
public class FailedExecution<R> implements JobExecution<R> {

    private final Throwable error;

    FailedExecution(Throwable error) {
        this.error = error;
    }

    @Override
    public CompletableFuture<R> resultAsync() {
        return failedFuture(error);
    }

    @Override
    public CompletableFuture<@Nullable JobState> stateAsync() {
        return failedFuture(error);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> cancelAsync() {
        return failedFuture(error);
    }

    @Override
    public CompletableFuture<@Nullable Boolean> changePriorityAsync(int newPriority) {
        return failedFuture(error);
    }

    @Override
    public Marshaler<R, byte[]> resultMarshaler() {
        return null;
    }
}
