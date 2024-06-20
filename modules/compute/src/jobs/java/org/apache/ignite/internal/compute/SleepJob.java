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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;

/** Compute job that sleeps for a number of milliseconds passed in the argument. */
public class SleepJob implements ComputeJob<Void> {
    @Override
    public CompletableFuture<Void> executeAsync(JobExecutionContext jobExecutionContext, Object... args) {
        try {
            TimeUnit.SECONDS.sleep((Long) args[0]);
            return null;
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
