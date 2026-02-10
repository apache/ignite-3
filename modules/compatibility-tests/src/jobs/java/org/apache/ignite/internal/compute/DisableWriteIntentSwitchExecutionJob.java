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
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.JobExecutionContext;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.wrapper.Wrappers;
import org.jetbrains.annotations.Nullable;

/** Disables write intent switches on the local node until restart. */
public class DisableWriteIntentSwitchExecutionJob implements ComputeJob<Void, Void> {
    @Override
    public @Nullable CompletableFuture<Void> executeAsync(JobExecutionContext context, @Nullable Void arg) {
        IgniteImpl igniteImpl = Wrappers.unwrap(context.ignite(), IgniteImpl.class);

        igniteImpl.dropMessages((recipientId, message) -> message.getClass().getName().contains("WriteIntentSwitchReplicaRequest"));

        return null;
    }
}
