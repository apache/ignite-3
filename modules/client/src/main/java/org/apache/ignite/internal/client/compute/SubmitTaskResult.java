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

package org.apache.ignite.internal.client.compute;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.PayloadInputChannel;
import org.jetbrains.annotations.Nullable;

/**
 * Result of the task submission.
 * Contains unpacked job id, a collection of ids of the jobs which are executing under this task, and notification future.
 */
class SubmitTaskResult extends SubmitResult {
    private final List<UUID> jobIds;

    SubmitTaskResult(UUID jobId, List<UUID> jobIds, CompletableFuture<PayloadInputChannel> notificationFuture) {
        super(jobId, notificationFuture);
        this.jobIds = jobIds;
    }

    List<@Nullable UUID> jobIds() {
        return jobIds;
    }
}
