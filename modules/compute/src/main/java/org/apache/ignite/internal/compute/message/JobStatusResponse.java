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

package org.apache.ignite.internal.compute.message;

import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.internal.compute.ComputeMessageTypes;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Marshallable;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/**
 * Remote job status response.
 */
@Transferable(ComputeMessageTypes.JOB_STATUS_RESPONSE)
public interface JobStatusResponse extends NetworkMessage {
    /**
     * Returns job status ({@code null} if the request has failed or the job with requested id doesn't exist).
     *
     * @return job status ({@code null} if the request has failed or the job with requested id doesn't exist)
     */
    @Nullable
    @Marshallable
    JobStatus status();

    /**
     * Returns a {@link Throwable} that was thrown during job status request ({@code null} if the request was successful).
     *
     * @return {@link Throwable} that was thrown during job status request ({@code null} if the request was successful)
     */
    @Nullable
    @Marshallable
    Throwable throwable();
}
