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

package org.apache.ignite.compute;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;
import org.jetbrains.annotations.Nullable;

/**
 * Job state.
 */
public interface JobState extends Serializable {
    /**
     * Returns job ID.
     *
     * @return Job ID.
     */
    UUID id();

    /**
     * Returns job status.
     *
     * @return Job status.
     */
    JobStatus status();

    /**
     * Returns job create time.
     *
     * @return Job create time.
     */
    Instant createTime();

    /**
     * Returns job start time. {@code null} if the job has not started yet.
     *
     * @return Job start time. {@code null} if the job has not started yet.
     */
    @Nullable
    Instant startTime();

    /**
     * Returns job finish time. {@code null} if the job has not finished yet.
     *
     * @return Job finish time. {@code null} if the job has not finished yet.
     */
    @Nullable
    Instant finishTime();
}
