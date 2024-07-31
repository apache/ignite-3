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

import static org.apache.ignite.compute.TaskStatus.CANCELED;
import static org.apache.ignite.compute.TaskStatus.CANCELING;
import static org.apache.ignite.compute.TaskStatus.COMPLETED;
import static org.apache.ignite.compute.TaskStatus.EXECUTING;
import static org.apache.ignite.compute.TaskStatus.FAILED;
import static org.apache.ignite.compute.TaskStatus.QUEUED;

import org.apache.ignite.compute.JobStatus;
import org.apache.ignite.compute.TaskStatus;

/**
 * Mapper for job status from\to task status.
 */
public class JobTaskStatusMapper {

    /**
     * Map task status to job status.
     *
     * @param taskStatus Task status.
     * @return Mapped job status.
     */
    public static JobStatus toJobStatus(TaskStatus taskStatus) {
        switch (taskStatus) {
            case QUEUED:
                return JobStatus.QUEUED;
            case EXECUTING:
                return JobStatus.EXECUTING;
            case FAILED:
                return JobStatus.FAILED;
            case COMPLETED:
                return JobStatus.COMPLETED;
            case CANCELING:
                return JobStatus.CANCELING;
            case CANCELED:
                return JobStatus.CANCELED;
            default:
                throw new IllegalArgumentException("Unknown task status.");
        }
    }

    /**
     * Map job status to task status.
     *
     * @param jobStatus Job status.
     * @return Mapped task status.
     */
    public static TaskStatus toTaskStatus(JobStatus jobStatus) {
        switch (jobStatus) {
            case QUEUED:
                return QUEUED;
            case EXECUTING:
                return EXECUTING;
            case FAILED:
                return FAILED;
            case COMPLETED:
                return COMPLETED;
            case CANCELING:
                return CANCELING;
            case CANCELED:
                return CANCELED;
            default:
                throw new IllegalArgumentException("Unknown job status.");
        }
    }
}
