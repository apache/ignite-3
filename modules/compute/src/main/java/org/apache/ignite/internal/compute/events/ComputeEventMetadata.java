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

package org.apache.ignite.internal.compute.events;

import java.util.UUID;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.jetbrains.annotations.Nullable;

/**
 * Compute job metadata used for logging events.
 */
public class ComputeEventMetadata {
    /** Event user. */
    private final EventUser eventUser;

    /** The type of the job. **/
    private final Type type;

    /** Job class name. **/
    private final String jobClassName;

    /** Compute job id. */
    private final @Nullable UUID jobId;

    /** Target node name. */
    private final String targetNode;

    /** Node that initiated execution. */
    private final String initiatorNode;

    /** Used in events for broadcast jobs - common id for a group of jobs. */
    private final @Nullable UUID taskId;

    /** For colocated jobs. */
    private final @Nullable String tableName;

    /** For client API. */
    private final @Nullable String clientAddress;

    ComputeEventMetadata(
            EventUser eventUser,
            Type type,
            String jobClassName,
            @Nullable UUID jobId,
            String targetNode,
            String initiatorNode,
            @Nullable UUID taskId,
            @Nullable String tableName,
            @Nullable String clientAddress
    ) {
        this.eventUser = eventUser;
        this.type = type;
        this.jobClassName = jobClassName;
        this.jobId = jobId;
        this.targetNode = targetNode;
        this.initiatorNode = initiatorNode;
        this.taskId = taskId;
        this.tableName = tableName;
        this.clientAddress = clientAddress;
    }

    /**
     * Creates new builder.
     *
     * @return Created builder.
     */
    public static ComputeEventMetadataBuilder builder() {
        return new ComputeEventMetadataBuilder();
    }

    /**
     * Creates new builder with specified job type.
     *
     * @param type Job type.
     * @return Created builder.
     */
    public static ComputeEventMetadataBuilder builder(Type type) {
        return new ComputeEventMetadataBuilder().type(type);
    }

    EventUser eventUser() {
        return eventUser;
    }

    Type type() {
        return type;
    }

    String jobClassName() {
        return jobClassName;
    }

    @Nullable
    UUID jobId() {
        return jobId;
    }

    String targetNode() {
        return targetNode;
    }

    String initiatorNode() {
        return initiatorNode;
    }

    @Nullable UUID taskId() {
        return taskId;
    }

    @Nullable String tableName() {
        return tableName;
    }

    @Nullable String clientAddress() {
        return clientAddress;
    }

    /**
     * Represents job type.
     */
    public enum Type {
        SINGLE,
        BROADCAST,
        MAP_REDUCE, // Combines split, reduce and actual job.
        DATA_RECEIVER
    }
}
