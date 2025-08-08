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

/**
 * Compute job metadata used for logging events.
 */
public class ComputeEventMetadata {
    /** The type of the job. **/
    private final Type type;

    /** Job class name. **/
    private final String jobClassName;

    /** Compute job id. */
    private final UUID jobId;

    /** Used in events for broadcast jobs - common id for a group of jobs. */
    private final UUID taskId;

    /** For colocated jobs. */
    private final String tableName;

    /** Node that initiated execution. */
    private final String initiatorNode;

    /** For client API. */
    private final String clientAddress;

    /** Event user. */
    private final EventUser eventUser;

    /** Node name. */
    private final String nodeName;

    ComputeEventMetadata(
            Type type,
            String jobClassName,
            UUID jobId,
            UUID taskId,
            String tableName,
            String initiatorNode,
            String clientAddress,
            EventUser eventUser,
            String nodeName
    ) {
        this.type = type;
        this.jobClassName = jobClassName;
        this.jobId = jobId;
        this.taskId = taskId;
        this.tableName = tableName;
        this.initiatorNode = initiatorNode;
        this.clientAddress = clientAddress;
        this.eventUser = eventUser;
        this.nodeName = nodeName;
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

    Type type() {
        return type;
    }

    String jobClassName() {
        return jobClassName;
    }

    UUID jobId() {
        return jobId;
    }

    UUID taskId() {
        return taskId;
    }

    String tableName() {
        return tableName;
    }

    String initiatorNode() {
        return initiatorNode;
    }

    String clientAddress() {
        return clientAddress;
    }

    EventUser eventUser() {
        return eventUser;
    }

    String nodeName() {
        return nodeName;
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
