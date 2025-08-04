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
import org.apache.ignite.internal.security.authentication.UserDetails;

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

    /** For embedded API. */
    private final String initiatorNodeId;

    /** For client API. */
    private final String initiatorClient;

    /** Event user. */
    private final EventUser eventUser;

    /** Node name. */
    private final String nodeName;

    private ComputeEventMetadata(
            Type type,
            String jobClassName,
            UUID jobId,
            UUID taskId,
            String tableName,
            String initiatorNodeId,
            String initiatorClient,
            EventUser eventUser,
            String nodeName
    ) {
        this.type = type;
        this.jobClassName = jobClassName;
        this.jobId = jobId;
        this.taskId = taskId;
        this.tableName = tableName;
        this.initiatorNodeId = initiatorNodeId;
        this.initiatorClient = initiatorClient;
        this.eventUser = eventUser;
        this.nodeName = nodeName;
    }

    /**
     * Creates new builder.
     *
     * @return Created builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Creates new builder with specified job type.
     *
     * @param type Job type.
     * @return Created builder.
     */
    public static Builder builder(Type type) {
        return new Builder().type(type);
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

    String initiatorNodeId() {
        return initiatorNodeId;
    }

    String initiatorClient() {
        return initiatorClient;
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

    /**
     * Metadata builder.
     */
    public static class Builder {
        private Type type;
        private String jobClassName;
        private UUID jobId;
        private UUID taskId;
        private String tableName;
        private String initiatorNodeId;
        private String initiatorClient;
        private EventUser eventUser;
        private String nodeName;

        private Builder type(Type type) {
            this.type = type;
            return this;
        }

        public Builder jobClassName(String jobClassName) {
            this.jobClassName = jobClassName;
            return this;
        }

        public Builder jobId(UUID jobId) {
            this.jobId = jobId;
            return this;
        }

        public Builder taskId(UUID taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder initiatorNodeId(String initiatorNodeId) {
            this.initiatorNodeId = initiatorNodeId;
            return this;
        }

        public Builder initiatorClient(String initiatorClient) {
            this.initiatorClient = initiatorClient;
            return this;
        }

        public Builder eventUser(UserDetails userDetails) {
            this.eventUser = EventUser.of(userDetails.username(), userDetails.providerName());
            return this;
        }

        public Builder nodeName(String nodeName) {
            this.nodeName = nodeName;
            return this;
        }

        /**
         * Builds the metadata.
         *
         * @return Event metadata.
         */
        public ComputeEventMetadata build() {
            return new ComputeEventMetadata(
                    type,
                    jobClassName,
                    jobId,
                    taskId,
                    tableName,
                    initiatorNodeId,
                    initiatorClient,
                    eventUser,
                    nodeName
            );
        }
    }
}
