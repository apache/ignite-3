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
import org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.security.authentication.UserDetails;
import org.jetbrains.annotations.Nullable;

/**
 * Metadata builder.
 */
public class ComputeEventMetadataBuilder {
    private EventUser eventUser;
    private Type type;
    private String jobClassName;
    private @Nullable UUID jobId;
    private String targetNode;
    private String initiatorNode;
    private @Nullable UUID taskId;
    private @Nullable String tableName;
    private @Nullable String clientAddress;

    ComputeEventMetadataBuilder type(Type type) {
        this.type = type;
        return this;
    }

    public ComputeEventMetadataBuilder eventUser(UserDetails userDetails) {
        this.eventUser = EventUser.of(userDetails.username(), userDetails.providerName());
        return this;
    }

    public ComputeEventMetadataBuilder jobClassName(String jobClassName) {
        this.jobClassName = jobClassName;
        return this;
    }

    public ComputeEventMetadataBuilder jobId(@Nullable UUID jobId) {
        this.jobId = jobId;
        return this;
    }

    public @Nullable UUID jobId() {
        return jobId;
    }

    public ComputeEventMetadataBuilder targetNode(String targetNode) {
        this.targetNode = targetNode;
        return this;
    }

    public ComputeEventMetadataBuilder initiatorNode(String initiatorNode) {
        this.initiatorNode = initiatorNode;
        return this;
    }

    public ComputeEventMetadataBuilder taskId(@Nullable UUID taskId) {
        this.taskId = taskId;
        return this;
    }

    public ComputeEventMetadataBuilder tableName(@Nullable String tableName) {
        this.tableName = tableName;
        return this;
    }

    public ComputeEventMetadataBuilder clientAddress(@Nullable String clientAddress) {
        this.clientAddress = clientAddress;
        return this;
    }

    /**
     * Builds the metadata.
     *
     * @return Event metadata.
     */
    public ComputeEventMetadata build() {
        return new ComputeEventMetadata(
                eventUser != null ? eventUser : EventUser.system(),
                type,
                jobClassName,
                jobId,
                targetNode,
                initiatorNode,
                taskId,
                tableName,
                clientAddress
        );
    }

    /**
     * Creates new builder from this builder.
     *
     * @return Created builder.
     */
    public ComputeEventMetadataBuilder copyOf() {
        ComputeEventMetadataBuilder builder = new ComputeEventMetadataBuilder()
                .type(type)
                .jobClassName(jobClassName)
                .jobId(jobId)
                .targetNode(targetNode)
                .initiatorNode(initiatorNode)
                .taskId(taskId)
                .tableName(tableName)
                .clientAddress(clientAddress);

        if (eventUser != null) {
            builder.eventUser(new UserDetails(eventUser.username(), eventUser.authenticationProvider()));
        }

        return builder;
    }
}
