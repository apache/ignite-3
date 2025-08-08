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
import org.apache.ignite.internal.tostring.S;

/**
 * Metadata builder.
 */
public class ComputeEventMetadataBuilder {
    private Type type;
    private String jobClassName;
    private UUID jobId;
    private UUID taskId;
    private String tableName;
    private String initiatorNodeId;
    private String initiatorClient;
    private EventUser eventUser;
    private String nodeName;

    ComputeEventMetadataBuilder type(Type type) {
        this.type = type;
        return this;
    }

    public ComputeEventMetadataBuilder jobClassName(String jobClassName) {
        this.jobClassName = jobClassName;
        return this;
    }

    public ComputeEventMetadataBuilder jobId(UUID jobId) {
        this.jobId = jobId;
        return this;
    }

    public UUID jobId() {
        return jobId;
    }

    public ComputeEventMetadataBuilder taskId(UUID taskId) {
        this.taskId = taskId;
        return this;
    }

    public ComputeEventMetadataBuilder tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public ComputeEventMetadataBuilder initiatorNodeId(String initiatorNodeId) {
        this.initiatorNodeId = initiatorNodeId;
        return this;
    }

    public ComputeEventMetadataBuilder initiatorClient(String initiatorClient) {
        this.initiatorClient = initiatorClient;
        return this;
    }

    public ComputeEventMetadataBuilder eventUser(UserDetails userDetails) {
        this.eventUser = EventUser.of(userDetails.username(), userDetails.providerName());
        return this;
    }

    public ComputeEventMetadataBuilder nodeName(String nodeName) {
        this.nodeName = nodeName;
        return this;
    }

    @Override
    public String toString() {
        return S.toString(this);
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
