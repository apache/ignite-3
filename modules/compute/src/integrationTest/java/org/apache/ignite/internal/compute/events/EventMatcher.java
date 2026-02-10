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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import org.apache.ignite.internal.compute.events.ComputeEventMetadata.Type;
import org.apache.ignite.internal.compute.events.ComputeEventsFactory.FieldNames;
import org.apache.ignite.internal.compute.utils.MismatchesDescriptor;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.jetbrains.annotations.Nullable;

/**
 * Matcher for JSON serialized compute job events.
 */
public class EventMatcher extends TypeSafeMatcher<String> {
    private final Matcher<? super String> eventTypeMatcher;
    private Matcher<? super Long> timestampMatcher;
    private Matcher<? super String> productVersionMatcher;
    private Matcher<? super String> usernameMatcher;
    private Matcher<? super String> typeMatcher;
    private Matcher<? super String> classNameMatcher;
    private Matcher<? super String> tableNameMatcher;
    private Matcher<? super UUID> jobIdMatcher;
    private Matcher<? super UUID> taskIdMatcher;
    private Matcher<? super String> targetNodeMatcher;
    private Matcher<? super String> initiatorNodeMatcher;
    private Matcher<? super String> clientAddressMatcher;

    private EventMatcher(String eventType) {
        this.eventTypeMatcher = is(eventType);
    }

    static EventMatcher computeJobEvent(IgniteEventType eventType) {
        return new EventMatcher(eventType.name());
    }

    /**
     * Generates a matcher for compute job event originated from the embedded API.
     *
     * @param eventType Event type.
     * @param jobType Job type.
     * @param jobId Job ID.
     * @param jobClassName Job class name.
     * @param targetNode Target node name.
     * @param initiatorNode Initiator node name.
     * @return Constructed event matcher.
     */
    public static EventMatcher embeddedJobEvent(
            IgniteEventType eventType,
            Type jobType,
            @Nullable UUID jobId,
            String jobClassName,
            String targetNode,
            String initiatorNode
    ) {
        return computeJobEvent(eventType)
                .withTimestamp(notNullValue(Long.class))
                .withProductVersion(is(IgniteProductVersion.CURRENT_VERSION.toString()))
                .withUsername(is("SYSTEM"))
                .withType(jobType.name())
                .withClassName(jobClassName)
                .withJobId(jobId)
                .withTargetNode(targetNode)
                .withInitiatorNode(is(initiatorNode))
                .withClientAddress(nullValue());
    }

    /**
     * Generates a matcher for compute job event originated from the thin client API.
     *
     * @param eventType Event type.
     * @param jobType Job type.
     * @param jobId Job ID.
     * @param jobClassName Job class name.
     * @param targetNode Target node name.
     * @param initiatorNode Initiator node name.
     * @return Constructed event matcher.
     */
    public static EventMatcher thinClientJobEvent(
            IgniteEventType eventType,
            Type jobType,
            @Nullable UUID jobId,
            String jobClassName,
            String targetNode,
            String initiatorNode
    ) {
        return embeddedJobEvent(eventType, jobType, jobId, jobClassName, targetNode, initiatorNode)
                .withUsername(is("unknown"))
                .withClientAddress(notNullValue(String.class));
    }

    public EventMatcher withTimestamp(Matcher<? super Long> matcher) {
        this.timestampMatcher = matcher;
        return this;
    }

    public EventMatcher withProductVersion(Matcher<? super String> matcher) {
        this.productVersionMatcher = matcher;
        return this;
    }

    public EventMatcher withUsername(Matcher<? super String> matcher) {
        this.usernameMatcher = matcher;
        return this;
    }

    public EventMatcher withType(String type) {
        this.typeMatcher = is(type);
        return this;
    }

    public EventMatcher withClassName(String className) {
        this.classNameMatcher = is(className);
        return this;
    }

    public EventMatcher withTableName(String tableName) {
        this.tableNameMatcher = is(tableName);
        return this;
    }

    public EventMatcher withJobId(@Nullable UUID jobId) {
        this.jobIdMatcher = is(jobId);
        return this;
    }

    public EventMatcher withTaskId(@Nullable UUID taskId) {
        this.taskIdMatcher = is(taskId);
        return this;
    }

    public EventMatcher withTargetNode(String targetNode) {
        return withTargetNode(is(targetNode));
    }

    public EventMatcher withTargetNode(Matcher<? super String> targetNodeMatcher) {
        this.targetNodeMatcher = targetNodeMatcher;
        return this;
    }

    public EventMatcher withInitiatorNode(Matcher<? super String> initiatorNodeMatcher) {
        this.initiatorNodeMatcher = initiatorNodeMatcher;
        return this;
    }

    public EventMatcher withClientAddress(Matcher<? super String> clientAddressMatcher) {
        this.clientAddressMatcher = clientAddressMatcher;
        return this;
    }

    @Override
    protected boolean matchesSafely(String item) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            Event event = mapper.readValue(item, Event.class);
            return eventTypeMatcher.matches(event.type)
                    && (timestampMatcher == null || timestampMatcher.matches(event.timestamp))
                    && (productVersionMatcher == null || productVersionMatcher.matches(event.productVersion))
                    && (usernameMatcher == null || usernameMatcher.matches(event.user.username))
                    && (typeMatcher == null || typeMatcher.matches(event.fields.type))
                    && (classNameMatcher == null || classNameMatcher.matches(event.fields.className))
                    && (tableNameMatcher == null || tableNameMatcher.matches(event.fields.tableName))
                    && (jobIdMatcher == null || jobIdMatcher.matches(event.fields.jobId))
                    && (taskIdMatcher == null || taskIdMatcher.matches(event.fields.taskId))
                    && (targetNodeMatcher == null || targetNodeMatcher.matches(event.fields.targetNode))
                    && (initiatorNodeMatcher == null || initiatorNodeMatcher.matches(event.fields.initiatorNode))
                    && (clientAddressMatcher == null || clientAddressMatcher.matches(event.fields.clientAddress));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("event of type ").appendDescriptionOf(eventTypeMatcher);
        appendDescription(description, "timestamp", timestampMatcher);
        appendDescription(description, "product version", productVersionMatcher);
        appendDescription(description, "username", usernameMatcher);
        appendDescription(description, "type", typeMatcher);
        appendDescription(description, "class name", classNameMatcher);
        appendDescription(description, "table name", tableNameMatcher);
        appendDescription(description, "job ID", jobIdMatcher);
        appendDescription(description, "task ID", taskIdMatcher);
        appendDescription(description, "target node", targetNodeMatcher);
        appendDescription(description, "initiator node", initiatorNodeMatcher);
        appendDescription(description, "client address", clientAddressMatcher);
    }

    @Override
    protected void describeMismatchSafely(String item, Description mismatchDescription) {
        ObjectMapper mapper = new ObjectMapper();
        try {
            Event event = mapper.readValue(item, Event.class);
            new MismatchesDescriptor(mismatchDescription)
                    .describeMismatch(eventTypeMatcher, event.type, "event type")
                    .describeMismatch(timestampMatcher, event.timestamp, "timestamp")
                    .describeMismatch(productVersionMatcher, event.productVersion, "product version")
                    .describeMismatch(usernameMatcher, event.user.username, "username")
                    .describeMismatch(typeMatcher, event.fields.type, "type")
                    .describeMismatch(classNameMatcher, event.fields.className, "class name")
                    .describeMismatch(tableNameMatcher, event.fields.tableName, "table name")
                    .describeMismatch(jobIdMatcher, event.fields.jobId, "job ID")
                    .describeMismatch(taskIdMatcher, event.fields.taskId, "task ID")
                    .describeMismatch(targetNodeMatcher, event.fields.targetNode, "target node")
                    .describeMismatch(initiatorNodeMatcher, event.fields.initiatorNode, "initiator node")
                    .describeMismatch(clientAddressMatcher, event.fields.clientAddress, "client address");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private static <T> void appendDescription(Description description, String text, Matcher<T> matcher) {
        if (matcher != null) {
            description.appendText(" and ").appendText(text).appendText(" that ").appendDescriptionOf(matcher);
        }
    }

    @SuppressWarnings({"WeakerAccess", "unused"})
    static class Event {
        public String type;
        public long timestamp;
        public String productVersion;
        public User user;
        public Fields fields;

        static class User {
            public String username;
            public String authenticationProvider;
        }

        static class Fields {
            @JsonProperty(FieldNames.TYPE)
            public String type;

            @JsonProperty(FieldNames.CLASS_NAME)
            public String className;

            @JsonProperty(FieldNames.TABLE_NAME)
            public String tableName;

            @JsonProperty(FieldNames.JOB_ID)
            public UUID jobId;

            @JsonProperty(FieldNames.TASK_ID)
            public UUID taskId;

            @JsonProperty(FieldNames.TARGET_NODE)
            public String targetNode;

            @JsonProperty(FieldNames.INITIATOR_NODE)
            public String initiatorNode;

            @JsonProperty(FieldNames.CLIENT_ADDRESS)
            public String clientAddress;
        }
    }
}
