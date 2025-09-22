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

import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_CANCELED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_CANCELING;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_COMPLETED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_EXECUTING;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_FAILED;
import static org.apache.ignite.internal.eventlog.api.IgniteEventType.COMPUTE_JOB_QUEUED;

import java.util.Map;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.util.IgniteUtils;
import org.jetbrains.annotations.Nullable;

/**
 * Compute events factory.
 */
public class ComputeEventsFactory {
    /**
     * Logs compute job queued event.
     *
     * @param eventLog Event log.
     * @param eventMetadata Event metadata.
     */
    public static void logJobQueuedEvent(EventLog eventLog, @Nullable ComputeEventMetadata eventMetadata) {
        logEvent(eventLog, COMPUTE_JOB_QUEUED, eventMetadata);
    }

    /**
     * Logs compute job executing event.
     *
     * @param eventLog Event log.
     * @param eventMetadata Event metadata.
     */
    public static void logJobExecutingEvent(EventLog eventLog, @Nullable ComputeEventMetadata eventMetadata) {
        logEvent(eventLog, COMPUTE_JOB_EXECUTING, eventMetadata);
    }

    /**
     * Logs compute job failed event.
     *
     * @param eventLog Event log.
     * @param eventMetadata Event metadata.
     */
    public static void logJobFailedEvent(EventLog eventLog, @Nullable ComputeEventMetadata eventMetadata) {
        logEvent(eventLog, COMPUTE_JOB_FAILED, eventMetadata);
    }

    /**
     * Logs compute job completed event.
     *
     * @param eventLog Event log.
     * @param eventMetadata Event metadata.
     */
    public static void logJobCompletedEvent(EventLog eventLog, @Nullable ComputeEventMetadata eventMetadata) {
        logEvent(eventLog, COMPUTE_JOB_COMPLETED, eventMetadata);
    }

    /**
     * Logs compute job canceling event.
     *
     * @param eventLog Event log.
     * @param eventMetadata Event metadata.
     */
    public static void logJobCancelingEvent(EventLog eventLog, @Nullable ComputeEventMetadata eventMetadata) {
        logEvent(eventLog, COMPUTE_JOB_CANCELING, eventMetadata);
    }

    /**
     * Logs compute job canceled event.
     *
     * @param eventLog Event log.
     * @param eventMetadata Event metadata.
     */
    public static void logJobCanceledEvent(EventLog eventLog, @Nullable ComputeEventMetadata eventMetadata) {
        logEvent(eventLog, COMPUTE_JOB_CANCELED, eventMetadata);
    }

    /**
     * Logs compute job event.
     *
     * @param eventLog Event log.
     * @param eventType Event type.
     * @param eventMetadata Event metadata.
     */
    public static void logEvent(EventLog eventLog, IgniteEventType eventType, @Nullable ComputeEventMetadata eventMetadata) {
        if (eventMetadata == null) {
            return;
        }

        eventLog.log(eventType.name(), () -> {
            Map<String, Object> fields = IgniteUtils.newLinkedHashMap(8);

            fields.put(FieldNames.TYPE, eventMetadata.type());
            fields.put(FieldNames.CLASS_NAME, eventMetadata.jobClassName());
            fields.put(FieldNames.TARGET_NODE, eventMetadata.targetNode());
            fields.put(FieldNames.INITIATOR_NODE, eventMetadata.initiatorNode());

            putIfNotNull(fields, FieldNames.JOB_ID, eventMetadata.jobId()); // Could be null for map reduce tasks
            putIfNotNull(fields, FieldNames.TASK_ID, eventMetadata.taskId());
            putIfNotNull(fields, FieldNames.TABLE_NAME, eventMetadata.tableName());
            putIfNotNull(fields, FieldNames.CLIENT_ADDRESS, eventMetadata.clientAddress());

            return eventType.builder()
                    .user(eventMetadata.eventUser())
                    .timestamp(System.currentTimeMillis())
                    .fields(fields)
                    .build();

        });
    }

    private static void putIfNotNull(Map<String, Object> map, String key, @Nullable Object value) {
        if (value != null) {
            map.put(key, value);
        }
    }

    /** Compute events field names. */
    static class FieldNames {
        // Required fields
        static final String TYPE = "type";
        static final String CLASS_NAME = "className";
        static final String JOB_ID = "jobId";
        static final String TARGET_NODE = "targetNode";
        static final String INITIATOR_NODE = "initiatorNode";

        // Optional fields
        static final String TASK_ID = "taskId";
        static final String TABLE_NAME = "tableName";
        static final String CLIENT_ADDRESS = "clientAddress";
    }
}
