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

package org.apache.ignite.internal.rest.events;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.security.authentication.Authentication;
import io.micronaut.security.filters.SecurityFilter;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.eventlog.api.IgniteEventType;
import org.apache.ignite.internal.eventlog.event.EventUser;
import org.apache.ignite.internal.rest.ResourceHolder;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * REST API event factory.
 */
public class RestEvents implements ResourceHolder {
    private EventLog eventLog;
    private String localNodeName;

    RestEvents(EventLog eventLog, String localNodeName) {
        this.eventLog = eventLog;
        this.localNodeName = localNodeName;
    }

    @Override
    public void cleanResources() {
        this.eventLog = null;
        this.localNodeName = null;
    }

    void logRequestStarted(HttpRequest<?> request) {
        long startTime = System.currentTimeMillis();
        request.setAttribute(Attributes.REQUEST_ID, UUID.randomUUID());
        request.setAttribute(Attributes.START_TIME, startTime);

        eventLog.log(
                IgniteEventType.REST_API_REQUEST_STARTED.name(),
                () -> {
                    Map<String, Object> fields = IgniteUtils.newLinkedHashMap(5);

                    fillCommonFields(fields, request);

                    return IgniteEventType.REST_API_REQUEST_STARTED.builder()
                            .user(extractEventUser(request))
                            .timestamp(startTime)
                            .fields(fields)
                            .build();
                }
        );
    }

    void logRequestFinished(HttpRequest<?> request, MutableHttpResponse<?> response) {
        long finishTime = System.currentTimeMillis();
        request.setAttribute(Attributes.FINISH_TIME, finishTime);

        eventLog.log(
                IgniteEventType.REST_API_REQUEST_FINISHED.name(),
                () -> {
                    Map<String, Object> fields = IgniteUtils.newLinkedHashMap(7);

                    fillCommonFields(fields, request);

                    fields.put(FieldNames.STATUS, response.getStatus().getCode());

                    return IgniteEventType.REST_API_REQUEST_FINISHED.builder()
                            .user(extractEventUser(request))
                            .timestamp(finishTime)
                            .fields(fields)
                            .build();
                }
        );
    }

    void logRequestError(HttpRequest<?> request, Throwable throwable) {
        long finishTime = System.currentTimeMillis();
        request.setAttribute(Attributes.FINISH_TIME, finishTime);

        eventLog.log(
                IgniteEventType.REST_API_REQUEST_FINISHED.name(),
                () -> {
                    Map<String, Object> fields = IgniteUtils.newLinkedHashMap(7);

                    fillCommonFields(fields, request);

                    // Log with 500 status for unhandled errors
                    fields.put(FieldNames.STATUS, 500);
                    fields.put(FieldNames.MESSAGE, throwable.getMessage());

                    return IgniteEventType.REST_API_REQUEST_FINISHED.builder()
                            .user(extractEventUser(request))
                            .timestamp(finishTime)
                            .fields(fields)
                            .build();
                }
        );
    }

    private void fillCommonFields(Map<String, Object> fields, HttpRequest<?> request) {
        fields.put(FieldNames.NODE_NAME, localNodeName);
        fields.put(FieldNames.METHOD, request.getMethod().name());
        fields.put(FieldNames.ENDPOINT, request.getPath());

        request.getAttribute(Attributes.REQUEST_ID, UUID.class)
                .ifPresent(id -> fields.put(FieldNames.REQUEST_ID, id.toString()));

        request.getAttribute(Attributes.START_TIME, Long.class)
                .ifPresent(startMillis -> {
                    fields.put(FieldNames.TIMESTAMP, Instant.ofEpochMilli(startMillis).toString());

                    request.getAttribute(Attributes.FINISH_TIME, Long.class)
                            .ifPresent(finishMillis -> fields.put(FieldNames.DURATION_MS, finishMillis - startMillis));
                });

    }

    private static EventUser extractEventUser(HttpRequest<?> request) {
        Optional<Authentication> authentication = request.getAttribute(
                SecurityFilter.AUTHENTICATION, Authentication.class);

        if (authentication.isPresent()) {
            String username = authentication.get().getName();
            return EventUser.of(username, "basic");
        }

        return EventUser.of("anonymous", "anonymous");
    }

    static class Attributes {
        static final String REQUEST_ID = "ignite.rest.request.id";
        static final String START_TIME = "ignite.rest.request.startTime";
        static final String FINISH_TIME = "ignite.rest.request.finishTime";
    }

    /** REST API events field names. */
    static class FieldNames {
        // Common fields.
        static final String TIMESTAMP = "timestamp";
        static final String REQUEST_ID = "requestId";
        static final String NODE_NAME = "nodeName";
        static final String METHOD = "method";
        static final String ENDPOINT = "endpoint";

        // Finish event fields.
        static final String STATUS = "status";
        static final String DURATION_MS = "durationMs";
        static final String MESSAGE = "message";
    }
}
