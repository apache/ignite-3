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

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.eventlog.api.EventLog;
import org.apache.ignite.internal.rest.RestFactory;

/**
 * Factory that creates beans needed for REST API event logging.
 */
@Factory
public class RestEventsFactory implements RestFactory {
    private EventLog eventLog;
    private String localNodeName;

    /** Constructor. */
    public RestEventsFactory(EventLog eventLog, String localNodeName) {
        this.eventLog = eventLog;
        this.localNodeName = localNodeName;
    }

    @Bean
    @Singleton
    public RestEvents restEvents() {
        return new RestEvents(eventLog, localNodeName);
    }

    @Override
    public void cleanResources() {
        this.eventLog = null;
        this.localNodeName = null;
    }
}
