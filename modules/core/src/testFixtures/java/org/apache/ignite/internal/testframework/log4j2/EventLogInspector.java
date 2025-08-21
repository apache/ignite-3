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

package org.apache.ignite.internal.testframework.log4j2;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.ignite.internal.testframework.log4j2.LogInspector.Handler;

/**
 * Helper class that listens for the messages from the EventLog logger and puts them into the list.
 */
public class EventLogInspector {
    private final List<String> events = new CopyOnWriteArrayList<>();

    private final LogInspector logInspector = new LogInspector(
            "EventLog", // From LogSinkConfigurationSchema.criteria default value
            new Handler(event -> true, event -> events.add(event.getMessage().getFormattedMessage()))
    );

    public void start() {
        events.clear();
        logInspector.start();
    }

    public void stop() {
        logInspector.stop();
    }

    public List<String> events() {
        return events;
    }
}
