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

package org.apache.ignite.internal.eventlog.config;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.LogSinkChange;
import org.apache.ignite.internal.eventlog.config.schema.LogSinkView;
import org.apache.ignite.internal.eventlog.config.schema.SinkView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ConfigurationExtension.class)
class EventLogConfigurationValidationTest {

    @InjectConfiguration
    private EventLogConfiguration cfg;

    @Test
    void defaultConfiguration() throws Exception {
        cfg.change(c -> c.changeSinks().create("logSink", s -> {
            LogSinkChange logSinkChange = (LogSinkChange) s.convert("log");
            logSinkChange.changeCriteria("EventLog");
            logSinkChange.changeLevel("INFO");
            logSinkChange.changeFormat("json");
        })).get();

        SinkView defaultLogSink = cfg.sinks().get("logSink").value();
        assertThat(defaultLogSink.id(), equalTo("log"));
        assertThat(defaultLogSink.name(), equalTo("logSink"));

        LogSinkView logSinkView = (LogSinkView) defaultLogSink;
        assertThat(logSinkView.criteria(), equalTo("EventLog"));
        assertThat(logSinkView.level(), equalTo("INFO"));
    }
}
