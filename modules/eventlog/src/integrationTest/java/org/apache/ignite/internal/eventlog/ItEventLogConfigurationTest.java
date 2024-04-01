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

package org.apache.ignite.internal.eventlog;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.eventlog.config.schema.EventLogConfiguration;
import org.apache.ignite.internal.eventlog.config.schema.LogSinkChange;
import org.junit.jupiter.api.Test;

class ItEventLogConfigurationTest extends ClusterPerClassIntegrationTest {
    @Test
    void correctConfiguration() {
        assertDoesNotThrow(() -> CLUSTER.aliveNode().clusterConfiguration().change(c ->
                c.changeRoot(EventLogConfiguration.KEY).changeSinks().create("logSink", s -> {
                    // Configure the channel.
                    s.changeChannel("testChannel");

                    // Configure the log sink.
                    var logSinkChange = (LogSinkChange) s.convert("log");
                    logSinkChange.changeCriteria("EventLog");
                    logSinkChange.changeLevel("info");
                    logSinkChange.changeFormat("json");
                    logSinkChange.changeChannel("testChannel");
                })).get()
        );
    }
}
