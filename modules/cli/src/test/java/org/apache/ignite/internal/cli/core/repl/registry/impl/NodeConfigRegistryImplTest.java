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

package org.apache.ignite.internal.cli.core.repl.registry.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.typesafe.config.ConfigFactory;
import java.util.Collections;
import org.apache.ignite.internal.cli.call.configuration.JsonString;
import org.apache.ignite.internal.cli.call.configuration.NodeConfigShowCall;
import org.apache.ignite.internal.cli.call.configuration.NodeConfigShowCallInput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.repl.Session;
import org.apache.ignite.internal.cli.core.repl.SessionContext;
import org.junit.jupiter.api.Test;

class NodeConfigRegistryImplTest {

    @Test
    void fetchesConfigurationOnConnect() {
        // Given
        NodeConfigShowCall call = mock(NodeConfigShowCall.class);
        NodeConfigRegistryImpl nodeConfigRegistry = new NodeConfigRegistryImpl(call);
        Session session = new Session(Collections.singletonList(nodeConfigRegistry));

        String config = "{id:1}";
        NodeConfigShowCallInput callInput = NodeConfigShowCallInput.builder().nodeUrl("nodeUrl").build();
        when(call.execute(eq(callInput))).thenReturn(DefaultCallOutput.success(JsonString.fromString(config)));

        // Then
        SessionContext context = new SessionContext("nodeUrl", "nodeName", "jdbc");
        session.connect(context);
        verify(call, times(1)).execute(callInput);
        assertEquals(ConfigFactory.parseString(config), nodeConfigRegistry.config());
    }
}
