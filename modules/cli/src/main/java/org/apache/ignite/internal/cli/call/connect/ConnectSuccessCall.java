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

package org.apache.ignite.internal.cli.call.connect;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.config.CliConfigKeys;
import org.apache.ignite.internal.cli.config.StateConfigProvider;
import org.apache.ignite.internal.cli.core.call.Call;
import org.apache.ignite.internal.cli.core.call.CallOutput;
import org.apache.ignite.internal.cli.core.call.DefaultCallOutput;
import org.apache.ignite.internal.cli.core.repl.SessionInfo;
import org.apache.ignite.internal.cli.core.style.component.MessageUiComponent;
import org.apache.ignite.internal.cli.core.style.element.UiElements;
import org.apache.ignite.internal.cli.event.EventPublisher;
import org.apache.ignite.internal.cli.event.Events;

/**
 * Call which store connection info and notify all listeners about successful connection to the Ignite 3 node.
 */
@Singleton
public class ConnectSuccessCall implements Call<SessionInfo, String> {

    private final StateConfigProvider stateConfigProvider;

    private final EventPublisher eventPublisher;

    /**
     * Constructor.
     */
    public ConnectSuccessCall(StateConfigProvider stateConfigProvider, EventPublisher eventPublisher) {
        this.stateConfigProvider = stateConfigProvider;
        this.eventPublisher = eventPublisher;
    }

    @Override
    public CallOutput<String> execute(SessionInfo sessionInfo) {
        stateConfigProvider.get().setProperty(CliConfigKeys.LAST_CONNECTED_URL.value(), sessionInfo.nodeUrl());

        eventPublisher.publish(Events.connect(sessionInfo));

        return DefaultCallOutput.success(MessageUiComponent.fromMessage("Connected to %s",
                UiElements.url(sessionInfo.nodeUrl())).render());
    }
}
