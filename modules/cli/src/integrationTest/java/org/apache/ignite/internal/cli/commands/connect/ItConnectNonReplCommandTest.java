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

package org.apache.ignite.internal.cli.commands.connect;

import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Replaces;
import jakarta.inject.Inject;
import org.apache.ignite.internal.cli.ReplManager;
import org.apache.ignite.internal.cli.commands.TopLevelCliCommand;
import org.apache.ignite.internal.cli.core.repl.EventListeningActivationPoint;
import org.junit.jupiter.api.Test;

class ItConnectNonReplCommandTest extends ItConnectCommandTest {
    @Inject
    private EventListeningActivationPoint eventListeningActivationPoint;

    @Bean
    @Replaces(ReplManager.class)
    public ReplManager replManager() {
        return new ReplManager() {
            @Override
            public void subscribe() {
                eventListeningActivationPoint.subscribe();
            }

            @Override
            public void startReplMode() {
                // Emulate repl start by asking a question.
                question.askQuestionOnReplStart();
            }
        };
    }

    @Override
    protected Class<?> getCommandClass() {
        return TopLevelCliCommand.class;
    }

    @Override
    protected boolean needToSubscribe() {
        return false;
    }

    @Override
    @Test
    void disconnect() {
        // Not implemented.
    }
}
