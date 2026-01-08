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

package org.apache.ignite.internal.cli.core.repl.executor;

import io.micronaut.configuration.picocli.MicronautFactory;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.config.ConfigManagerProvider;
import org.jline.terminal.Terminal;
import picocli.shell.jline3.PicocliCommands.PicocliCommandsFactory;

/**
 * Provider of {@link ReplExecutor}.
 */
@Singleton
public class ReplExecutorProviderImpl implements ReplExecutorProvider {
    private PicocliCommandsFactory factory;

    @Inject
    private Terminal terminal;

    @Inject
    private ConfigManagerProvider configManagerProvider;

    @Override
    public ReplExecutor get() {
        return new ReplExecutorImpl(factory, terminal, configManagerProvider);
    }

    public void injectFactory(MicronautFactory micronautFactory) {
        factory = new PicocliCommandsFactory(micronautFactory);
        factory.setTerminal(terminal);
    }
}
