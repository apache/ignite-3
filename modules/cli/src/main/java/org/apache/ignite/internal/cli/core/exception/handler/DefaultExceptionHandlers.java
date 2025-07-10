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

package org.apache.ignite.internal.cli.core.exception.handler;

import org.apache.ignite.internal.cli.commands.cluster.init.ConfigAsPathExceptionHandler;
import org.apache.ignite.internal.cli.commands.cluster.init.ConfigParseExceptionHandler;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandlers;

/**
 * Default collection of exception handlers.
 */
public final class DefaultExceptionHandlers extends ExceptionHandlers {

    /**
     * Constructor.
     */
    public DefaultExceptionHandlers() {
        addDefaultHandlers();
    }

    /**
     * Constructor with custom default handler.
     *
     * @param defaultHandler defaultHandler.
     */
    public DefaultExceptionHandlers(ExceptionHandler<Throwable> defaultHandler) {
        super(defaultHandler);
        addDefaultHandlers();
    }

    private void addDefaultHandlers() {
        addExceptionHandler(new FlowInterruptExceptionHandler());
        addExceptionHandler(new TimeoutExceptionHandler());
        addExceptionHandler(new IgniteCliExceptionHandler());
        addExceptionHandler(new IgniteCliApiExceptionHandler());
        addExceptionHandler(new UnknownCommandExceptionHandler());
        addExceptionHandler(new ConfigStoringExceptionHandler());
        addExceptionHandler(new ProfileNotFoundExceptionHandler());
        addExceptionHandler(new SectionAlreadyExistsExceptionHandler());
        addExceptionHandler(new UnitNotFoundExceptionHandler());
        addExceptionHandler(new UnitAlreadyExistsExceptionHandler());
        addExceptionHandler(new FileNotFoundExceptionHandler());
        addExceptionHandler(new ConfigParseExceptionHandler());
        addExceptionHandler(new ConfigAsPathExceptionHandler());
        addExceptionHandler(new ConfigurationArgsParseExceptionHandler());
    }
}
