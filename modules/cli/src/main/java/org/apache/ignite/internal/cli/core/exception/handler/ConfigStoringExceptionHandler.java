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

import org.apache.ignite.internal.cli.config.exception.ConfigStoringException;
import org.apache.ignite.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite.internal.cli.core.style.component.ErrorUiComponent;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;

/**
 * Handler for {@link ConfigStoringException}.
 */
public class ConfigStoringExceptionHandler implements ExceptionHandler<ConfigStoringException> {
    private static final IgniteLogger log = Loggers.forClass(ConfigStoringExceptionHandler.class);

    @Override
    public int handle(ExceptionWriter err, ConfigStoringException e) {
        ErrorUiComponent errorComponent = ErrorUiComponent.builder()
                .header("Could not save CLI config")
                .details(e.getMessage())
                .build();

        log.error(errorComponent.header(), e);
        err.write(errorComponent.render());
        return 1;
    }

    @Override
    public Class<ConfigStoringException> applicableException() {
        return ConfigStoringException.class;
    }
}
