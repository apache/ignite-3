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

package org.apache.ignite.migrationtools.cli.exceptions;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.ignite3.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite3.internal.cli.core.exception.ExceptionHandlers;
import org.apache.ignite3.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite3.internal.cli.core.style.component.ErrorUiComponent;
import org.apache.ignite3.internal.cli.core.style.component.ErrorUiComponent.ErrorComponentBuilder;
import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.BeanInitializationException;

/** ExceptionHandlers for Error loading input configurations. */
public class ErrorLoadingInputConfigurationHandlers {
    /**
     * Create method.
     *
     * @return A new instance of these handlers.
     */
    public static ExceptionHandlers create() {
        var handlers = new ExceptionHandlers();
        handlers.addExceptionHandler(new Base<>(BeanInitializationException.class));
        handlers.addExceptionHandler(new Base<>(BeanDefinitionStoreException.class));
        return handlers;
    }

    private static class Base<T extends Throwable> implements ExceptionHandler<T> {
        private final Class<T> klass;

        Base(Class<T> klass) {
            this.klass = klass;
        }

        @Override
        public int handle(ExceptionWriter writer, T e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);

            ErrorComponentBuilder errorUiBuilder = ErrorUiComponent.builder()
                    .header("Error loading input configurations")
                    .details(rootCause.getMessage());

            writer.write(errorUiBuilder.build().render());

            return 1;
        }

        @Override
        public Class<T> applicableException() {
            return klass;
        }
    }
}
