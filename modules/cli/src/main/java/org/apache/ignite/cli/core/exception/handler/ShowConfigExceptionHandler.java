/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.core.exception.handler;

import org.apache.ignite.cli.core.exception.ExceptionWriter;
import org.apache.ignite.cli.core.exception.IgniteCliApiException;
import org.apache.ignite.cli.core.style.component.ErrorUiComponent;
import org.apache.ignite.cli.core.style.element.UiElements;
import org.apache.ignite.rest.client.invoker.ApiException;

/**
 * This exception handler is used only for `cluster config show` command and handles a tricky case with the 500 error on not initialized
 * cluster.
 */
public class ShowConfigExceptionHandler extends IgniteCliApiExceptionHandler {
    @Override
    public int handle(ExceptionWriter err, IgniteCliApiException e) {
        if (e.getCause() instanceof ApiException) {
            ApiException apiException = (ApiException) e.getCause();
            if (apiException.getCode() == 500) { //TODO: https://issues.apache.org/jira/browse/IGNITE-17510
                err.write(
                        ErrorUiComponent.builder()
                                .header("Cannot show cluster config")
                                .details("Probably, you have not initialized the cluster, try to run %s command",
                                        UiElements.command("cluster init"))
                                .build()
                                .render()
                );
                return 1;
            }
        }
        return super.handle(err, e);
    }
}
