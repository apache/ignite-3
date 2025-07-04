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

import static org.apache.ignite3.lang.ErrorGroups.Authentication.AUTHENTICATION_ERR_GROUP;
import static org.apache.ignite3.lang.ErrorGroups.Authentication.INVALID_CREDENTIALS_ERR;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.ignite3.client.IgniteClientConnectionException;
import org.apache.ignite3.internal.cli.core.exception.ExceptionHandler;
import org.apache.ignite3.internal.cli.core.exception.ExceptionWriter;
import org.apache.ignite3.internal.cli.core.style.component.ErrorUiComponent;
import org.apache.ignite3.internal.cli.core.style.component.ErrorUiComponent.ErrorComponentBuilder;
import org.apache.ignite3.lang.IgniteException;

/** Simple exception handler for {@link IgniteClientConnectionException}. */
public class IgniteClientConnectionExceptionHandler implements ExceptionHandler<IgniteClientConnectionException> {
    @Override
    public int handle(ExceptionWriter writer, IgniteClientConnectionException e) {
        ErrorComponentBuilder errorUiBuilder = ErrorUiComponent.builder()
                .header("Could not connect to cluster: " + e.getMessage());

        Throwable rootCause = ExceptionUtils.getRootCause(e);
        if (rootCause instanceof IgniteException) {
            IgniteException rootCauseIgn = (IgniteException) rootCause;
            if (rootCauseIgn.groupCode() == AUTHENTICATION_ERR_GROUP.groupCode() && rootCauseIgn.code() == INVALID_CREDENTIALS_ERR) {
                errorUiBuilder.header("Could not connect to cluster: Invalid client authentication credentials.")
                        .details("Please check the command help for more information on how to correctly supply the client credentials.");

            }
        }

        writer.write(errorUiBuilder.build().render());

        return e.code();
    }

    @Override
    public Class<IgniteClientConnectionException> applicableException() {
        return IgniteClientConnectionException.class;
    }
}
