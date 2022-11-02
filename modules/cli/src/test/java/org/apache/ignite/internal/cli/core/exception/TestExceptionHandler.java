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

package org.apache.ignite.internal.cli.core.exception;

import org.apache.ignite.internal.cli.logger.CliLoggers;
import org.apache.ignite.internal.logger.IgniteLogger;

/** Exception handler that writes exception messages and logs verbose output. **/
public class TestExceptionHandler implements ExceptionHandler<RuntimeException> {
    private static final IgniteLogger LOG = CliLoggers.forClass(TestExceptionHandler.class);

    @Override
    public int handle(ExceptionWriter err, RuntimeException e) {
        err.write(e.getMessage());
        LOG.debug("verbose output");
        return 0;
    }

    @Override
    public Class<RuntimeException> applicableException() {
        return RuntimeException.class;
    }
}
