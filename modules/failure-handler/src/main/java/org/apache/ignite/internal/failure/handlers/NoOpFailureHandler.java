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

package org.apache.ignite.internal.failure.handlers;

import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.tostring.S;

/**
 * Just ignores any failure. It's useful for tests and debugging.
 */
public class NoOpFailureHandler extends AbstractFailureHandler {
    private static final IgniteLogger LOG = Loggers.forClass(NoOpFailureHandler.class);

    @Override
    protected boolean handle(FailureContext failureCtx) {
        LOG.error("FailureHandlerHandle " + failureCtx);
        return false;
    }

    @Override
    public String toString() {
        return S.toString(NoOpFailureHandler.class, this, super.toString());
    }
}
