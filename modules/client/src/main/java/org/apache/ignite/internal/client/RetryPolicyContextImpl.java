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

package org.apache.ignite.internal.client;

import org.apache.ignite.client.ClientOperationType;
import org.apache.ignite.client.IgniteClientConfiguration;
import org.apache.ignite.client.IgniteClientConnectionException;
import org.apache.ignite.client.RetryPolicyContext;

/**
 * Retry policy context.
 */
public class RetryPolicyContextImpl implements RetryPolicyContext {
    /** Configuration. */
    private final IgniteClientConfiguration configuration;

    /** Operation type. */
    private final ClientOperationType operation;

    /** Iteration count. */
    private final int iteration;

    /** Exception that caused current iteration. */
    private final IgniteClientConnectionException exception;

    /**
     * Constructor.
     *
     * @param configuration Configuration.
     * @param operation Operation.
     * @param iteration Iteration.
     * @param exception Exception.
     */
    public RetryPolicyContextImpl(
            IgniteClientConfiguration configuration,
            ClientOperationType operation,
            int iteration,
            IgniteClientConnectionException exception) {
        this.configuration = configuration;
        this.operation = operation;
        this.iteration = iteration;
        this.exception = exception;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteClientConfiguration configuration() {
        return configuration;
    }

    /** {@inheritDoc} */
    @Override
    public ClientOperationType operation() {
        return operation;
    }

    /** {@inheritDoc} */
    @Override
    public int iteration() {
        return iteration;
    }

    /** {@inheritDoc} */
    @Override
    public IgniteClientConnectionException exception() {
        return exception;
    }
}
