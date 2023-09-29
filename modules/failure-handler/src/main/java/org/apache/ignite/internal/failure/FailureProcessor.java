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

package org.apache.ignite.internal.failure;

import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * General failure processing API.
 */
public class FailureProcessor implements IgniteComponent {
    /** Handler. */
    private final FailureHandler handler;

    /** Node name. */
    private final String nodeName;

    /**
     * Creates a new instance of a failure processor.
     *
     * @param nodeName Node name.
     * @param handler Handler.
     */
    public FailureProcessor(String nodeName, FailureHandler handler) {
        this.nodeName = nodeName;
        this.handler = handler;
    }

    /** {@inheritDoc} */
    @Override
    public void start() {

    }

    /** {@inheritDoc} */
    @Override
    public void stop() {

    }

    /**
     * Processes failure accordingly to configured {@link FailureHandler}.
     *
     * @param failureCtx Failure context.
     * @return {@code True} If this very call led to Ignite node invalidation.
     */
    public boolean process(FailureContext failureCtx) {
        return process(failureCtx, handler);
    }

    /**
     * Processes failure accordingly to given failure handler.
     *
     * @param failureCtx Failure context.
     * @param handler Failure handler.
     * @return {@code True} If this very call led to Ignite node invalidation.
     */
    private boolean process(FailureContext failureCtx, FailureHandler handler) {
        return handler.onFailure(nodeName, failureCtx);
    }
}
