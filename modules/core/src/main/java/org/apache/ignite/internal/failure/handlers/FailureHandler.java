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

import java.util.Set;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureProcessor;
import org.apache.ignite.internal.failure.FailureType;

/**
 * Provides facility to handle failures.
 */
public interface FailureHandler {
    /**
     * Handles failure occurred on {@code ignite} instance. Failure details is contained in {@code failureCtx}. Returns {@code true} if
     * Ignite node must be invalidated by {@link FailureProcessor} after calling this method.
     *
     * @param failureCtx Failure context.
     * @return Whether Ignite node must be invalidated or not.
     */
    boolean onFailure(FailureContext failureCtx);

    /**
     * Returns unmodifiable set of ignored failure types.
     */
    Set<FailureType> ignoredFailureTypes();
}
