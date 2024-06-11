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

package org.apache.ignite.internal.failure.handlers.configuration;

import org.apache.ignite.configuration.annotation.PolymorphicConfigInstance;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/**
 * Configuration schema for {@link org.apache.ignite.internal.failure.handlers.StopNodeOrHaltFailureHandler}.
 */
@PolymorphicConfigInstance(StopNodeOrHaltFailureHandlerConfigurationSchema.TYPE)
public class StopNodeOrHaltFailureHandlerConfigurationSchema extends FailureHandlerConfigurationSchema {
    /** Failure handler type name. */
    public static final String TYPE = "stopOrHalt";

    /** This field indicates that the failure handler should try to gracefully stop a node. */
    @Value(hasDefault = true)
    public boolean tryStop = false;

    /**
     * Timeout in ms that is used to gracefully stop a node before
     * JVM process will be terminated forcibly using {@code Runtime.getRuntime().halt()}.
     * The value {@code 0} means that the node will be stopped immediately.
     *
     * @see #tryStop
     */
    @Value(hasDefault = true)
    @Range(min = 0)
    public long timeout = 0;
}
