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

package org.apache.ignite.internal.compute.configuration;

import static java.lang.Math.max;

import java.util.concurrent.TimeUnit;
import org.apache.ignite.configuration.annotation.ConfigurationRoot;
import org.apache.ignite.configuration.annotation.ConfigurationType;
import org.apache.ignite.configuration.annotation.Value;
import org.apache.ignite.configuration.validation.Range;

/**
 * Configuration schema for Compute functionality.
 */
@SuppressWarnings("PMD.UnusedPrivateField")
@ConfigurationRoot(rootName = "compute", type = ConfigurationType.LOCAL)
public class ComputeConfigurationSchema {
    /** Job thread pool size. */
    @Range(min = 1)
    @Value(hasDefault = true)
    public final int threadPoolSize = max(Runtime.getRuntime().availableProcessors(), 8);

    /** Job thread pool stop timeout (milliseconds). */
    @Range(min = 1)
    @Value(hasDefault = true)
    public final long threadPoolStopTimeoutMillis = 10_000;

    /**
     * Job queue max size.
     */
    @Range(min = 1)
    @Value(hasDefault = true)
    public final int queueMaxSize = Integer.MAX_VALUE;

    /**
     * The lifetime of job states in milliseconds after the Compute job finishes.
     */
    @Range(min = 0)
    @Value(hasDefault = true)
    public final long statesLifetimeMillis = TimeUnit.MINUTES.toMillis(1);
}
