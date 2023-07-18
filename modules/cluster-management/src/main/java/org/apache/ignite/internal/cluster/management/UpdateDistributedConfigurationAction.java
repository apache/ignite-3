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

package org.apache.ignite.internal.cluster.management;

import static java.util.concurrent.CompletableFuture.completedFuture;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.jetbrains.annotations.Nullable;

/**
 * Action to update the distributed configuration.
 */
public class UpdateDistributedConfigurationAction {

    public static final UpdateDistributedConfigurationAction NOP = new UpdateDistributedConfigurationAction();

    /**
     * Configuration that should be applied.
     */
    private final String configuration;

    /**
     * The next action to execute.
     */
    private final Supplier<CompletableFuture<Void>> nextAction;

    /**
     * Constructor.
     *
     * @param configuration the configuration.
     * @param nextAction the next action.
     */
    public UpdateDistributedConfigurationAction(
            @Nullable String configuration,
            Supplier<CompletableFuture<Void>> nextAction) {
        this.configuration = configuration;
        this.nextAction = nextAction;
    }

    private UpdateDistributedConfigurationAction() {
        this(null, () -> completedFuture(null));
    }

    /**
     * Returns the configuration.
     *
     * @return the configuration.
     */
    public Optional<String> configuration() {
        return Optional.ofNullable(configuration);
    }

    /**
     * Returns the next action to execute.
     *
     * @return the next action.
     */
    public Supplier<CompletableFuture<Void>> nextAction() {
        return nextAction;
    }
}
