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

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Composite action to update the distributed configuration.
 */
public class UpdateDistributedConfigurationAction {

    /**
     * Configuration that should be applied.
     */
    private final String configuration;

    private final Function<CompletableFuture<Void>, CompletableFuture<Void>> nextAction;


    /**
     * Constructor.
     *
     * @param configuration Configuration that should be applied.
     * @param nextAction The next action to be performed.
     */
    public UpdateDistributedConfigurationAction(
            @Nullable String configuration,
            Function<CompletableFuture<Void>, CompletableFuture<Void>> nextAction
    ) {
        this.configuration = configuration;
        this.nextAction = nextAction;
    }

    /**
     * Returns the configuration that should be applied.
     *
     * @return Configuration that should be applied.
     */
    @Nullable
    public String configuration() {
        return configuration;
    }

    /**
     * Returns the next action to be performed.
     *
     * @return The next action to be performed.
     */
    public Function<CompletableFuture<Void>, CompletableFuture<Void>> nextAction() {
        return nextAction;
    }
}
