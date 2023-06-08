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

package org.apache.ignite.internal.cli.call.cluster.unit;

import jakarta.inject.Singleton;
import org.apache.ignite.internal.cli.core.call.ProgressTracker;
import org.apache.ignite.internal.cli.core.repl.registry.UnitsRegistry;

/** Factory for {@link DeployUnitCall} in REPL mode. */
@Singleton
public class DeployUnitReplCallFactory {
    private final DeployUnitCallFactory callFactory;

    private final UnitsRegistry registry;

    public DeployUnitReplCallFactory(DeployUnitCallFactory callFactory, UnitsRegistry registry) {
        this.callFactory = callFactory;
        this.registry = registry;
    }

    public DeployUnitReplCall create(ProgressTracker tracker) {
        return new DeployUnitReplCall(callFactory.create(tracker), registry);
    }
}
