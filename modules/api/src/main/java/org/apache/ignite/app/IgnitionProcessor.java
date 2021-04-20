/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.app;

import java.util.ServiceLoader;
import org.jetbrains.annotations.Nullable;

/**
 * Service loader based implementation of an entry point for handling grid lifecycle.
 */
public enum IgnitionProcessor implements Ignition {
    /** */
    INSTANCE;

    /** Loaded ignition instance. */
    private Ignition ignition;

    /** Constructor. */
    IgnitionProcessor() {
        ServiceLoader<Ignition> ldr = ServiceLoader.load(Ignition.class);
        // TODO IGNITE-14580 Add exception handling logic to IgnitionProcessor.
        ignition = ldr.iterator().next();
    }

    /** {@inheritDoc} */
    @Override public synchronized Ignite start(@Nullable String jsonStrBootstrapCfg) {
        // TODO IGNITE-14580 Add exception handling logic to IgnitionProcessor.
        return ignition.start(jsonStrBootstrapCfg);
    }
}
