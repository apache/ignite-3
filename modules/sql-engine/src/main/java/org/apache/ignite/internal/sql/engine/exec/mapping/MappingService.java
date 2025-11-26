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

package org.apache.ignite.internal.sql.engine.exec.mapping;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.sql.engine.prepare.MultiStepPlan;

/**
 * A service to map multi step plan to an actual topology.
 */
@FunctionalInterface
public interface MappingService {
    /**
     * Maps given plan to a cluster topology.
     *
     * @param multiStepPlan A plan to map.
     * @return A list of fragments with metadata related to a fragment topology.
     */
    CompletableFuture<MappedFragments> map(MultiStepPlan multiStepPlan, MappingParameters parameters);
}
