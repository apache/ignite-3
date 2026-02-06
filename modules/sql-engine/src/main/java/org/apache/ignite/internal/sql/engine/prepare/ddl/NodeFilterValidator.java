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

package org.apache.ignite.internal.sql.engine.prepare.ddl;

import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Common validator for node filter. This is the best-effort validation because the topology may change before the actual
 * data nodes calculation that is done during the zone creation.
 */
@FunctionalInterface
public interface NodeFilterValidator {
    /**
     * Checks that provided node filter doesn't exclude all topology nodes from the set of zone's data nodes.
     *
     * @param nodeFilter Node filter to check.
     * @return A future that will be completed when validation is done.
     */
    CompletableFuture<Void> validate(@Nullable String nodeFilter);
}
