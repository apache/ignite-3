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

/**
 * An integration point that helps the mapper to acquire an execution target of particular
 * relation from fragment.
 */
@SuppressWarnings("InterfaceMayBeAnnotatedFunctional")
public interface ExecutionTargetProvider {
    /**
     * Returns an execution target for a table with given id.
     *
     * @param factory A factory to create target for given table.
     * @param tableId A table id to create execution target for.
     * @return A future representing the result.
     */
    CompletableFuture<ExecutionTarget> forTable(ExecutionTargetFactory factory, int tableId);
}
