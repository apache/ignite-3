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

package org.apache.ignite.internal.catalog;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.manager.IgniteComponent;

/**
 * The catalog manager provides schema manipulation methods and is responsible for managing distributed operations.
 */
public interface CatalogManager extends IgniteComponent, CatalogService {
    /**
     * Executes given command.
     *
     * @param command Command to execute.
     * @return Future representing result of execution (it will be completed with the created catalog version).
     */
    CompletableFuture<Integer> execute(CatalogCommand command);

    /**
     * Executes given list of commands atomically. That is, either all commands will be applied at once
     * or neither of them. The whole bulk will increment catalog's version by a single point.
     *
     * @param commands Commands to execute.
     * @return Future representing result of execution (it will be completed with the created catalog version).
     */
    CompletableFuture<Integer> execute(List<CatalogCommand> commands);
}
