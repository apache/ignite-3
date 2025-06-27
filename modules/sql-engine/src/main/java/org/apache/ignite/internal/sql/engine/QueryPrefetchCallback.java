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

package org.apache.ignite.internal.sql.engine;

import org.apache.ignite.internal.catalog.CatalogCommand;
import org.apache.ignite.internal.sql.engine.exec.ddl.DdlCommandHandler;
import org.apache.ignite.internal.sql.engine.exec.rel.AsyncRootNode;
import org.jetbrains.annotations.Nullable;

/**
 * Callback that is invoked when the query finishes prefetching. It is designed
 * to allow sequential execution of SQL statements that are dependent on each other.
 *
 * <ol>
 *     <li>For {@code DML} queries, it is called after the cursor has finished prefetching
 *     the initial batch of rows (see {@link AsyncRootNode#startPrefetch}).</li>
 *     <li>For {@code DDL} queries, it is called after the corresponding DDL
 *     command has completed (see {@link DdlCommandHandler#handle(CatalogCommand)}.</li>
 * </ol>
 *
 * <p>This callback is invoked asynchronously in the "{@code execution pool}".
 */
@FunctionalInterface
public interface QueryPrefetchCallback {
    /**
     * Called when the query finishes prefetching.
     *
     * @param ex Exceptional completion cause, or {@code null} if prefetch completed successfully.
     */
    void onPrefetchComplete(@Nullable Throwable ex);
}
