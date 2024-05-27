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

package org.apache.ignite.internal.sql.engine.exec;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.util.AsyncCursor;

/**
 * Asynchronous cursor that introduce ability to listen for certain events of cursor's lifecycle.
 *
 * @param <T> Type of the items.
 */
public interface AsyncDataCursor<T> extends AsyncCursor<T> {
    /**
     * Returns {@link CompletableFuture} that will be completed when cursor is closed.
     *
     * <p>The future will be completed with {@link null} if cursor was completed successfully,
     * or with an exception otherwise.
     *
     * @return A future representing result of operation.
     */
    CompletableFuture<Void> onClose();

    /**
     * Returns {@link CompletableFuture} that will be completed when first page is fetched and ready
     * to be returned to user.
     *
     * @return A future representing result of operation.
     */
    CompletableFuture<Void> onFirstPageReady();
}
