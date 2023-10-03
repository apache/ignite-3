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

package org.apache.ignite.internal.pagememory.persistence;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.lang.IgniteInternalCheckedException;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.store.PageStore;

/**
 * Interface for write page to {@link PageStore}.
 */
public interface PageStoreWriter {
    /**
     * Callback for write page. {@link PersistentPageMemory} will copy page content to buffer before call.
     *
     * @param fullPageId Page ID to get byte buffer for. The page ID must be present in the collection returned by the {@link
     * PersistentPageMemory#beginCheckpoint(CompletableFuture)} method call.
     * @param buf Temporary buffer to write changes into.
     * @param tag {@code Partition generation} if data was read.
     * @throws IgniteInternalCheckedException If write page failed.
     */
    void writePage(FullPageId fullPageId, ByteBuffer buf, int tag) throws IgniteInternalCheckedException;
}
