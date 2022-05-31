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

package org.apache.ignite.internal.pagememory.persistence.checkpoint;

import java.nio.ByteBuffer;
import org.apache.ignite.internal.pagememory.FullPageId;
import org.apache.ignite.internal.pagememory.persistence.store.PageStore;
import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Interface which allows writing one page to page store.
 */
public interface CheckpointPageWriter {
    /**
     * Writes the page to the page store.
     *
     * @param fullPageId Full page id.
     * @param buf Byte buffer to write from.
     * @param tag Page tag.
     * @return {@link PageStore} which was used to write.
     * @throws IgniteInternalCheckedException If failed.
     */
    PageStore write(FullPageId fullPageId, ByteBuffer buf, int tag) throws IgniteInternalCheckedException;
}
