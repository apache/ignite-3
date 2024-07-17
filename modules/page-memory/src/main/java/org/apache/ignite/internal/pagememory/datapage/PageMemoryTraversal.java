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

package org.apache.ignite.internal.pagememory.datapage;

import org.apache.ignite.internal.pagememory.io.DataPagePayload;

/**
 * Controls page memory traversal.
 *
 * @param <T> Argument type.
 *
 * @see DataPageReader#traverse(long, PageMemoryTraversal, Object)
 */
public interface PageMemoryTraversal<T> {
    /**
     * Returned to signal that the traversal has to stop.
     */
    long STOP_TRAVERSAL = 0;

    /**
     * Consumes the currently traversed data payload and decides how to proceed.
     *
     * @param link     link to the current data payload
     * @param pageAddr address of the current page
     * @param payload  represents the row content
     * @param arg      argument passed to the traversal
     * @return next row link or {@link #STOP_TRAVERSAL} to stop the traversal
     */
    long consumePagePayload(long link, long pageAddr, DataPagePayload payload, T arg);

    /**
     * Called when the traversal is finished successfully.
     */
    default void finish() {
        // no-op
    }
}
