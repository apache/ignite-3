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

import org.apache.ignite.lang.IgniteInternalCheckedException;

/**
 * Listener which methods will be called in a corresponded checkpoint life cycle period.
 */
public interface CheckpointListener {
    /**
     * Callback at the checkpoint start mark.
     *
     * <p>Holds checkpoint write lock.
     *
     * @param progress Progress of the current checkpoint.
     * @throws IgniteInternalCheckedException If failed.
     */
    default void onMarkCheckpointBegin(CheckpointProgress progress) throws IgniteInternalCheckedException {
    }

    /**
     * Callback at the beginning of the checkpoint.
     *
     * <p>After release checkpoint write lock.
     *
     * @param progress Progress of the current checkpoint.
     * @throws IgniteInternalCheckedException If failed.
     */
    default void onCheckpointBegin(CheckpointProgress progress) throws IgniteInternalCheckedException {
    }

    /**
     * Callback before the start of the checkpoint.
     *
     * <p>Holds checkpoint read lock.
     *
     * @param progress Progress of the current checkpoint.
     * @throws IgniteInternalCheckedException If failed.
     */
    default void beforeCheckpointBegin(CheckpointProgress progress) throws IgniteInternalCheckedException {
    }

    /**
     * Callback after checkpoint ends.
     *
     * <p>Does not hold checkpoint read and write locks.
     *
     * @param progress Progress of the current checkpoint.
     * @throws IgniteInternalCheckedException If failed.
     */
    default void afterCheckpointEnd(CheckpointProgress progress) throws IgniteInternalCheckedException {
    }
}
