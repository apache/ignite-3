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


import java.util.concurrent.CompletableFuture;

/**
 * Represents information of progress of a current checkpoint and allows obtaining future to wait for a particular checkpoint state.
 */
// TODO: IGNITE-16887 добавлять методы по мере надобности
public interface CheckpointProgress {
    /**
     * Returns future which can be used for detection when current checkpoint reaches the specific state.
     */
    CompletableFuture<?> futureFor(CheckpointState state);
}
