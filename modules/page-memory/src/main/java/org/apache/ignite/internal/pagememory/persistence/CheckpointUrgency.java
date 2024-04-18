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

/**
 * Checkpoint urgency status enum.
 */
public enum CheckpointUrgency {
    /**
     * Signifies that there's enough free space in page memory to allow taking checkpoint read locks, and there's no need to trigger
     * checkpoint.
     */
    NOT_REQUIRED,

    /**
     * Signifies that there's still enough free space in page memory to allow taking checkpoint read locks, but it's more limited and we're
     * at the point where we should schedule a checkpoint.
     */
    SHOULD_TRIGGER,

    /**
     * Signifies that there might not be enough free space in page memory to allow taking checkpoint read locks, and we should wait until
     * scheduled checkpoint is started and collection of dirty pages is empty once again.
     */
    MUST_TRIGGER
}
