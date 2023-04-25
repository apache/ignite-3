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

package org.apache.ignite.internal.metastorage.command;

import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.raft.ReadCommand;
import org.jetbrains.annotations.Nullable;

/**
 * Base class for commands that retrieve values for an interval of keys.
 */
public interface PaginationCommand extends ReadCommand {
    /**
     * Returns the upper bound for entry revision or {@link MetaStorageManager#LATEST_REVISION} for no revision bound.
     */
    long revUpperBound();

    /**
     * Returns the boolean value indicating whether this range command is supposed to include tombstone entries into the cursor.
     */
    boolean includeTombstones();

    /**
     * Last retrieved key or {@code null} for the first ever request (used for pagination purposes).
     */
    byte @Nullable [] previousKey();

    /**
     * Max amount of values to retrieve.
     */
    int batchSize();
}
