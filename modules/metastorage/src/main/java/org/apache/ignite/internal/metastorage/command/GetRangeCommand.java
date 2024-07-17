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

import java.nio.ByteBuffer;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.jetbrains.annotations.Nullable;

/**
 * Range command for MetaStorageCommandListener that retrieves entries for the given key range in lexicographic order. Entries will be
 * filtered out by upper bound of given revision number.
 */
@Transferable(MetastorageCommandsMessageGroup.GET_RANGE)
public interface GetRangeCommand extends PaginationCommand {
    /**
     * Returns start key of range (inclusive).
     */
    ByteBuffer keyFrom();

    /**
     * Returns end key of range (exclusive), {@code null} means open upper bound.
     */
    @Nullable ByteBuffer keyTo();
}
