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

package org.apache.ignite.internal.storage.pagememory.index;

/**
 * Collection of all page types that relate to indexes.
 */
public interface IndexPageTypes {
    /** Index meta tree meta IO type. */
    short T_INDEX_META_TREE_META_IO = 100;

    /** Index meta tree inner IO type. */
    short T_INDEX_META_INNER_IO = 101;

    /** Index meta tree leaf IO type. */
    short T_INDEX_META_LEAF_IO = 102;

    /** Hash index tree meta IO type. */
    short T_HASH_INDEX_META_IO = 103;

    /** Starting hash index tree inner IO type. No more than the {@link InlineUtils#MAX_BINARY_TUPLE_INLINE_SIZE}. */
    short T_HASH_INDEX_INNER_IO_START = 10_000;

    /** Starting hash index tree leaf IO type. No more than the {@link InlineUtils#MAX_BINARY_TUPLE_INLINE_SIZE}. */
    short T_HASH_INDEX_LEAF_IO_START = 15_000;

    /** Sorted index tree meta IO type. */
    short T_SORTED_INDEX_META_IO = 104;

    /** Starting sorted index tree inner IO type. No more than the {@link InlineUtils#MAX_BINARY_TUPLE_INLINE_SIZE}. */
    short T_SORTED_INDEX_INNER_IO_START = 20_000;

    /** Starting sorted index tree leaf IO type. No more than the {@link InlineUtils#MAX_BINARY_TUPLE_INLINE_SIZE}. */
    short T_SORTED_INDEX_LEAF_IO_START = 25_000;
}
