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

package org.apache.ignite.internal.storage.pagememory.mv;

/**
 * Collection of all page types that relate to multi-versioned partition storage.
 */
public interface MvPageTypes {
    /** Version chain tree meta page IO type. */
    short T_VERSION_CHAIN_META_IO = 9;

    /** Version chain tree inner page IO type. */
    short T_VERSION_CHAIN_INNER_IO = 10;

    /** Version chain tree leaf page IO type. */
    short T_VERSION_CHAIN_LEAF_IO = 11;

    /** Blob fragment page IO type. */
    short T_BLOB_FRAGMENT_IO = 13;

    /** Garbage collection queue meta page IO type. */
    short T_GC_META_IO = 14;

    /** Garbage collection queue inner page IO type. */
    short T_GC_INNER_IO = 15;

    /** Garbage collection queue leaf page IO type. */
    short T_GC_LEAF_IO = 16;
}
