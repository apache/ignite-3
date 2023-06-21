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

package org.apache.ignite.internal.metastorage;

import java.util.List;

/**
 * Manager that provides operations on the local meta storage.
 */
public interface LocalMetaStorageManager {
    /**
     * Returns all entries corresponding to given key and bounded by given revisions. All these entries have the same key.
     *
     * @param key The key.
     * @param revLowerBound The lower bound of revision.
     * @param revUpperBound The upper bound of revision.
     * @return Entries corresponding to the given key.
     */
    List<Entry> get(byte[] key, long revLowerBound, long revUpperBound);
}
