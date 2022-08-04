/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
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

package org.apache.ignite.internal.storage.index;

/**
 * Represents an Index row prefix, used to perform prefix scans over the Sorted Index storage.
 */
public interface IndexRowPrefix {
    /**
     * Returns a list of column values that comprise a prefix of an Index row. Values will be sorted in the same order as the
     * Sorted Index columns, specified by {@link SortedIndexDescriptor#indexColumns()}.
     */
    Object[] prefixColumnValues();
}
