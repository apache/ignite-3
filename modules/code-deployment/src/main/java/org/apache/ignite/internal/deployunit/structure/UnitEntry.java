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

package org.apache.ignite.internal.deployunit.structure;

/**
 * Represents an entry in a deployment unit's content structure.
 *
 * <p>A unit entry can be either a file or a folder within a deployment unit.
 * This interface provides basic operations to access the entry's metadata such as name and size.
 *
 * @see UnitFile
 * @see UnitFolder
 */
public interface UnitEntry {
    /**
     * Returns the name of this entry.
     *
     * <p>For files, this is the file name. For folders, this is the folder name.
     *
     * @return the entry name, never {@code null}
     */
    String name();

    /**
     * Returns the size of this entry in bytes.
     *
     * <p>For files, this is the actual file size. For folders, this is the sum
     * of all children sizes (recursive).
     *
     * @return the entry size in bytes, always non-negative
     */
    long size();
}
