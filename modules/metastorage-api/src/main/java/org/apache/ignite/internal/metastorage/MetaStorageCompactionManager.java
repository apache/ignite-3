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

import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.exceptions.MetaStorageException;

/** Contains methods for implementing metastorage compaction. */
// TODO: IGNITE-23293 Describe the metastore compaction algorithm
public interface MetaStorageCompactionManager extends IgniteComponent {
    /**
     * Compacts outdated key versions and removes tombstones of metastorage locally.
     *
     * <p>We do not compact the only and last version of the key unless it is a tombstone.</p>
     *
     * <p>Let's look at some examples, let's say we have the following keys with their versions:</p>
     * <ul>
     *     <li>Key "foo" with versions that have revisions (1, 3, 5) - "foo" [1, 3, 5].</li>
     *     <li>Key "bar" with versions that have revisions (1, 2, 5) the last revision is a tombstone - "bar" [1, 2, 5 tomb].</li>
     * </ul>
     *
     * <p>Let's look at examples of invoking the current method and what will be in the storage after:</p>
     * <ul>
     *     <li>Compaction revision is {@code 1}: "foo" [3, 5], "bar" [2, 5 tomb].</li>
     *     <li>Compaction revision is {@code 2}: "foo" [3, 5], "bar" [5 tomb].</li>
     *     <li>Compaction revision is {@code 3}: "foo" [5], "bar" [5 tomb].</li>
     *     <li>Compaction revision is {@code 4}: "foo" [5], "bar" [5 tomb].</li>
     *     <li>Compaction revision is {@code 5}: "foo" [5].</li>
     *     <li>Compaction revision is {@code 6}: "foo" [5].</li>
     * </ul>
     *
     * <p>Compaction revision is expected to be less than the current metastorage revision.</p>
     *
     * <p>Since the node may stop or crash, after restoring the node on its startup we need to run the compaction for the latest known
     * compaction revision.</p>
     *
     * <p>Compaction revision is not updated or saved.</p>
     *
     * @param revision Revision up to which (including) the metastorage keys will be compacted.
     * @throws IgniteInternalException with cause {@link NodeStoppingException} if the node is in the process of stopping.
     * @throws MetaStorageException If there is an error during the metastorage compaction process.
     * @see #setCompactionRevisionLocally(long)
     * @see #getCompactionRevisionLocally()
     */
    void compactLocally(long revision);

    /**
     * Sets the compaction revision locally, but does not save it, after invoking this method the metastorage read methods will throw a
     * {@link CompactedException} if they request a revision less than or equal to the new one.
     *
     * <p>Compaction revision is expected to be less than the current metastorage revision.</p>
     *
     * @param revision Compaction revision.
     * @throws IgniteInternalException with cause {@link NodeStoppingException} if the node is in the process of stopping.
     */
    void setCompactionRevisionLocally(long revision);

    /**
     * Returns the local compaction revision that was set or restored from a metastorage snapshot, {@code -1} if not changed.
     *
     * @throws IgniteInternalException with cause {@link NodeStoppingException} if the node is in the process of stopping.
     * @see #setCompactionRevisionLocally(long)
     */
    long getCompactionRevisionLocally();
}
