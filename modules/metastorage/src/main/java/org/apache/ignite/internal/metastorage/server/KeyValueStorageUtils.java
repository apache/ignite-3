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

package org.apache.ignite.internal.metastorage.server;

/** Helper class with useful methods and constants for {@link KeyValueStorage} implementations. */
public class KeyValueStorageUtils {
    /** Asserts that the compaction revision is less than the current repository revision. */
    public static void assertCompactionRevisionLessCurrent(long compactionRevision, long revision) {
        assert compactionRevision < revision : String.format(
                "Compaction revision should be less than the current: [compaction=%s, current=%s]",
                compactionRevision, revision
        );
    }
}
