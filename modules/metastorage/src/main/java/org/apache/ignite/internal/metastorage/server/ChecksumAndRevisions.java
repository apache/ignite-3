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

/**
 * Information about a checksum and checksummed revisions.
 */
public class ChecksumAndRevisions {
    private final long checksum;
    private final long minChecksummedRevision;
    private final long maxChecksummedRevision;

    /**
     * Constructor.
     */
    public ChecksumAndRevisions(long checksum, long minChecksummedRevision, long maxChecksummedRevision) {
        this.checksum = checksum;
        this.minChecksummedRevision = minChecksummedRevision;
        this.maxChecksummedRevision = maxChecksummedRevision;
    }

    /** Checksum (or 0 if there is no checksum for the requested revision). */
    public long checksum() {
        return checksum;
    }

    /** Min revision that has a checksum (0 if there are no such revisions). */
    public long minChecksummedRevision() {
        return minChecksummedRevision;
    }

    /** Max revision that has a checksum (0 if there are no such revisions). */
    public long maxChecksummedRevision() {
        return maxChecksummedRevision;
    }
}
