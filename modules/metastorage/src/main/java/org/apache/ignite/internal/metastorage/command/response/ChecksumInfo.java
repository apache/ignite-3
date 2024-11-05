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

package org.apache.ignite.internal.metastorage.command.response;

import java.io.Serializable;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.metastorage.server.ChecksumAndRevisions;
import org.apache.ignite.internal.tostring.S;

/**
 * Information about checksum and revisions of the Metastorage.
 */
public class ChecksumInfo implements Serializable {
    private static final long serialVersionUID = 8681846172504003981L;

    private final long checksum;
    private final long minRevision;
    private final long maxRevision;

    private final long compactionRevision;

    /**
     * Constructor.
     */
    public ChecksumInfo(long checksum, long minRevision, long maxRevision, long compactionRevision) {
        this.checksum = checksum;
        this.minRevision = minRevision;
        this.maxRevision = maxRevision;
        this.compactionRevision = compactionRevision;
    }

    /**
     * The checksum corresponding to the requested revision, or 0 if it does not fit the [{@link #minRevision()}-{@link #maxRevision()}]
     * interval.
     */
    public long checksum() {
        return checksum;
    }

    /**
     * Minimum revision for which the leader has checksum info (or 0 if the cluster has not been yet initialized).
     * This is usually first not-yet-compacted revision.
     */
    public long minRevision() {
        return minRevision;
    }

    /**
     * Maximum revision for which the leader has checksum info (this matches current revision).
     */
    public long maxRevision() {
        return maxRevision;
    }

    /**
     * Returns metastorage compaction revision of the up to which (inclusive) key versions will be deleted and when trying to read them,
     * {@link CompactedException} will occur, {@code -1} if it has never been updated.
     */
    public long compactionRevision() {
        return compactionRevision;
    }

    /** Converts to {@link ChecksumInfo}. */
    public static ChecksumInfo of(ChecksumAndRevisions checksumAndRevisions) {
        return new ChecksumInfo(
                checksumAndRevisions.checksum(),
                checksumAndRevisions.minChecksummedRevision(),
                checksumAndRevisions.maxChecksummedRevision(),
                checksumAndRevisions.compactionRevision()
        );
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
