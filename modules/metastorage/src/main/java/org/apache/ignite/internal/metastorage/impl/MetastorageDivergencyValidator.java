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

package org.apache.ignite.internal.metastorage.impl;

import org.apache.ignite.internal.metastorage.command.response.ChecksumInfo;

/**
 * Validates local Metastorage on divergency against the leader when a node tries to reenter the Metastorage group (after it missed
 * the Metastorage repair).
 */
public class MetastorageDivergencyValidator {
    /**
     * Validates local Metastorage against leader's Metastorage to detect if the local one has diverged.
     *
     * @param revision Current revision locally.
     * @param localChecksum Checksum corresponding to the local current revision.
     * @param leaderChecksumInfo Checksum info obtained from the leader.
     */
    public void validate(long revision, long localChecksum, ChecksumInfo leaderChecksumInfo) {
        if (leaderChecksumInfo.minRevision() == 0 || leaderChecksumInfo.maxRevision() == 0) {
            throw new MetastorageDivergedException("Metastorage on leader does not have any checksums, this should not happen");
        }

        if (revision >= leaderChecksumInfo.minRevision() && revision <= leaderChecksumInfo.maxRevision()) {
            if (localChecksum != leaderChecksumInfo.checksum()) {
                throw new MetastorageDivergedException(String.format(
                        "Metastorage has diverged [revision=%d, localChecksum=%d, leaderChecksum=%d",
                        revision, localChecksum, leaderChecksumInfo.checksum()
                ));
            }
        } else if (revision > leaderChecksumInfo.maxRevision()) {
            throw new MetastorageDivergedException(String.format(
                    "Node is ahead of the leader, this should not happen; probably means divergence [localRevision=%d, leaderRevision=%d]",
                    revision, leaderChecksumInfo.maxRevision()
            ));
        } else {
            assert revision < leaderChecksumInfo.minRevision();

            throw new MetastorageDivergedException(String.format(
                    "Node revision is already removed due to compaction on the leader [localRevision=%d, minLeaderRevision=%d]",
                    revision, leaderChecksumInfo.minRevision()
            ));
        }
    }
}
