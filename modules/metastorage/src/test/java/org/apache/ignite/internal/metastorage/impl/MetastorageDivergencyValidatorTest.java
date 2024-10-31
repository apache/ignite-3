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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.ignite.internal.metastorage.command.response.ChecksumInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MetastorageDivergencyValidatorTest {
    private final MetastorageDivergencyValidator validator = new MetastorageDivergencyValidator();

    @Test
    void doesNothingIfLeaderHasNoChecksumsInfo() {
        MetastorageDivergedException ex = assertThrows(
                MetastorageDivergedException.class,
                () -> validator.validate(1, 42, checksumInfo(0, 0, 0))
        );

        assertThat(ex.getMessage(), is("Metastorage on leader does not have any checksums, this should not happen"));
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 5, 10})
    void validationSucceedsIfChecksumMatches(long revision) {
        assertDoesNotThrow(() -> validator.validate(revision, 42, checksumInfo(42, 1, 10)));
    }

    @ParameterizedTest
    @ValueSource(longs = {1, 5, 10})
    void validationFailsIfChecksumDoesNotMatchForKnownRevision(long revision) {
        MetastorageDivergedException ex = assertThrows(
                MetastorageDivergedException.class,
                () -> validator.validate(revision, 42, checksumInfo(123, 1, 10))
        );

        assertThat(ex.getMessage(), is("Metastorage has diverged [revision=" + revision + ", localChecksum=42, leaderChecksum=123"));
    }

    @Test
    void validationFailsIfNodeIsAheadLeader() {
        MetastorageDivergedException ex = assertThrows(
                MetastorageDivergedException.class,
                () -> validator.validate(11, 42, checksumInfo(0, 1, 10))
        );

        assertThat(ex.getMessage(), is("Node is ahead of the leader, this should not happen; probably means divergence "
                + "[localRevision=11, leaderRevision=10]"));
    }

    @Test
    void validationFailsIfNodeIsBehindCompactionRevision() {
        MetastorageDivergedException ex = assertThrows(
                MetastorageDivergedException.class,
                () -> validator.validate(9, 42, checksumInfo(0, 10, 20))
        );

        assertThat(
                ex.getMessage(),
                is("Node revision is already removed due to compaction on the leader [localRevision=9, minLeaderRevision=10]")
        );
    }

    private static ChecksumInfo checksumInfo(long checksum, long minRevision, long maxRevision) {
        return new ChecksumInfo(checksum, minRevision, maxRevision);
    }
}
