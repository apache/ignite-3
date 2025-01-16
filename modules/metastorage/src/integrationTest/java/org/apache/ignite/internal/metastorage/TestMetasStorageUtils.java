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

import static org.apache.ignite.internal.metastorage.impl.MetaStorageCompactionTriggerConfiguration.DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME;
import static org.apache.ignite.internal.metastorage.impl.MetaStorageCompactionTriggerConfiguration.INTERVAL_SYSTEM_PROPERTY_NAME;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.jetbrains.annotations.Nullable;

/** Helper class for use in integration tests that may contain useful methods and constants. */
public class TestMetasStorageUtils {
    /** Special value representing any random timestamp. */
    public static final HybridTimestamp ANY_TIMESTAMP = new HybridTimestamp(1L, 0);

    /** Foo key. */
    public static final ByteArray FOO_KEY = ByteArray.fromString("foo_key");

    /** Bar key. */
    public static final ByteArray BAR_KEY = ByteArray.fromString("bar_key");

    /** Random value. */
    public static final byte[] VALUE = ByteArray.fromString("value").bytes();

    /** Checks the metastore entry. */
    public static void checkEntry(Entry actEntry, byte[] expKey, byte @Nullable [] expValue, long expRevision) {
        assertEquals(expRevision, actEntry.revision(), () -> "entry=" + actEntry);
        assertArrayEquals(expKey, actEntry.key(), () -> "entry=" + actEntry);
        assertArrayEquals(expValue, actEntry.value(), () -> "entry=" + actEntry);
    }

    /** Returns {@code true} if entries are equal. */
    public static boolean equals(Entry act, Entry exp) {
        if (act.revision() != exp.revision()) {
            return false;
        }

        if (act.timestamp() != ANY_TIMESTAMP && exp.timestamp() != ANY_TIMESTAMP) {
            if (!Objects.equals(act.timestamp(), exp.timestamp())) {
                return false;
            }
        }

        if (!Arrays.equals(act.key(), exp.key())) {
            return false;
        }

        return Arrays.equals(act.value(), exp.value());
    }

    /** Creates a cluster configuration with metastorage compaction properties. */
    public static String createClusterConfigWithCompactionProperties(long interval, long dataAvailabilityTime) {
        return String.format(
                "ignite.system.properties: {"
                        + "%s = \"%s\", "
                        + "%s = \"%s\""
                        + "}",
                INTERVAL_SYSTEM_PROPERTY_NAME, interval, DATA_AVAILABILITY_TIME_SYSTEM_PROPERTY_NAME, dataAvailabilityTime
        );
    }

    /** Returns the latest revision for the key from the leader. */
    public static long latestKeyRevision(MetaStorageManager metaStorageManager, ByteArray key) {
        CompletableFuture<Entry> latestEntryFuture = metaStorageManager.get(key);
        assertThat(latestEntryFuture.thenApply(Entry::empty), willBe(false));

        return latestEntryFuture.join().revision();
    }

    /** Returns {@code true} if the metastorage key has only one revision in the cluster. */
    public static boolean allNodesContainSingleRevisionForKeyLocally(Cluster cluster, ByteArray key, long revision) {
        return collectRevisionForKeyForAllNodesLocally(cluster, key).values().stream()
                .allMatch(keyRevisions -> keyRevisions.size() == 1 && keyRevisions.contains(revision));
    }

    /** Returns a mapping of a node name to local key revisions. */
    public static Map<String, Set<Long>> collectRevisionForKeyForAllNodesLocally(Cluster cluster, ByteArray key) {
        return cluster.runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .collect(Collectors.toMap(IgniteImpl::name, ignite -> collectRevisionsLocally(ignite.metaStorageManager(), key)));
    }

    /** Returns a mapping of a node name to local compaction revisions. */
    public static Map<String, Long> collectCompactionRevisionForAllNodesLocally(Cluster cluster) {
        return cluster.runningNodes()
                .map(TestWrappers::unwrapIgniteImpl)
                .collect(Collectors.toMap(IgniteImpl::name, ignite -> ignite.metaStorageManager().getCompactionRevisionLocally()));
    }

    private static Set<Long> collectRevisionsLocally(MetaStorageManager metaStorageManager, ByteArray key) {
        var res = new HashSet<Long>();

        for (int i = 0; i <= metaStorageManager.appliedRevision(); i++) {
            try {
                Entry entry = metaStorageManager.getLocally(key, i);

                if (!entry.empty()) {
                    res.add(entry.revision());
                }
            } catch (CompactedException ignore) {
                // Do nothing.
            }
        }

        return res;
    }
}
