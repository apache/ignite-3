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

package org.apache.ignite.internal.table.distributed.raft.snapshot;

import java.util.UUID;

/**
 * Snapshot URI.
 */
public class SnapshotUri {
    /** Length of UUID's sting representation in characters. Includes 32 hexadecimals and 4 dashes. */
    private static final int UUID_LENGTH = 36;

    /**
     * Creates a string representation of the snapshot URI.
     *
     * @param snapshotId Snapshot id.
     * @param nodeName Sender node (consistent id) name.
     */
    public static String toStringUri(UUID snapshotId, String nodeName) {
        return nodeName + "-" + snapshotId;
    }

    /**
     * Parses a string representation of the snapshot URI.
     *
     * @param uri Snapshot URI.
     */
    public static SnapshotUri fromStringUri(String uri) {
        UUID snapshotId = UUID.fromString(uri.substring(uri.length() - UUID_LENGTH));

        String nodeName = uri.substring(0, uri.length() - UUID_LENGTH - 1);

        return new SnapshotUri(snapshotId, nodeName);
    }

    /** Snapshot id. */
    public final UUID snapshotId;

    /** Sender node (consistent id) name. */
    public final String nodeName;

    /**
     * Constructor.
     *
     * @param snapshotId Snapshot id.
     * @param nodeName Sender node name.
     */
    private SnapshotUri(UUID snapshotId, String nodeName) {
        this.snapshotId = snapshotId;
        this.nodeName = nodeName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SnapshotUri that = (SnapshotUri) o;

        if (!snapshotId.equals(that.snapshotId)) {
            return false;
        }
        return nodeName.equals(that.nodeName);
    }

    @Override
    public int hashCode() {
        int result = snapshotId.hashCode();
        result = 31 * result + nodeName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return toStringUri(snapshotId, nodeName);
    }
}
