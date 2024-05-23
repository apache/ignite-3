package org.apache.ignite.internal.datareplication.snapshot;

import org.apache.ignite.internal.raft.storage.SnapshotStorageFactory;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.option.SnapshotCopierOptions;
import org.apache.ignite.raft.jraft.storage.SnapshotStorage;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotCopier;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;

/**
 * Temporary snapshot factory, which must help to identify the unexpected snapshots,
 * while zone based replicas is not support snapshotting yet.
 */
// TODO https://issues.apache.org/jira/browse/IGNITE-22115 remove it
public class FailFastSnapshotStorageFactory implements SnapshotStorageFactory {
    @Override
    public SnapshotStorage createSnapshotStorage(String uri, RaftOptions raftOptions) {
        return new SnapshotStorage() {

            private <T> T fail() {
                throw new UnsupportedOperationException("Snapshotting is not implemented yet for the zone based partitions");
            }

            @Override
            public boolean setFilterBeforeCopyRemote() {
                return fail();
            }

            @Override
            public SnapshotWriter create() {
                return fail();
            }

            @Override
            public SnapshotReader open() {
                return null;
            }

            @Override
            public SnapshotReader copyFrom(String uri, SnapshotCopierOptions opts) {
                return fail();
            }

            @Override
            public SnapshotCopier startToCopyFrom(String uri, SnapshotCopierOptions opts) {
                return fail();
            }

            @Override
            public void setSnapshotThrottle(SnapshotThrottle snapshotThrottle) {
                // No-op
            }

            @Override
            public boolean init(Void opts) {
                return true;
            }

            @Override
            public void shutdown() {
                // No-op
            }
        };
    }
}
