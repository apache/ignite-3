package org.apache.ignite.internal.datareplication;

import java.nio.file.Path;
import java.util.Iterator;
import java.util.function.Consumer;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;

public class PartitionGroupListener implements RaftGroupListener {

    @Override
    public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        // No-op
    }

    @Override
    public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
        // No-op
    }

    @Override
    public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        throw new UnsupportedOperationException("Snapshotting is not implemented");
    }

    @Override
    public boolean onSnapshotLoad(Path path) {
        throw new UnsupportedOperationException("Snapshotting is not implemented");
    }

    @Override
    public void onShutdown() {
        // No-op
    }
}
