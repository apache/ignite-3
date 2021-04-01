package org.apache.ignite.raft.server;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandFuture;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;

public class CounterCommandListener implements RaftGroupCommandListener {
    /** */
    private AtomicInteger counter = new AtomicInteger();

    @Override public void onRead(Iterator<CommandFuture<ReadCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandFuture<ReadCommand> cmdFut = iterator.next();

            assert cmdFut.command() instanceof GetValueCommand;

            cmdFut.future().complete(counter.get());
        }
    }

    @Override public void onWrite(Iterator<CommandFuture<WriteCommand>> iterator) {
        while (iterator.hasNext()) {
            CommandFuture<WriteCommand> cmdFut = iterator.next();

            IncrementAndGetCommand cmd0 = (IncrementAndGetCommand) cmdFut.command();

            cmdFut.future().complete(counter.addAndGet(cmd0.delta));
        }
    }
}
