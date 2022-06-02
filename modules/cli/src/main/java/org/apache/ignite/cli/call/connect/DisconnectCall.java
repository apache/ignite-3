package org.apache.ignite.cli.call.connect;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.call.EmptyCallInput;
import org.apache.ignite.cli.core.repl.Session;

/**
 * Call for disconnect.
 */
@Singleton
public class DisconnectCall implements Call<EmptyCallInput, String> {

    @Inject
    private final Session session;

    public DisconnectCall(Session session) {
        this.session = session;
    }

    @Override
    public CallOutput<String> execute(EmptyCallInput input) {
        if (session.isConnectedToNode()) {
            String nodeUrl = session.getNodeUrl();
            session.setNodeUrl(null);
            session.setConnectedToNode(false);

            return DefaultCallOutput.success("Disconnected from " + nodeUrl);
        }

        return DefaultCallOutput.empty();
    }
}
