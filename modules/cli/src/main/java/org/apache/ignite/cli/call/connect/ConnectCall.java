package org.apache.ignite.cli.call.connect;

import com.google.gson.Gson;
import jakarta.inject.Singleton;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.URL;
import org.apache.ignite.cli.core.call.Call;
import org.apache.ignite.cli.core.call.CallOutput;
import org.apache.ignite.cli.core.call.DefaultCallOutput;
import org.apache.ignite.cli.core.exception.ConnectCommandException;
import org.apache.ignite.cli.core.repl.Session;
import org.apache.ignite.cli.core.repl.config.RootConfig;
import org.apache.ignite.rest.client.api.NodeConfigurationApi;
import org.apache.ignite.rest.client.invoker.ApiClient;
import org.apache.ignite.rest.client.invoker.ApiException;
import org.apache.ignite.rest.client.invoker.Configuration;
import org.jetbrains.annotations.NotNull;


/**
 * Call for connect to Ignite 3 node.
 */
@Singleton
public class ConnectCall implements Call<ConnectCallInput, String> {

    private final Session session;

    public ConnectCall(Session session) {
        this.session = session;
    }

    @Override
    public CallOutput<String> execute(ConnectCallInput input) {
        NodeConfigurationApi api = createApiClient(input);
        String nodeUrl = input.getNodeUrl();
        try {
            String configuration = api.getNodeConfiguration();
            setJdbcUrl(configuration, nodeUrl);
        } catch (ApiException e) {
            session.setConnectedToNode(false);
            if (e.getCause() instanceof ConnectException) {
                return DefaultCallOutput.failure(new ConnectCommandException("Can not connect to " + input.getNodeUrl()));
            }
            return DefaultCallOutput.failure(e);
        }

        session.setNodeUrl(nodeUrl);
        session.setConnectedToNode(true);
        return DefaultCallOutput.success("Connected to " + nodeUrl);
    }

    @NotNull
    private NodeConfigurationApi createApiClient(ConnectCallInput input) {
        ApiClient client = Configuration.getDefaultApiClient();
        client.setBasePath(input.getNodeUrl());
        return new NodeConfigurationApi(client);
    }

    private void setJdbcUrl(String configuration, String nodeUrl) {
        try {
            String host = new URL(nodeUrl).getHost();
            RootConfig config = new Gson().fromJson(configuration, RootConfig.class);
            session.setJdbcUrl("jdbc:ignite:thin://" + host + ":" + config.clientConnector.port);
        } catch (MalformedURLException ignored) {
            // Shouldn't happen ever since we are now connected to this URL
        }
    }
}
