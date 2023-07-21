package org.apache.ignite.internal.cli.call.connect;

import javax.annotation.Nullable;
import org.apache.ignite.internal.cli.core.call.CallInput;

/** Input for the {@link ConnectCall} call. */
public class ConnectCallInput implements CallInput {

    private final String url;
    @Nullable
    private final String username;
    @Nullable
    private final String password;

    public ConnectCallInput(String url, @Nullable String username, @Nullable String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    String getUrl() {
        return url;
    }

    @Nullable
    String getUsername() {
        return username;
    }

    @Nullable
    String getPassword() {
        return password;
    }
}
