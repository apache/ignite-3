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

    public ConnectCallInput(String url, @org.jetbrains.annotations.Nullable String username, @org.jetbrains.annotations.Nullable String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    String getUrl() {
        return url;
    }

    String getUsername() {
        return username;
    }

    String getPassword() {
        return password;
    }
}
