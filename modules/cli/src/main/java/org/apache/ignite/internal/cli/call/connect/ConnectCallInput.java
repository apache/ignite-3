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

    private ConnectCallInput(String url, @Nullable String username, @Nullable String password) {
        this.url = url;
        this.username = username;
        this.password = password;
    }

    String url() {
        return url;
    }

    @Nullable
    String username() {
        return username;
    }

    @Nullable
    String password() {
        return password;
    }

    /**
     * Builder method provider.
     *
     * @return new instance of {@link ConnectCallInputBuilder}.
     */
    public static ConnectCallInputBuilder builder() {
        return new ConnectCallInputBuilder();
    }

    public static class ConnectCallInputBuilder {

        private String url;
        @Nullable
        private String username;
        @Nullable
        private String password;

        private ConnectCallInputBuilder() {
        }

        public ConnectCallInputBuilder url(String url) {
            this.url = url;
            return this;
        }

        public ConnectCallInputBuilder username(@Nullable String username) {
            this.username = username;
            return this;
        }

        public ConnectCallInputBuilder password(@Nullable String password) {
            this.password = password;
            return this;
        }
        
        public ConnectCallInput build() {
            return new ConnectCallInput(url, username, password);
        }
    }
}
