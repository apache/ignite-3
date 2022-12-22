package org.apache.ignite.internal.cli.core;

import java.net.MalformedURLException;
import java.net.URL;

/** Representation of Ignite JDBC url. */
public class JdbcUrl {
    private final String host;
    private final int port;

    /** Constructor. */
    private JdbcUrl(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /** Returns {@link JdbcUrl} with provided host and port. */
    public static JdbcUrl of(String url, int port) throws MalformedURLException {
        return new JdbcUrl(new URL(url).getHost(), port);
    }

    public String toString() {
        return "jdbc:ignite:thin://" + host + ":" + port;
    }
}
