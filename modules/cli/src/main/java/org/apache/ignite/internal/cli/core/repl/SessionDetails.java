package org.apache.ignite.internal.cli.core.repl;

public class SessionDetails {
    private final String nodeUrl;

    private final String nodeName;

    private final String jdbcUrl;

    public SessionDetails() {
        this.nodeUrl = null;
        this.nodeName = null;
        this.jdbcUrl = null;
    }

    public SessionDetails(String nodeUrl, String nodeName, String jdbcUrl) {
        this.nodeUrl = nodeUrl;
        this.nodeName = nodeName;
        this.jdbcUrl = jdbcUrl;
    }

    public String nodeUrl() {
        return nodeUrl;
    }

    public String nodeName() {
        return nodeName;
    }

    public String jdbcUrl() {
        return jdbcUrl;
    }
}
