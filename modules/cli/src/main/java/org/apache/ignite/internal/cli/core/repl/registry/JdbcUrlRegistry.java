package org.apache.ignite.internal.cli.core.repl.registry;

import java.util.Set;

public interface JdbcUrlRegistry {

    /** Returns set of jdbc urls. */
    Set<String> jdbcUrls();
}
