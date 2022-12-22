package org.apache.ignite.internal.cli.core.repl.registry;

import java.net.URL;
import java.util.Optional;
import java.util.Set;

public interface NodeNameRegistry {

    /** Returns a node url by a provided node name. */
    Optional<URL> nodeUrlByName(String nodeName);

    /** Returns set of node names. */
    Set<String> names();

    /** Returns set of node urls. */
    Set<URL> urls();

}
