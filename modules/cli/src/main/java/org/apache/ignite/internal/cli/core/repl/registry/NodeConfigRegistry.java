package org.apache.ignite.internal.cli.core.repl.registry;

import com.typesafe.config.Config;

public interface NodeConfigRegistry {

    /** Returns node config. */
    Config config();
}
