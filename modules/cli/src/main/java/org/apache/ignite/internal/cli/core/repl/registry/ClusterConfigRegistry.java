package org.apache.ignite.internal.cli.core.repl.registry;

import com.typesafe.config.Config;

public interface ClusterConfigRegistry {

    /** Returns cluster config. */
    Config config();
}
