/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.app.config;

import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.NodeConfig;
import org.apache.ignite.internal.configuration.NodeConfigWriteException;

public class NodeConfigFactory {
    public static NodeConfig fromFile(Path configPath) {
        if (!Files.isWritable(configPath)) {
            throw new NodeConfigWriteException(
                    "The configuration file is read-only, so changes cannot be applied. "
                            + "Check your system configuration. "
                            + "If you are using containerization, such as Kubernetes, "
                            + "the file can only be modified through native Kubernetes methods."
            );
        }
        return new FileNodeConfig(configPath);
    }

    public static NodeConfig fromBootstrap(String nodeConfig) {

        return new BootstrapNodeConfig(nodeConfig);
    }
}
