/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.app.config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.NodeConfig;
import org.apache.ignite.internal.configuration.NodeConfigParseException;

public class FileNodeConfig implements NodeConfig {
    private final Path configPath;

    public FileNodeConfig(Path configPath) {
        this.configPath = configPath;
    }

    public Path configPath() {
        return configPath;
    }

    @Override
    public String initialContent() {
        try {
            return Files.readString(configPath);
        } catch (IOException e) {
            throw new NodeConfigParseException("Failed to read configuration file: " + configPath, e);
        }
    }

    @Override
    public <T> T visit(NodeConfigVisitor<T> visitor) {
        return visitor.visitFileConfig(this);
    }
}
