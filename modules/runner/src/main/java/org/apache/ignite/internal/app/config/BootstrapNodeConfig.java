/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.app.config;

import org.apache.ignite.NodeConfig;

public class BootstrapNodeConfig implements NodeConfig {
    private final String nodeConfig;

    public BootstrapNodeConfig(String nodeConfig) {
        this.nodeConfig = nodeConfig;
    }

    public String nodeConfig() {
        return nodeConfig;
    }

    @Override
    public String initialContent() {
        return nodeConfig;
    }

    @Override
    public <T> T visit(NodeConfigVisitor<T> visitor) {
        return visitor.visitBootstrapConfig(this);
    }
}
