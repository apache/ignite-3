/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.apache.ignite.raft.jraft.util.Describer;
import org.apache.ignite.raft.jraft.util.FileOutputSignalHandler;
import org.apache.ignite.raft.jraft.util.SystemPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jiachun.fjc
 */
public class NodeDescribeSignalHandler extends FileOutputSignalHandler {

    private static Logger       LOG       = LoggerFactory.getLogger(NodeDescribeSignalHandler.class);

    private static final String DIR       = SystemPropertyUtil.get("jraft.signal.node.describe.dir", "");
    private static final String BASE_NAME = "node_describe.log";

    @Override
    public void handle(final String signalName) {
        final List<Node> nodes = NodeManager.getInstance().getAllNodes();
        if (nodes.isEmpty()) {
            return;
        }

        try {
            final File file = getOutputFile(DIR, BASE_NAME);

            LOG.info("Describing raft nodes with signal: {} to file: {}.", signalName, file);

            try (final PrintWriter out = new PrintWriter(new OutputStreamWriter(new FileOutputStream(file, true),
                StandardCharsets.UTF_8))) {
                final Describer.Printer printer = new Describer.DefaultPrinter(out);
                for (final Node node : nodes) {
                    node.describe(printer);
                }
            }
        } catch (final IOException e) {
            LOG.error("Fail to describe nodes: {}.", nodes, e);
        }
    }
}
