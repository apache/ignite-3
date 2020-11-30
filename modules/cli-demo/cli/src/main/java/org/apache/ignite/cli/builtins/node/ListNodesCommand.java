/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.cli.builtins.node;

import java.util.Arrays;
import java.util.List;
import javax.inject.Inject;
import javax.inject.Singleton;
import com.github.freva.asciitable.AsciiTable;
import com.github.freva.asciitable.Column;
import com.github.freva.asciitable.HorizontalAlign;
import org.apache.ignite.cli.AbstractCliCommand;
import org.apache.ignite.cli.CliPathsConfigLoader;
import org.apache.ignite.cli.IgnitePaths;

@Singleton
public class ListNodesCommand extends AbstractCliCommand {

    private final NodeManager nodeManager;
    private final CliPathsConfigLoader cliPathsConfigLoader;

    @Inject
    public ListNodesCommand(NodeManager nodeManager,
        CliPathsConfigLoader cliPathsConfigLoader) {
        this.nodeManager = nodeManager;
        this.cliPathsConfigLoader = cliPathsConfigLoader;
    }

    public void run() {
        IgnitePaths paths = cliPathsConfigLoader.loadIgnitePathsOrThrowError();
        List<NodeManager.RunningNode> nodes = nodeManager
            .getRunningNodes(paths.workDir, paths.cliPidsDir());

        if (nodes.isEmpty())
            out.println("No running nodes");
        else {
            String table = AsciiTable.getTable(nodes, Arrays.asList(
                new Column().header("PID").dataAlign(HorizontalAlign.LEFT).with(n -> String.valueOf(n.pid)),
                new Column().header("Consistent Id").dataAlign(HorizontalAlign.LEFT).with(n -> n.consistentId),
                new Column().header("Log").maxColumnWidth(Integer.MAX_VALUE).dataAlign(HorizontalAlign.LEFT)
                    .with(n -> n.logFile.toString())
            ));
            out.println(table);
        }
    }
}
