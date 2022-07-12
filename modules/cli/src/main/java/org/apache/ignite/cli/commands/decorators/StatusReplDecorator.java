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

package org.apache.ignite.cli.commands.decorators;

import org.apache.ignite.cli.call.status.Status;
import org.apache.ignite.cli.core.decorator.Decorator;
import org.apache.ignite.cli.core.decorator.TerminalOutput;
import picocli.CommandLine.Help.Ansi;

/**
 * Decorator for {@link Status}.
 */
public class StatusReplDecorator implements Decorator<Status, TerminalOutput> {

    @Override
    public TerminalOutput decorate(Status data) {
        if (!data.isConnected()) {
            return () -> "You are not connected to any Ignite 3 node. Try to run 'connect' command.";
        }

        return () -> Ansi.AUTO.string("Connected to " + data.getConnectedNodeUrl() + System.lineSeparator()
                + "[nodes: " + data.getNodeCount() + ", status: "
                + (data.isInitialized() ? "@|fg(10) active|@" : "@|fg(9) not initialized|@") + "]");
    }
}
