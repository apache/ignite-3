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

package org.apache.ignite.cli.commands;

import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Base class for commands.
 */
public abstract class BaseCommand {
    /** Help option specification. */
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit.")
    protected boolean usageHelpRequested;

    @Spec
    protected CommandSpec spec;

    /**
     * Constructs current full command name from the {@code CommandSpec} including all parent commands.
     *
     * @return full command name.
     */
    protected String getCommandName() {
        StringBuilder sb = new StringBuilder();
        CommandSpec root = spec;
        do {
            sb.insert(0, root.name());
            sb.insert(0, " ");
            root = root.parent();
        } while (root != null);
        return sb.toString().trim();
    }
}
