/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.cli.commands;

import static org.apache.ignite.internal.cli.commands.CommandConstants.HELP_OPTION_ORDER;
import static org.apache.ignite.internal.cli.commands.CommandConstants.VERBOSE_OPTION_ORDER;
import static org.apache.ignite.internal.cli.commands.Options.Constants.HELP_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.HELP_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.HELP_OPTION_SHORT;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERBOSE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERBOSE_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.VERBOSE_OPTION_SHORT;

import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * Base class for commands.
 */
public abstract class BaseCommand {
    /** Help option specification. */
    @Option(names = {HELP_OPTION, HELP_OPTION_SHORT}, description = HELP_OPTION_DESC, order = HELP_OPTION_ORDER)
    protected boolean usageHelpRequested;

    /** Verbose option specification. */
    @Option(names = {VERBOSE_OPTION, VERBOSE_OPTION_SHORT}, description = VERBOSE_OPTION_DESC, order = VERBOSE_OPTION_ORDER)
    protected boolean verbose;

    /** Instance of picocli command specification. */
    @Spec
    protected CommandSpec spec;
}
