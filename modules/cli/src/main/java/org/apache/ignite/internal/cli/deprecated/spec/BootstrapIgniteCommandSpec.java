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

package org.apache.ignite.internal.cli.deprecated.spec;

import jakarta.inject.Inject;
import java.net.URL;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.cli.commands.BaseCommand;
import org.apache.ignite.internal.cli.deprecated.builtins.init.InitIgniteCommand;
import picocli.CommandLine;

/**
 * Command for install Ignite distributive to start new nodes on the current machine.
 */
@CommandLine.Command(name = "bootstrap", description = "Installs Ignite core modules locally.")
public class BootstrapIgniteCommandSpec extends BaseCommand implements Callable<Integer> {
    /** Init command implementation. */
    @Inject
    private InitIgniteCommand cmd;

    /** Option for custom maven repository to download Ignite core. */
    @CommandLine.Option(
            names = "--repo",
            description = "Additional Maven repository URL"
    )
    private URL[] urls;

    /** {@inheritDoc} */
    @Override
    public Integer call() {
        cmd.init(urls, spec.commandLine().getOut(), spec.commandLine().getColorScheme());

        return 0;
    }
}
