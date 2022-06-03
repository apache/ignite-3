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

package org.apache.ignite.cli.commands.version;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.ignite.cli.VersionProvider;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Spec;

/**
 * Command that prints CLI version.
 */
@Command(name = "version", description = "Prints CLI version.")
@Singleton
public class VersionCommand implements Runnable {

    @Spec
    private CommandSpec commandSpec;

    @Inject
    private VersionProvider versionProvider;

    /** {@inheritDoc} */
    @Override
    public void run() {
        commandSpec.commandLine().getOut().println(versionProvider.getVersion()[0]);
    }
}
