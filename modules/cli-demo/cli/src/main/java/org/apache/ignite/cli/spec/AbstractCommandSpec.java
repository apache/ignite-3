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

package org.apache.ignite.cli.spec;

import org.apache.ignite.cli.CliHelper;
import org.apache.ignite.cli.CliVersionInfo;
import picocli.CommandLine;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Model.CommandSpec;

public abstract class AbstractCommandSpec implements Runnable, IVersionProvider {
    @CommandLine.Spec
    protected CommandSpec spec;

    @Override public final void run() {
        CliHelper.initCli(spec.commandLine());

        doRun();
    }

    protected abstract void doRun();

    @Override public String[] getVersion() {
        return new String[] { new CliVersionInfo().version };
    }
}
