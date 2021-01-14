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

package org.apache.ignite.cli;

import java.io.File;
import java.nio.file.Path;

public class IgnitePaths {

    public final Path binDir;
    public final Path workDir;
    private final String version;

    public IgnitePaths(Path binDir, Path workDir, String version) {
        this.binDir = binDir;
        this.workDir = workDir;
        this.version = version;
    }


    public Path cliLibsDir() {
        return binDir.resolve(version).resolve("cli");
    }

    public Path libsDir() {
        return binDir.resolve(version).resolve("libs");
    }

    public Path cliPidsDir() {
        return workDir.resolve("cli").resolve("pids");
    }

    public Path installedModulesFile() {
        return workDir.resolve("modules.json");
    }
    
    public Path serverConfigDir() {
        return workDir.resolve("config");
    }

    public Path serverDefaultConfigFile() {
        return serverConfigDir().resolve("default-config.xml");
    }

    public void initOrRecover() {
        File igniteWork = workDir.toFile();
        if (!(igniteWork.exists() || igniteWork.mkdirs()))
            throw new IgniteCLIException("Can't create working directory: " + workDir);

        File igniteBin = libsDir().toFile();
        if (!(igniteBin.exists() || igniteBin.mkdirs()))
            throw new IgniteCLIException("Can't create a directory for ignite modules: " + libsDir());

        File igniteBinCli = cliLibsDir().toFile();
        if (!(igniteBinCli.exists() || igniteBinCli.mkdirs()))
            throw new IgniteCLIException("Can't create a directory for cli modules: " + cliLibsDir());

        File serverConfig = serverConfigDir().toFile();
        if (!(serverConfig.exists() || serverConfig.mkdirs()))
            throw new IgniteCLIException("Can't create a directory for server configs: " + serverConfigDir());
    }

    public boolean validateDirs() {
        return workDir.toFile().exists() &&
                libsDir().toFile().exists() &&
                cliLibsDir().toFile().exists() &&
                serverConfigDir().toFile().exists();
    }
}
