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
    public final Path configDir;
    public final Path logDir;
    private final String version;

    public IgnitePaths(Path binDir, Path workDir, Path configDir, Path logDir, String version) {
        this.binDir = binDir;
        this.workDir = workDir;
        this.version = version;
        this.configDir = configDir;
        this.logDir = logDir;
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
        return configDir;
    }

    public Path logDir() {
        return logDir;
    }

    public Path serverDefaultConfigFile() {
        return serverConfigDir().resolve("default-config.xml");
    }

    public void initOrRecover() {
        initDirIfNeeded(workDir,"Can't create working directory: " + workDir);
        initDirIfNeeded(libsDir(),"Can't create a directory for ignite modules: " + libsDir());
        initDirIfNeeded(cliLibsDir(),"Can't create a directory for cli modules: " + cliLibsDir());
        initDirIfNeeded(serverConfigDir(),"Can't create a directory for server configs: " + serverConfigDir());
        initDirIfNeeded(logDir(),"Can't create a directory for server logs: " + logDir());
    }

    private void initDirIfNeeded(Path dir, String exceptionMessage) {
        File dirFile = dir.toFile();
        if (!(dirFile.exists() || dirFile.mkdirs()))
            throw new IgniteCLIException(exceptionMessage);
    }

    public boolean validateDirs() {
        return workDir.toFile().exists() &&
                libsDir().toFile().exists() &&
                cliLibsDir().toFile().exists() &&
                serverConfigDir().toFile().exists() &&
                logDir().toFile().exists();
    }
}
