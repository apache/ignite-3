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

package org.apache.ignite.cli.builtins;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import javax.inject.Singleton;
import io.micronaut.core.annotation.Introspected;
import net.harawata.appdirs.AppDirs;
import net.harawata.appdirs.AppDirsFactory;
import org.apache.ignite.cli.IgniteCLIException;

public interface SystemPathResolver {

    /**
     * @return
     */
    Path osgGlobalConfigPath();

    Path osHomeDirectoryPath();

    Path osCurrentDirPath();

    static URL[] list(Path path) {
        try {
            return Files.list(path)
                .map(p -> {
                    try {
                        return p.toUri().toURL();
                    }
                    catch (MalformedURLException e) {
                        throw new RuntimeException(e);
                    }
                }).toArray(URL[]::new);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     */
    @Singleton
    @Introspected
    class DefaultPathResolver implements SystemPathResolver {

        private static final String APP_NAME = "ignite";

        private final AppDirs appsDir = AppDirsFactory.getInstance();

        @Override public Path osgGlobalConfigPath() {

            String osName = System.getProperty("os.name").toLowerCase();

            // TODO: check if appdirs is suitable for all cases (xdg integration and mac os path should be checked)
            if (osName.contains("unix"))
                return PathHelpers.pathOf("/etc/").resolve(APP_NAME);
            else if (osName.startsWith("windows"))
                return PathHelpers.pathOf(appsDir.getSiteConfigDir(APP_NAME, null, null));
            else if (osName.startsWith("mac os"))
                return PathHelpers.pathOf("/Library/App \\Support/").resolve(APP_NAME);
            else throw new IgniteCLIException("Unknown OS. Can't detect the appropriate config path");
        }

        @Override public Path osHomeDirectoryPath() {
            return PathHelpers.pathOf(System.getProperty("user.home"));
        }

        @Override public Path osCurrentDirPath() {
            return PathHelpers.pathOf(System.getProperty("user.dir"));
        }

    }
}
