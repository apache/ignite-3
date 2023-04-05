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

package org.apache.ignite.internal.component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.NetworkAddress;
import org.jetbrains.annotations.Nullable;

/**
 * Can write network address to file that could be used by other systems to know on what port Ignite 3 REST server is started.
 */
public class RestAddressReporter {

    private static final IgniteLogger LOG = Loggers.forClass(RestAddressReporter.class);

    private static final String REPORT_FILE_NAME = "rest-address";

    private final Path workDir;

    /** Main constructor that accept the root directory where report file will be written. */
    public RestAddressReporter(Path workDir) {
        this.workDir = workDir;
    }

    /** Write network address to file. */
    public void writeReport(@Nullable NetworkAddress httpAddress, @Nullable NetworkAddress httpsAddress) {
        try {
            Files.writeString(workDir.resolve(REPORT_FILE_NAME), report(httpAddress, httpsAddress));
        } catch (IOException e) {
            String message = "Unexpected error when trying to write REST server network address to file";
            throw new IgniteException(Common.UNEXPECTED_ERR, message, e);
        }
    }

    private String report(@Nullable NetworkAddress httpAddress, @Nullable NetworkAddress httpsAddress) {
        return Stream.of(report("http", httpAddress), report("https", httpsAddress))
                .filter(Objects::nonNull)
                .collect(Collectors.joining(", "));
    }

    @Nullable
    private String report(String protocol, @Nullable NetworkAddress httpAddress) {
        if (httpAddress == null) {
            return null;
        } else {
            return protocol + "://" + httpAddress.host() + ":" + httpAddress.port();
        }
    }

    /** Remove report file. The method is expected to be called on node stop. */
    public void removeReport() {
        try {
            Files.delete(workDir.resolve(REPORT_FILE_NAME));
        } catch (IOException e) {
            String message = "Unexpected error when trying to remove REST server network address file";
            LOG.error(message, new IgniteException(Common.UNEXPECTED_ERR, message, e));
        }
    }
}
