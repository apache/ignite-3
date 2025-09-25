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

package org.apache.ignite.internal.rest.deployment;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.zip.ZipInputStream;
import org.apache.ignite.internal.deployunit.DeploymentUnit;
import org.apache.ignite.internal.deployunit.ZipDeploymentUnit;
import org.apache.ignite.internal.deployunit.exception.DeploymentUnitZipException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.lang.IgniteException;

/**
 * Advanced implementation of {@link InputStreamCollector} that automatically detects and handles ZIP content.
 *
 * <p>This decorator implementation automatically detects ZIP archive and throws exception in case when provided more than one archive.
 */
public class ZipInputStreamCollector implements InputStreamCollector {
    private static final IgniteLogger LOG = Loggers.forClass(ZipInputStreamCollector.class);
    private static final byte[] ZIP_MAGIC_HEADER = {0x50, 0x4b, 0x03, 0x04};

    private ZipInputStream zis;

    private IgniteException igniteException;

    @Override
    public void addInputStream(String filename, InputStream is) {
        if (zis != null || igniteException != null) {
            // We don't need the stream anymore, so we close it to avoid resource leak.
            safeClose(is);
            if (igniteException == null) {
                igniteException = new DeploymentUnitZipException("Deployment unit with unzip supports only single zip file.");
            }
            return;
        }

        InputStream result = is.markSupported() ? is : new BufferedInputStream(is);

        if (isZip(result)) {
            zis = new ZipInputStream(result);
        } else {
            safeClose(result);
            igniteException = new DeploymentUnitZipException("Only zip file is supported.");
        }
    }

    private static void safeClose(InputStream is) {
        try {
            is.close();
        } catch (IOException e) {
            LOG.warn("Failed to close non-zip input stream.", e);
        }
    }

    private static boolean isZip(InputStream is) {
        try {
            boolean isZip = Objects.deepEquals(ZIP_MAGIC_HEADER, is.readNBytes(4));
            is.reset();
            return isZip;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void close() throws Exception {
        if (zis != null) {
            zis.close();
        }
    }

    @Override
    public DeploymentUnit toDeploymentUnit() {
        if (igniteException != null) {
            throw igniteException;
        }
        return new ZipDeploymentUnit(zis);
    }
}
