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

import static org.apache.ignite.internal.util.IgniteUtils.closeAll;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.zip.ZipInputStream;
import org.apache.ignite.internal.deployunit.DeploymentUnit;
import org.apache.ignite.internal.deployunit.ZipDeploymentUnit;

/**
 * Advanced implementation of {@link InputStreamCollector} that automatically detects and handles ZIP content.
 *
 * <p>This decorator implementation wraps another {@link InputStreamCollector} and provides intelligent
 * handling of mixed content types. It automatically detects ZIP archives among the added input streams
 * and separates them from regular file content, creating a {@link ZipDeploymentUnit} that can handle
 * both types of content appropriately.
 */
public class ZipInputStreamCollector implements InputStreamCollector {
    private static final byte[] ZIP_MAGIC_HEADER = {0x50, 0x4b, 0x03, 0x04};

    /**
     * The delegate collector that handles regular (non-ZIP) content.
     * All streams that are not identified as ZIP archives are forwarded to this collector.
     */
    private final InputStreamCollector delegate;

    /**
     * Collection of detected ZIP input streams that require special processing.
     * These streams will be handled by the ZipDeploymentUnit for automatic extraction.
     */
    private final List<ZipInputStream> zipContent = new ArrayList<>();

    /**
     * Constructor.
     */
    public ZipInputStreamCollector(InputStreamCollector delegate) {
        this.delegate = delegate;
    }

    @Override
    public void addInputStream(String filename, InputStream is) {
        InputStream result = is.markSupported() ? is : new BufferedInputStream(is);

        if (isZip(result)) {
            zipContent.add(new ZipInputStream(result));
        } else {
            delegate.addInputStream(filename, result);
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
        List<AutoCloseable> toClose = new ArrayList<>(zipContent.size() + 1);
        toClose.add(delegate);
        toClose.addAll(zipContent);
        closeAll(toClose);
    }

    @Override
    public DeploymentUnit toDeploymentUnit() {
        return new ZipDeploymentUnit(delegate.toDeploymentUnit(), zipContent);
    }
}
