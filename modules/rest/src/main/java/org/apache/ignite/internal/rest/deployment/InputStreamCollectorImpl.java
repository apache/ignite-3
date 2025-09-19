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

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.deployunit.DeploymentUnit;
import org.apache.ignite.internal.deployunit.DeploymentUnitImpl;

/**
 * Standard implementation of {@link InputStreamCollector} for collecting regular file content.
 *
 * <p>This implementation provides a straightforward approach to collecting input streams and
 * converting them into a standard {@link DeploymentUnitImpl}. It maintains an internal map
 * of filename-to-stream associations and creates deployment units containing regular
 * (non-compressed) file content.
 */
public class InputStreamCollectorImpl implements InputStreamCollector {
    /**
     * Internal storage for collected input streams mapped by their filenames.
     * The map maintains the association between logical file paths and their content streams.
     */
    private final Map<String, InputStream> content = new HashMap<>();

    /** {@inheritDoc} */
    @Override
    public void addInputStream(String filename, InputStream is) {
        content.put(filename, is);
    }

    /** {@inheritDoc} */
    @Override
    public DeploymentUnit toDeploymentUnit() {
        return new DeploymentUnitImpl(content);
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws Exception {
        closeAll(content.values());
    }
}
