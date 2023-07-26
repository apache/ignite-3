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

package org.apache.ignite.internal.deployunit;

import java.io.InputStream;
import java.util.Map;
import org.apache.ignite.internal.util.IgniteUtils;

/**
 * Deployment unit interface.
 */
public class DeploymentUnit implements AutoCloseable {
    private final Map<String, InputStream> content;

    public DeploymentUnit(Map<String, InputStream> content) {
        this.content = content;
    }

    /**
     * Deployment unit content - a map from file name to input stream.
     *
     * @return Deployment unit content.
     */
    public Map<String, InputStream> content() {
        return content;
    }

    @Override
    public void close() throws Exception {
        IgniteUtils.closeAll(content.values());
    }
}
