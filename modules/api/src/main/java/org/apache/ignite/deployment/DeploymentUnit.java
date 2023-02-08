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

package org.apache.ignite.deployment;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;

/**
 * Deployment unit interface.
 */
public interface DeploymentUnit {

    /**
     * Unit content.
     *
     * @return Name of deployment unit.
     */
    String unitName();

    /**
     * Input stream with deployment unit content.
     *
     * @return input stream with deployment unit content.
     */
    InputStream content();


    /**
     * Create deployment unit from local path.
     *
     * @param path Path to local file.
     * @return Deployment unit based on local file.
     */
    static DeploymentUnit fromPath(Path path) {
        return new DeploymentUnit() {

            @Override
            public String unitName() {
                return path.getFileName().toString();
            }

            @Override
            public InputStream content() {
                try {
                    return new FileInputStream(path.toFile());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
