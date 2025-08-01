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

package org.apache.ignite.migrationtools.cli.configs.calls;

import java.nio.file.Path;
import org.apache.ignite.migrationtools.config.ConfigurationConverter;
import org.apache.ignite3.internal.cli.core.call.Call;
import org.apache.ignite3.internal.cli.core.call.CallInput;
import org.apache.ignite3.internal.cli.core.call.CallOutput;
import org.apache.ignite3.internal.cli.core.call.DefaultCallOutput;

/** Call to the Configuration Converter. */
public class ConfigurationConverterCall implements Call<ConfigurationConverterCall.Input, String> {

    @Override
    public CallOutput<String> execute(Input params) {
        try {
            ConfigurationConverter.convertConfigurationFile(
                    params.inputConfigFilePath().toFile(),
                    params.outputNodeConfigFilePath().toFile(),
                    params.outputClusterConfigFilePath().toFile(),
                    params.includeDefaults(),
                    params.clientClassLoader()
            );

            return DefaultCallOutput.success("");
        } catch (Exception e) {
            return DefaultCallOutput.failure(e);
        }
    }

    /** Inputs. */
    public static class Input implements CallInput {
        private Path inputConfigFilePath;

        private Path outputNodeConfigFilePath;

        private Path outputClusterConfigFilePath;

        private boolean includeDefaults;

        private ClassLoader clientClassLoader;

        /**
         * Contructor.
         *
         * @param inputConfigFilePath InputConfigurationFile
         * @param outputNodeConfigFilePath Output Node Configuration File Path
         * @param outputClusterConfigFilePath Output Cluster Configuration File Path
         * @param includeDefaults Whether defaults should be included in the output.
         * @param clientClassLoader The classloader to use.
         */
        public Input(
                Path inputConfigFilePath,
                Path outputNodeConfigFilePath,
                Path outputClusterConfigFilePath,
                boolean includeDefaults,
                ClassLoader clientClassLoader
        ) {
            this.inputConfigFilePath = inputConfigFilePath;
            this.outputNodeConfigFilePath = outputNodeConfigFilePath;
            this.outputClusterConfigFilePath = outputClusterConfigFilePath;
            this.includeDefaults = includeDefaults;
            this.clientClassLoader = clientClassLoader;
        }

        public Path inputConfigFilePath() {
            return inputConfigFilePath;
        }

        public Path outputNodeConfigFilePath() {
            return outputNodeConfigFilePath;
        }

        public Path outputClusterConfigFilePath() {
            return outputClusterConfigFilePath;
        }

        public boolean includeDefaults() {
            return includeDefaults;
        }

        public ClassLoader clientClassLoader() {
            return clientClassLoader;
        }
    }
}
