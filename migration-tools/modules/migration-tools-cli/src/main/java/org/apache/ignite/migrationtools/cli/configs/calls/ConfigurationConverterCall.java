/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
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
