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

package org.apache.ignite.internal.cli.commands.cluster.init;

import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_CONFIG_FILE_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_CONFIG_FILE_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_CONFIG_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_CONFIG_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CLUSTER_NAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CMG_NODE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.CMG_NODE_NAME_OPTION_DESC;
import static org.apache.ignite.internal.cli.commands.Options.Constants.META_STORAGE_NODE_NAME_OPTION;
import static org.apache.ignite.internal.cli.commands.Options.Constants.META_STORAGE_NODE_NAME_OPTION_DESC;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import org.apache.ignite.internal.cli.core.exception.IgniteCliException;
import org.jetbrains.annotations.Nullable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.IParameterPreprocessor;
import picocli.CommandLine.Model.ArgSpec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;

/**
 * Mixin class for cluster init command options.
 */
public class ClusterInitOptions {
    /**
     * List of names of the nodes (each represented by a separate command line argument) that will host the Meta Storage. If the
     * "--cmg-nodes" parameter is omitted, the same nodes will also host the Cluster Management Group.
     */
    @Option(names = META_STORAGE_NODE_NAME_OPTION, required = true,
            description = META_STORAGE_NODE_NAME_OPTION_DESC, split = ",",
            preprocessor = SingleOccurrenceMetastorageConsumer.class
    )
    private List<String> metaStorageNodes;

    /**
     * List of names of the nodes (each represented by a separate command line argument) that will host the Cluster Management Group.
     */
    @Option(names = CMG_NODE_NAME_OPTION,
            description = CMG_NODE_NAME_OPTION_DESC, split = ",",
            preprocessor = SingleOccurrenceClusterManagementConsumer.class
    )
    private List<String> cmgNodes = new ArrayList<>();

    /** Name of the cluster. */
    @Option(names = CLUSTER_NAME_OPTION, required = true, description = CLUSTER_NAME_OPTION_DESC)
    private String clusterName;

    @ArgGroup
    private ClusterConfigOptions clusterConfigOptions;

    private static class ClusterConfigOptions {
        @Option(names = CLUSTER_CONFIG_OPTION, description = CLUSTER_CONFIG_OPTION_DESC)
        private String config;

        @Option(names = CLUSTER_CONFIG_FILE_OPTION, description = CLUSTER_CONFIG_FILE_OPTION_DESC)
        private File file;
    }

    /**
     * Consistent IDs of the nodes that will host the Meta Storage.
     *
     * @return Meta storage node ids.
     */
    public List<String> metaStorageNodes() {
        return metaStorageNodes;
    }

    /**
     * Consistent IDs of the nodes that will host the Cluster Management Group; if empty, {@code metaStorageNodeIds} will be used to host
     * the CMG as well.
     *
     * @return Cluster management node ids.
     */
    public List<String> cmgNodes() {
        return cmgNodes;
    }

    /**
     * Human-readable name of the cluster.
     *
     * @return Cluster name.
     */
    public String clusterName() {
        return clusterName;
    }

    /**
     * Cluster configuration.
     *
     * @return Cluster configuration.
     */
    @Nullable
    public String clusterConfiguration() {
        if (clusterConfigOptions == null) {
            return null;
        } else if (clusterConfigOptions.config != null) {
            return clusterConfigOptions.config;
        } else if (clusterConfigOptions.file != null) {
            try {
                return Files.readString(clusterConfigOptions.file.toPath());
            } catch (IOException e) {
                throw new IgniteCliException("Couldn't read cluster configuration file: " + clusterConfigOptions.file, e);
            }
        } else {
            return null;
        }
    }


    private static class DuplicatesChecker {
        private final String optionToCheck;

        private DuplicatesChecker(String optionToCheck) {
            this.optionToCheck = optionToCheck;
        }

        private boolean hasDuplicate(Stack<String> args) {
            var arr = args.toArray(String[]::new);
            for (var str : arr) {
                if (optionToCheck.equals(str.trim())) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class SingleOccurrenceClusterManagementConsumer implements IParameterPreprocessor {
        private final DuplicatesChecker checker = new DuplicatesChecker(CMG_NODE_NAME_OPTION);

        @Override
        public boolean preprocess(Stack<String> args, CommandSpec commandSpec, ArgSpec argSpec, Map<String, Object> info) {
            return checker.hasDuplicate(args);
        }
    }

    private static class SingleOccurrenceMetastorageConsumer implements IParameterPreprocessor {
        private final DuplicatesChecker checker = new DuplicatesChecker(META_STORAGE_NODE_NAME_OPTION);

        @Override
        public boolean preprocess(Stack<String> args, CommandSpec commandSpec, ArgSpec argSpec, Map<String, Object> info) {
            return checker.hasDuplicate(args);
        }
    }
}
