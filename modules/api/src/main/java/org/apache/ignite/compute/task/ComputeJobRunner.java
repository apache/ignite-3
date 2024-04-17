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

package org.apache.ignite.compute.task;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.network.ClusterNode;

public class ComputeJobRunner {
    private final String jobClassName;

    private final Set<ClusterNode> nodes;

    private final JobExecutionOptions jobOptions;

    private final List<DeploymentUnit> units;

    private final Object[] args;

    private ComputeJobRunner(
            String jobClassName,
            Set<ClusterNode> nodes,
            JobExecutionOptions jobOptions,
            List<DeploymentUnit> units,
            Object[] args
    ) {
        this.jobClassName = jobClassName;
        this.nodes = Collections.unmodifiableSet(nodes);
        this.jobOptions = jobOptions;
        this.units = units;
        this.args = args;
    }

    public String jobClassName() {
        return jobClassName;
    }

    public Set<ClusterNode> nodes() {
        return nodes;
    }

    public JobExecutionOptions options() {
        return jobOptions;
    }

    public List<DeploymentUnit> units() {
        return units;
    }

    public Object[] args() {
        return args;
    }

    public JobExecutionParametersBuilder toBuilder() {
        return builder().jobClassName(jobClassName).nodes(nodes).options(jobOptions).units(units);
    }

    public static JobExecutionParametersBuilder builder() {
        return new JobExecutionParametersBuilder();
    }

    public static class JobExecutionParametersBuilder {
        private String jobClassName;

        private final Set<ClusterNode> nodes = new HashSet<>();

        private JobExecutionOptions jobOptions = JobExecutionOptions.DEFAULT;

        private List<DeploymentUnit> units = Collections.emptyList();

        private Object[] args;

        public JobExecutionParametersBuilder jobClassName(String jobClassName) {
            this.jobClassName = jobClassName;
            return this;
        }

        public JobExecutionParametersBuilder nodes(Set<ClusterNode> nodes) {
            this.nodes.addAll(nodes);
            return this;
        }

        public JobExecutionParametersBuilder options(JobExecutionOptions options) {
            this.jobOptions = options;
            return this;
        }

        public JobExecutionParametersBuilder units(List<DeploymentUnit> units) {
            this.units = units;
            return this;
        }

        public JobExecutionParametersBuilder args(Object... args) {
            this.args = args;
            return this;
        }

        public ComputeJobRunner build() {
            Objects.requireNonNull(nodes);
            if (nodes.isEmpty()) {
                throw new IllegalArgumentException();
            }

            return new ComputeJobRunner(
                    jobClassName,
                    nodes,
                    jobOptions,
                    units,
                    args
            );
        }

        public JobExecutionParametersBuilder node(ClusterNode value) {
            nodes.add(value);
            return this;
        }
    }
}
