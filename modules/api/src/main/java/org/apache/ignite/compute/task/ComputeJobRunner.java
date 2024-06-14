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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.ignite.compute.DeploymentUnit;
import org.apache.ignite.compute.JobExecutionOptions;
import org.apache.ignite.network.ClusterNode;

/**
 * A description of the job to be submitted as a result of the split step of the {@link MapReduceTask}. Reflects the parameters of the
 * {@link org.apache.ignite.compute.IgniteCompute#submit(Set, List, String, JobExecutionOptions, Object...) IgniteCompute#submit} method.
 */
public class ComputeJobRunner {
    private final Set<ClusterNode> nodes;

    private final List<DeploymentUnit> units;

    private final String jobClassName;

    private final JobExecutionOptions options;

    private final Object args;

    private ComputeJobRunner(
            Set<ClusterNode> nodes,
            List<DeploymentUnit> units,
            String jobClassName,
            JobExecutionOptions options,
            Object args
    ) {
        this.nodes = Collections.unmodifiableSet(nodes);
        this.units = units;
        this.jobClassName = jobClassName;
        this.options = options;
        this.args = args;
    }

    /**
     * Candidate nodes; the job will be executed on one of them.
     *
     * @return A set of candidate nodes.
     */
    public Set<ClusterNode> nodes() {
        return nodes;
    }

    /**
     * Deployment units. Can be empty.
     *
     * @return Deployment units.
     */
    public List<DeploymentUnit> units() {
        return units;
    }

    /**
     * Name of the job class to execute.
     *
     * @return Name of the job class to execute.
     */
    public String jobClassName() {
        return jobClassName;
    }

    /**
     * Job execution options (priority, max retries).
     *
     * @return Job execution options.
     */
    public JobExecutionOptions options() {
        return options;
    }

    /**
     * Arguments of the job.
     *
     * @return Arguments of the job.
     */
    public Object args() {
        return args;
    }

    /**
     * Returns new builder using this definition.
     *
     * @return New builder.
     */
    public ComputeJobRunnerBuilder toBuilder() {
        return builder().nodes(nodes).units(units).jobClassName(jobClassName).options(options).args(args);
    }

    /**
     * Returns new builder.
     *
     * @return New builder.
     */
    public static ComputeJobRunnerBuilder builder() {
        return new ComputeJobRunnerBuilder();
    }

    /**
     * Job submit parameters builder.
     */
    public static class ComputeJobRunnerBuilder {
        private final Set<ClusterNode> nodes = new HashSet<>();

        private final List<DeploymentUnit> units = new ArrayList<>();

        private String jobClassName;

        private JobExecutionOptions options = JobExecutionOptions.DEFAULT;

        private Object args;

        /**
         * Adds nodes to the set of candidate nodes.
         *
         * @param nodes A collection of candidate nodes.
         * @return Builder instance.
         */
        public ComputeJobRunnerBuilder nodes(Collection<ClusterNode> nodes) {
            this.nodes.addAll(nodes);
            return this;
        }

        /**
         * Adds a node to the set of candidate nodes.
         *
         * @param node Candidate node.
         * @return Builder instance.
         */
        public ComputeJobRunnerBuilder node(ClusterNode node) {
            nodes.add(node);
            return this;
        }

        /**
         * Adds deployment units.
         *
         * @param units A collection of deployment units.
         * @return Builder instance.
         */
        public ComputeJobRunnerBuilder units(Collection<DeploymentUnit> units) {
            this.units.addAll(units);
            return this;
        }

        /**
         * Sets the name of the job class to execute.
         *
         * @param jobClassName A job class name.
         * @return Builder instance.
         */
        public ComputeJobRunnerBuilder jobClassName(String jobClassName) {
            this.jobClassName = jobClassName;
            return this;
        }

        /**
         * Sets job execution options (priority, max retries).
         *
         * @param options Job execution options.
         * @return Builder instance.
         */
        public ComputeJobRunnerBuilder options(JobExecutionOptions options) {
            this.options = options;
            return this;
        }

        /**
         * Sets arguments of the job.
         *
         * @param args Arguments of the job.
         * @return Builder instance.
         */
        public ComputeJobRunnerBuilder args(Object args) {
            this.args = args;
            return this;
        }

        /**
         * Constructs a compute job description object.
         *
         * @return Description object.
         */
        public ComputeJobRunner build() {
            if (nodes.isEmpty()) {
                throw new IllegalArgumentException();
            }

            return new ComputeJobRunner(nodes, units, jobClassName, options, args);
        }
    }
}
