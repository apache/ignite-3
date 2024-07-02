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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.compute.JobDescriptor;
import org.apache.ignite.compute.JobTarget;
import org.apache.ignite.network.ClusterNode;

/**
 * A description of the job to be submitted as a result of the split step of the {@link MapReduceTask}. Reflects the parameters of the
 * {@link org.apache.ignite.compute.IgniteCompute#submit(JobTarget, JobDescriptor, Object)} method.
 */
public class MapReduceJob<T, R> {
    private final Set<ClusterNode> nodes;

    private final JobDescriptor<T, R> jobDescriptor;

    private final T args;

    private MapReduceJob(
            Set<ClusterNode> nodes,
            JobDescriptor<T, R> jobDescriptor,
            T args
    ) {
        this.nodes = Collections.unmodifiableSet(nodes);
        this.jobDescriptor = jobDescriptor;
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
     * Job descriptor.
     *
     * @return Job descriptor.
     */
    public JobDescriptor<T, R> jobDescriptor() {
        return jobDescriptor;
    }

    /**
     * Arguments of the job.
     *
     * @return Arguments of the job.
     */
    public T arg() {
        return args;
    }

    /**
     * Returns new builder using this definition.
     *
     * @return New builder.
     */
    public ComputeJobRunnerBuilder<T, R> toBuilder() {
        return MapReduceJob.<T, R>builder().jobDescriptor(jobDescriptor).nodes(nodes).args(args);
    }

    /**
     * Returns new builder.
     *
     * @return New builder.
     */
    public static <T, R> ComputeJobRunnerBuilder<T, R> builder() {
        return new ComputeJobRunnerBuilder<>();
    }

    /**
     * Job submit parameters builder.
     */
    public static class ComputeJobRunnerBuilder<T, R> {
        private final Set<ClusterNode> nodes = new HashSet<>();

        private JobDescriptor<T, R> jobDescriptor;

        private T args;

        /**
         * Adds nodes to the set of candidate nodes.
         *
         * @param nodes A collection of candidate nodes.
         * @return Builder instance.
         */
        public ComputeJobRunnerBuilder<T, R> nodes(Collection<ClusterNode> nodes) {
            this.nodes.addAll(nodes);
            return this;
        }

        /**
         * Adds a node to the set of candidate nodes.
         *
         * @param node Candidate node.
         * @return Builder instance.
         */
        public ComputeJobRunnerBuilder<T, R> node(ClusterNode node) {
            nodes.add(node);
            return this;
        }

        /**
         * Sets job descriptor.
         *
         * @param jobDescriptor A job descriptor.
         * @return Builder instance.
         */
        public ComputeJobRunnerBuilder<T, R> jobDescriptor(JobDescriptor<T, R> jobDescriptor) {
            this.jobDescriptor = jobDescriptor;
            return this;
        }

        /**
         * Sets arguments of the job.
         *
         * @param args Arguments of the job.
         * @return Builder instance.
         */
        public ComputeJobRunnerBuilder<T, R> args(T args) {
            this.args = args;
            return this;
        }

        /**
         * Constructs a compute job description object.
         *
         * @return Description object.
         */
        public MapReduceJob<T, R> build() {
            if (nodes.isEmpty()) {
                throw new IllegalArgumentException();
            }

            return new MapReduceJob<>(nodes, jobDescriptor, args);
        }
    }
}
