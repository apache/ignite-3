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

package org.apache.ignite.compute;

import java.util.List;
import java.util.Objects;
import org.apache.ignite.compute.task.MapReduceTask;
import org.apache.ignite.deployment.DeploymentUnit;

/**
 * Compute task descriptor.
 */
public class TaskDescriptor<T, R> {
    private final String taskClassName;

    private final List<DeploymentUnit> units;

    private TaskDescriptor(
            String taskClassName,
            List<DeploymentUnit> units
    ) {
        this.taskClassName = taskClassName;
        this.units = units;
    }

    /**
     * Task class name.
     *
     * @return Task class name.
     */
    public String taskClassName() {
        return taskClassName;
    }

    /**
     * Deployment units.
     *
     * @return Deployment units.
     */
    public List<DeploymentUnit> units() {
        return units;
    }

    /**
     * Create a new builder.
     *
     * @return Task descriptor builder.
     */
    public static <T, R> Builder<T, R> builder(String taskClassName) {
        Objects.requireNonNull(taskClassName);

        return new Builder<>(taskClassName);
    }

    /**
     * Create a new builder.
     *
     * @return Task descriptor builder.
     */
    public static <I, M, T, R> Builder<I, R> builder(Class<? extends MapReduceTask<I, M, T, R>> taskClass) {
        Objects.requireNonNull(taskClass);

        return new Builder<>(taskClass.getName());
    }

    /**
     * Builder.
     */
    public static class Builder<T, R> {
        private final String taskClassName;
        private List<DeploymentUnit> units;

        private Builder(String taskClassName) {
            Objects.requireNonNull(taskClassName);

            this.taskClassName = taskClassName;
        }

        /**
         * Sets the deployment units.
         *
         * @param units Deployment units.
         * @return This builder.
         */
        public Builder<T, R> units(List<DeploymentUnit> units) {
            this.units = units;
            return this;
        }

        /**
         * Sets the deployment units.
         *
         * @param units Deployment units.
         * @return This builder.
         */
        public Builder<T, R> units(DeploymentUnit... units) {
            this.units = List.of(units);
            return this;
        }


        /**
         * Builds the task descriptor.
         *
         * @return Task descriptor.
         */
        public TaskDescriptor<T, R> build() {
            return new TaskDescriptor<>(
                    taskClassName,
                    units == null ? List.of() : units
            );
        }
    }
}
