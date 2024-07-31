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
import org.apache.ignite.deployment.DeploymentUnit;
import org.apache.ignite.marshalling.Marshaller;
import org.jetbrains.annotations.Nullable;

/**
 * Compute job descriptor.
 */
public class JobDescriptor<T, R> {
    private final String jobClassName;

    private final List<DeploymentUnit> units;

    private final JobExecutionOptions options;

    private final @Nullable Marshaller<R, byte[]> resultMarshaller;

    private final @Nullable Marshaller<T, byte[]> argumentMarshaller;

    private JobDescriptor(
            String jobClassName,
            List<DeploymentUnit> units,
            JobExecutionOptions options,
            @Nullable Marshaller<T, byte[]> argumentMarshaller,
            @Nullable Marshaller<R, byte[]> resultMarshaller
    ) {
        this.jobClassName = jobClassName;
        this.units = units;
        this.options = options;
        this.argumentMarshaller = argumentMarshaller;
        this.resultMarshaller = resultMarshaller;
    }

    /**
     * Job class name.
     *
     * @return Job class name.
     */
    public String jobClassName() {
        return jobClassName;
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
     * Job execution options.
     *
     * @return Job execution options.
     */
    public JobExecutionOptions options() {
        return options;
    }

    /**
     * Create a new builder.
     *
     * @return Job descriptor builder.
     */
    public static <T, R> Builder<T, R> builder(String jobClassName) {
        Objects.requireNonNull(jobClassName);

        return new Builder<>(jobClassName);
    }

    /**
     * Create a new builder.
     *
     * @return Job descriptor builder.
     */
    public static <T, R> Builder<T, R> builder(Class<? extends ComputeJob<T, R>> jobClass) {
        Objects.requireNonNull(jobClass);

        return new Builder<>(jobClass.getName());
    }

    public @Nullable Marshaller<R, byte[]> resultMarshaller() {
        return resultMarshaller;
    }

    public @Nullable Marshaller<T, byte[]> argumentMarshaller() {
        return argumentMarshaller;
    }

    /**
     * Builder.
     */
    public static class Builder<T, R> {
        private final String jobClassName;
        private List<DeploymentUnit> units;
        private JobExecutionOptions options;
        private Marshaller<T, byte[]> argumentMarshaller;
        private Marshaller<R, byte[]> resultMarshaller;

        private Builder(String jobClassName) {
            Objects.requireNonNull(jobClassName);

            this.jobClassName = jobClassName;
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
         * Sets the job execution options.
         *
         * @param options Job execution options.
         * @return This builder.
         */
        public Builder<T, R> options(JobExecutionOptions options) {
            this.options = options;
            return this;
        }

        /**
         * Sets the result marshaller.
         *
         * @param marshaller Result marshaller.
         *
         * @return This builder.
         */
        public Builder<T, R> resultMarshaller(Marshaller<R, byte[]> marshaller) {
            this.resultMarshaller = marshaller;
            return this;
        }

        /**
         * Sets the argument marshaller.
         *
         * @param marshaller Argument marshaller.
         *
         * @return This builder.
         */
        public Builder<T, R> argumentMarshaller(Marshaller<T, byte[]> marshaller) {
            this.argumentMarshaller = marshaller;
            return this;
        }

        /**
         * Builds the job descriptor.
         *
         * @return Job descriptor.
         */
        public JobDescriptor<T, R> build() {
            return new JobDescriptor<>(
                    jobClassName,
                    units == null ? List.of() : units,
                    options == null ? JobExecutionOptions.DEFAULT : options,
                    argumentMarshaller,
                    resultMarshaller
            );
        }
    }
}
