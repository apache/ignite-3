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

public class JobDescriptor {
    private final String jobClassName;

    private final List<DeploymentUnit> units;

    private final JobExecutionOptions options;

    private JobDescriptor(String jobClassName, List<DeploymentUnit> units, JobExecutionOptions options) {
        this.jobClassName = jobClassName;
        this.units = units;
        this.options = options;
    }

    public String jobClassName() {
        return jobClassName;
    }

    public List<DeploymentUnit> units() {
        return units;
    }

    public JobExecutionOptions options() {
        return options;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String jobClassName;
        private List<DeploymentUnit> units;
        private JobExecutionOptions options;

        public Builder jobClassName(String jobClassName) {
            this.jobClassName = jobClassName;
            return this;
        }

        public Builder jobClass(Class<? extends ComputeJob<?>> jobClass) {
            this.jobClassName = jobClass.getName();
            return this;
        }

        public Builder units(List<DeploymentUnit> units) {
            this.units = units;
            return this;
        }

        public Builder units(DeploymentUnit... units) {
            this.units = List.of(units);
            return this;
        }

        public Builder options(JobExecutionOptions options) {
            this.options = options;
            return this;
        }

        public JobDescriptor build() {
            Objects.requireNonNull(jobClassName, "Job class name must be set");

            return new JobDescriptor(
                    jobClassName,
                    units == null ? List.of() : units,
                    options == null ? JobExecutionOptions.DEFAULT : options);
        }
    }
}
