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

public class JobDescriptor {
    private final String jobClassName;

    private final List<DeploymentUnit> units;

    private final JobExecutionOptions options;

    public JobDescriptor(String jobClassName) {
        this(jobClassName, List.of(), JobExecutionOptions.DEFAULT);
    }

    public JobDescriptor(String jobClassName, List<DeploymentUnit> units) {
        this(jobClassName, units, JobExecutionOptions.DEFAULT);
    }

    public JobDescriptor(String jobClassName, List<DeploymentUnit> units, JobExecutionOptions options) {
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
}
