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

#pragma once

#include "ignite/client/compute/deployment_unit.h"
#include "ignite/client/compute/job_execution_options.h"

#include <string>

namespace ignite {

/**
 * Compute job descriptor.
 */
class job_descriptor {
public:
    /**
     * Default constructor.
     */
    job_descriptor() = default;

    /**
     * Get job class name.
     *
     * @return Job class name.
     */
    [[nodiscard]] const std::string &get_job_class_name() const { return m_job_class_name; }

    /**
     * Set job class name.
     *
     * @param job_class_name Job class name.
     */
    void set_job_class_name(const std::string &job_class_name) { m_job_class_name = job_class_name; }

    /**
     * Get deployment units.
     *
     * @return Deployment units.
     */
    [[nodiscard]] const std::vector<deployment_unit> &get_deployment_units() const { return m_units; }

    /**
     * Get deployment units.
     *
     * @return Deployment units.
     */
    [[nodiscard]] std::vector<deployment_unit> &get_deployment_units() { return m_units; }

    /**
     * Set deployment units.
     *
     * @param units Deployment units to set.
     */
    void set_deployment_units(std::vector<deployment_unit> units) { m_units = std::move(units); }

    /**
     * Get execution options.
     *
     * @return Execution options.
     */
    [[nodiscard]] const job_execution_options &get_execution_options() const { return m_options; }

    /**
     * Set execution options.
     *
     * @param options Execution options.
     */
    void set_execution_options(const job_execution_options &options) { m_options = options; }

private:
    /** Job class name. */
    std::string m_job_class_name;

    /** Units. */
    std::vector<deployment_unit> m_units;

    /** Options. */
    job_execution_options m_options;
};

} // namespace ignite
