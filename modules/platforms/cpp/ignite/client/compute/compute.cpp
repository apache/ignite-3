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

#include "ignite/client/compute/compute.h"
#include "ignite/client/detail/compute/compute_impl.h"

#include <random>

namespace ignite {

template<typename T>
typename T::value_type get_random_element(const T &cont) {
    // TODO: Move to utils
    static std::mutex randomMutex;
    static std::random_device rd;
    static std::mt19937 gen(rd());

    assert(!cont.empty());

    std::uniform_int_distribution<size_t> distrib(0, cont.size() - 1);

    std::lock_guard<std::mutex> lock(randomMutex);

    return cont[distrib(gen)];
}

/**
 * Check value argument.
 *
 * @param value Value tuple.
 */
template<typename T>
void inline check_non_empty(const T &cont, const std::string& title) {
    if (cont.empty())
        throw ignite_error(title + " can not be empty");
}

void compute::execute_async(const std::vector<cluster_node>& nodes, std::string_view job_class_name,
    const std::vector<primitive>& args, ignite_callback<std::optional<primitive>> callback) {
    check_non_empty(nodes, "Nodes container");
    check_non_empty(job_class_name, "Job class name");

    m_impl->execute_on_one_node(get_random_element(nodes), job_class_name, args, std::move(callback));
}

} // namespace ignite
