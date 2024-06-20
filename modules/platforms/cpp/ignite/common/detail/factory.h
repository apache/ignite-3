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

#include <memory>

namespace ignite::detail {

/**
 * Factory class.
 *
 * @tparam T Instances of this type factory builds.
 */
template<typename T>
class factory {
public:
    /**
     * Destructor.
     */
    virtual ~factory() = default;

    /**
     * Build instance.
     *
     * @return New instance of type @c T.
     */
    virtual std::unique_ptr<T> build() = 0;
};

/**
 * Basic factory class.
 *
 * @tparam TB Base type.
 * @tparam TC Concrete type.
 */
template<typename TB, typename TC>
class basic_factory : public factory<TB> {
public:
    /**
     * Destructor.
     */
    virtual ~basic_factory() = default;

    /**
     * Build instance.
     *
     * @return New instance of type @c T.
     */
    [[nodiscard]] std::unique_ptr<TB> build() override { return std::make_unique<TC>(); }
};

} // namespace ignite::detail
