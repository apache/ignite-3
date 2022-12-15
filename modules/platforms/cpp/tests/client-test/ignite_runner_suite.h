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

#include "gtest_logger.h"

#include <gtest/gtest.h>

#include <memory>
#include <string_view>

namespace ignite {

using namespace std::string_view_literals;

/**
 * Test suite.
 */
class ignite_runner_suite : public ::testing::Test {
protected:
    static constexpr std::initializer_list<std::string_view> NODE_ADDRS = {"127.0.0.1:10942"sv, "127.0.0.1:10943"sv};
    static constexpr std::string_view TABLE_1 = "tbl1"sv;

    /**
     * Get logger.
     *
     * @return Logger for tests.
     */
    static std::shared_ptr<gtest_logger> get_logger() { return std::make_shared<gtest_logger>(false, true); }
};

} // namespace ignite
