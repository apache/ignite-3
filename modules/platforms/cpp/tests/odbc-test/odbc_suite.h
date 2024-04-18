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

#ifdef _WIN32
# include <windows.h>
#endif

#include "ignite_runner.h"
#include "odbc_connection.h"
#include "odbc_test_utils.h"
#include "test_utils.h"

#include "tests/test-common/basic_auth_test_suite.h"

#include <gtest/gtest.h>

#include <memory>
#include <string_view>

#include <sql.h>
#include <sqlext.h>

namespace ignite {

/**
 * Test suite.
 */
class odbc_suite : public virtual ::testing::Test, public odbc_connection {
public:
    static inline const std::string TABLE_1 = "tbl1";
    static inline const std::string TABLE_NAME_ALL_COLUMNS = "tbl_all_columns";
    static inline const std::string TABLE_NAME_ALL_COLUMNS_SQL = "tbl_all_columns_sql";

    static constexpr const char *KEY_COLUMN = "key";
    static constexpr const char *VAL_COLUMN = "val";

    static inline const std::string DRIVER_NAME = "Apache Ignite 3";

    /**
     * Get node addresses to use for tests.
     *
     * @return Addresses.
     */
    static std::string get_nodes_address() {
        std::string res;
        for (const auto &addr : ignite_runner::get_node_addrs())
            res += addr + ',';

        return res;
    }

    /**
     * Get node addresses to use for tests.
     *
     * @return Addresses.
     */
    static std::string get_basic_connection_string() {
        return "driver={" + DRIVER_NAME + "};address=" + get_nodes_address() + ';';
    }
};

} // namespace ignite
