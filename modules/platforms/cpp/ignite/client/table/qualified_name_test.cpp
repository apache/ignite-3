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

#include "ignite/client/table/qualified_name.h"
#include "ignite/common/ignite_error.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

using namespace ignite;
using namespace detail;


TEST(client_qualified_name, create_empty_schema_empty_name_throws) {
    EXPECT_THROW(
        {
            try {
                (void)qualified_name::create({}, {});
            } catch (const ignite_error &e) {
                EXPECT_EQ(e.get_status_code(), error::code::ILLEGAL_ARGUMENT);
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Object name can not be empty"));
                throw;
            }
        },
        ignite_error);
}

TEST(client_qualified_name, create_empty_name_throws) {
    EXPECT_THROW(
        {
            try {
                (void)qualified_name::create("TEST", {});
            } catch (const ignite_error &e) {
                EXPECT_EQ(e.get_status_code(), error::code::ILLEGAL_ARGUMENT);
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Object name can not be empty"));
                throw;
            }
        },
        ignite_error);
}

TEST(client_qualified_name, parse_empty_name_throws) {
    EXPECT_THROW(
        {
            try {
                (void)qualified_name::parse({});
            } catch (const ignite_error &e) {
                EXPECT_EQ(e.get_status_code(), error::code::ILLEGAL_ARGUMENT);
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Object name can not be empty"));
                throw;
            }
        },
        ignite_error);
}

TEST(client_qualified_name, create_empty_schema_default) {
    auto name = qualified_name::create({}, "TEST");
    EXPECT_EQ(name.get_schema_name(), qualified_name::DEFAULT_SCHEMA_NAME);
}

TEST(client_qualified_name, parse_implicit_schema_default) {
    auto name = qualified_name::parse("TEST");
    EXPECT_EQ(name.get_schema_name(), qualified_name::DEFAULT_SCHEMA_NAME);
}

