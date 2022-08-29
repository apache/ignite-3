/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <thread>
#include <chrono>

#include <gtest/gtest.h>

#include "ignite_node.h"

class ClientTest : public ::testing::Test {
protected:
    ClientTest() = default;
    ~ClientTest() = default;

    void SetUp() override
    {
        // Code here will be called immediately after the constructor (right
        // before each test).
    }

    void TearDown() override
    {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }
};

TEST_F(ClientTest, TestTest)
{
    std::cout << "Hello" << std::endl;

    ignite::IgniteNode node;

    node.start();

    for (int i = 0; i < 20; ++i)
    {
        std::cout << node.getOutput();

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    node.stop();

    for (int i = 0; i < 2; ++i)
    {
        std::cout << node.getOutput();

        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}