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

#include <chrono>
#include <thread>

#include <gtest/gtest.h>

#include "ignite_node.h"

/**
 * Run prior to any other tests.
 */
void BeforeAll()
{
    ignite::IgniteNode node;

    // Ignite dry run to make sure everything is built, all artifacts downloaded
    // and Ignite node is ready to run.
    node.start(true);

    // Five minutes should be enough but feel free to increase.
    node.join(std::chrono::minutes(5));
    node.stop();
}


int main(int argc, char** argv)
{
    BeforeAll();

    ignite::IgniteNode testNode;
    testNode.start(false);

    // TODO: Implement node startup await
    std::this_thread::sleep_for(std::chrono::seconds(20));

    ::testing::InitGoogleTest(&argc, argv);
    auto res = RUN_ALL_TESTS();

    testNode.stop();

    return res;
}
