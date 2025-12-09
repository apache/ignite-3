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
#include <chrono>

namespace ignite {

enum action_type { DROP, DELAY };

class response_action {
public:
    virtual action_type type() = 0;

    virtual ~response_action() = default;
};

class drop_action: public response_action {
public:
    action_type type() override {
        return DROP;
    }
};

class delayed_action: public response_action {
public:
    explicit delayed_action(std::chrono::milliseconds delay)
        : m_delay(delay) {}

    action_type type() override {
        return DELAY;
    }

    std::chrono::milliseconds delay() const {
        return m_delay;
    }

private:
    std::chrono::milliseconds m_delay;
};

}