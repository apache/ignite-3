// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#pragma once
#include <queue>
#include <utility>

#include "message.h"

namespace ignite::proxy {

class message_listener {
public:
    void register_out_message(message msg) {
        m_out_queue.push(std::move(msg));
    }

    void register_in_message(message msg) {
        m_in_queue.push(std::move(msg));
    }
private:
    std::queue<message> m_out_queue{};
    std::queue<message> m_in_queue{};
};
} // namespace ignite::proxy
