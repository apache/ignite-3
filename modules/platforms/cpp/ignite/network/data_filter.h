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

#include <ignite/network/async_handler.h>
#include <ignite/network/data_sink.h>

#include <memory>
#include <vector>

namespace ignite::network {

/**
 * Data buffer.
 */
class data_filter : public data_sink, public async_handler {
public:
    /**
     * Set sink.
     *
     * @param sink Data sink
     */
    void set_sink(data_sink *sink) { m_sink = sink; }

    /**
     * Get sink.
     *
     * @return Data sink.
     */
    data_sink *get_sink() { return m_sink; }

    /**
     * Set handler.
     *
     * @param handler Event handler.
     */
    void set_handler(std::weak_ptr<async_handler> handler) { m_handler = std::move(handler); }

    /**
     * Get handler.
     *
     * @return Event handler.
     */
    std::shared_ptr<async_handler> get_handler() { return m_handler.lock(); }

protected:
    /** Sink. */
    data_sink *m_sink{nullptr};

    /** Handler. */
    std::weak_ptr<async_handler> m_handler{};
};

typedef std::vector<std::shared_ptr<data_filter>> data_filters;

} // namespace ignite::network
