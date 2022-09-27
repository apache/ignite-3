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

#include <vector>
#include <memory>

#include <ignite/network/data_sink.h>
#include <ignite/network/async_handler.h>

namespace ignite::network
{

/**
 * Data buffer.
 */
class DataFilter : public DataSink, public AsyncHandler
{
public:
    // Default
    DataFilter() = default;
    ~DataFilter() override = default;

    /**
     * Set sink.
     *
     * @param sink Data sink
     */
    void setSink(DataSink* sink)
    {
        m_sink = sink;
    }

    /**
     * Get sink.
     *
     * @return Data sink.
     */
    DataSink* getSink()
    {
        return m_sink;
    }

    /**
     * Set handler.
     *
     * @param handler Event handler.
     */
    void setHandler(std::weak_ptr<AsyncHandler> handler)
    {
        m_handler = std::move(handler);
    }

    /**
     * Get handler.
     *
     * @return Event handler.
     */
    std::shared_ptr<AsyncHandler> getHandler()
    {
        return m_handler.lock();
    }

protected:
    /** Sink. */
    DataSink* m_sink{nullptr};

    /** Handler. */
    std::weak_ptr<AsyncHandler> m_handler{};
};

typedef std::vector<std::shared_ptr<DataFilter>> DataFilters;

} // namespace ignite::network

