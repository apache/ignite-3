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

#pragma once

#include <map>
#include <mutex>

#include <ignite/network/codec.h>
#include <ignite/network/data_filter_adapter.h>

namespace ignite::network
{

/**
 * Data filter that uses codecs inside to encode/decode data.
 */
class CodecDataFilter : public DataFilterAdapter
{
public:
    //Default
    ~CodecDataFilter() override = default;

    /**
     * Constructor.
     *
     * @param factory Codec factory.
     */
    explicit CodecDataFilter(std::shared_ptr<Factory<Codec>> factory);

    /**
     * Send data to specific established connection.
     *
     * @param id Client ID.
     * @param data Data to be sent.
     * @return @c true if connection is present and @c false otherwise.
     *
     * @throw IgniteError on error.
     */
    bool send(uint64_t id, std::vector<std::byte>&& data) override;

    /**
      * Callback that called on successful connection establishment.
      *
      * @param addr Address of the new connection.
      * @param id Connection ID.
      */
    void onConnectionSuccess(const EndPoint& addr, uint64_t id) override;

    /**
     * Callback that called on error during connection establishment.
     *
     * @param id Async client ID.
     * @param err Error. Can be null if connection closed without error.
     */
    void onConnectionClosed(uint64_t id, std::optional<IgniteError> err) override;

    /**
     * Callback that called when new message is received.
     *
     * @param id Async client ID.
     * @param msg Received message.
     */
    void onMessageReceived(uint64_t id, const DataBufferRef& msg) override;

private:
    /**
     * Get codec for connection.
     *
     * @param id Connection ID.
     * @return Codec if found or null.
     */
    std::shared_ptr<Codec> FindCodec(uint64_t id);

    /** Codec factory. */
    std::shared_ptr<Factory<Codec>> m_codecFactory;

    /** Codecs. */
    std::map<uint64_t, std::shared_ptr<Codec>> m_codecs;

    /** Mutex for secure access to codecs map. */
    std::mutex m_codecsMutex;
};

} // namespace ignite::network
