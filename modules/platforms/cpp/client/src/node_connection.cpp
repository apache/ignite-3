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

#include "ignite/protocol/utils.h"

#include "node_connection.h"

namespace ignite::detail
{

NodeConnection::NodeConnection(uint64_t id, std::shared_ptr<network::AsyncClientPool> pool,
    std::shared_ptr<IgniteLogger> logger) :
    m_id(id),
    m_pool(std::move(pool)),
    m_reqIdGen(0),
    m_logger(std::move(logger)) { }

void NodeConnection::processMessage(const network::DataBuffer &msg)
{
    protocol::Reader reader(msg.getBytesView());

    auto responseType = reader.readInt32();
    if (MessageType(responseType) != MessageType::RESPONSE) {
        m_logger->logWarning("Unsupported message type: " + std::to_string(responseType));
        return;
    }

    auto reqId = reader.readInt64();
    auto handler = getAndRemoveHandler(reqId);

    if (!handler) {
        m_logger->logError("Missing handler for request with id=" + std::to_string(reqId));
        return;
    }

    auto err = protocol::readError(reader);
    if (err) {
        handler->setError(std::move(err.value()));
        m_logger->logError("Error: " + err->whatStr());
        return;
    }

    handler->handle(reader);
}

std::shared_ptr<ResponseHandler> NodeConnection::getAndRemoveHandler(int64_t id)
{
    std::lock_guard<std::mutex> lock(m_requestHandlersMutex);

    auto it = m_requestHandlers.find(id);
    if (it == m_requestHandlers.end())
        return {};

    auto res = std::move(it->second);
    m_requestHandlers.erase(it);

    return res;
}

} // namespace ignite::detail
