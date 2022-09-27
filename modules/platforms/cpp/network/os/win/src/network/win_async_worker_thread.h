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

#include <cstdint>
#include <thread>

#include "common/ignite_error.h"

namespace ignite::network
{

/** Windows async client pool. */
class WinAsyncClientPool;

/**
 * Async pool worker thread.
 */
class WinAsyncWorkerThread
{
public:
    /**
     * Constructor.
     */
    WinAsyncWorkerThread();

    /**
     * Start thread.
     *
     * @param clientPool Client pool.
     * @param iocp Valid IOCP instance handle.
     */
    void start(WinAsyncClientPool& clientPool, HANDLE iocp);

    /**
     * Stop thread.
     */
    void stop();

private:
    /**
     * Run thread.
     */
    void run();

    /** Thread. */
    std::thread m_thread;

    /** Flag to signal that thread should stop. */
    volatile bool m_stopping;

    /** Client pool. */
    WinAsyncClientPool* m_clientPool;

    /** IO Completion Port. Windows-specific primitive for asynchronous IO. */
    HANDLE m_iocp;
};

} // namespace ignite::network
