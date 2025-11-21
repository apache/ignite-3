//
// Created by Ed on 19.11.2025.
//

#pragma once

#include <atomic>
#include <chrono>
#include <forward_list>
#include <memory>
#include <thread>
#include "node_connection.h"

namespace ignite::detail {

class timeout_worker {
public:
    timeout_worker();

    void add_connection(const std::shared_ptr<node_connection>& connection);
private:
    std::chrono::milliseconds sleep_duration{500};

    std::thread m_thread;

    std::atomic<bool> m_stopping{false};

    std::forward_list<std::weak_ptr<node_connection>> connections;
};
} // namespace ignite::detail
