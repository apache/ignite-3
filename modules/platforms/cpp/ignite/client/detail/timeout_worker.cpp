//
// Created by Ed on 19.11.2025.
//

#include "timeout_worker.h"

namespace ignite::detail {

timeout_worker::timeout_worker() {
    this->m_thread = std::thread([this] {
        while (!this->m_stopping.load()) {
            std::this_thread::sleep_for(this->sleep_duration);

            auto prev = this->connections.before_begin();
            for (auto curr = this->connections.begin(); curr != this->connections.end();) {
                if (auto conn_ptr = curr->lock()) {
                    conn_ptr->handle_timeouts();
                    prev = curr;
                    ++curr;
                } else {
                    curr = this->connections.erase_after(prev);
                }
            }
        }
    });
}

void timeout_worker::add_connection(const std::shared_ptr<node_connection> &connection) {
    // connections.push_front(connection);
}
} // namespace ignite::detail