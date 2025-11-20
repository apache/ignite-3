//
// Created by Ed on 19.11.2025.
//

#include "timeout_worker.h"

namespace ignite::detail {

timeout_worker::timeout_worker() {
    this->m_thread = std::thread([this] {
        while (!this->m_stopping.load()) {
            std::this_thread::sleep_for(this->sleep_duration);

            for (auto it = this->connections.begin(); it != this->connections.end(); ++it) {
                auto conn_wptr = *it;

                if (auto conn_ptr = conn_wptr.lock()) {
                    conn_ptr->handle_timeouts();
                }

            }

        }
    });
}
} // namespace ignite::detail