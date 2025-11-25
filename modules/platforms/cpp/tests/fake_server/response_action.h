//
// Created by Ed on 25.11.2025.
//

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
    std::chrono::milliseconds m_delay;;
};

}