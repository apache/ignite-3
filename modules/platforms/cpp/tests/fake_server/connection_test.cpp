//
// Created by Ed on 21.11.2025.
//

#include "tests/client-test/ignite_runner_suite.h"
#include "ignite/client/ignite_client.h"
#include "fake_server.h"

#include <gtest/gtest.h>
#include <thread>

using namespace ignite;

class connection_test : public ignite_runner_suite {

};


TEST_F(connection_test, handshake_with_fake_server) {
    using namespace std::chrono_literals;
    fake_server fs{};

    fs.start();

    ignite_client_configuration cfg;
    cfg.set_logger(get_logger());
    cfg.set_endpoints({"127.0.0.1:10800"});

    auto cl = ignite_client::start(cfg, 5s);
}

TEST_F(connection_test, request_timeout) {
    using namespace std::chrono_literals;
    fake_server fs{
        10800,
        [](protocol::client_operation op) -> std::unique_ptr<response_action> {
            switch (op) {
                case protocol::client_operation::CLUSTER_GET_NODES:
                    return std::make_unique<drop_action>();
                default:
                    return nullptr;
            }
        }
    };

    fs.start();

    ignite_client_configuration cfg;
    cfg.set_logger(get_logger());
    cfg.set_endpoints({"127.0.0.1:10800"});
    cfg.set_operation_timeout(std::chrono::milliseconds{100});

    auto cl = ignite_client::start(cfg, 5s);

    try {
        auto cluster_nodes = cl.get_cluster_nodes();
    } catch (ignite_error& err) {
        std::cout << err.what() << "\n";
    }
}