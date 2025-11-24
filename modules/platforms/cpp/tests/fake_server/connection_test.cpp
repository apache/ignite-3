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
    fake_server fs;

    fs.start();

    ignite_client_configuration cfg;
    cfg.set_endpoints({"127.0.0.1:10800"});

    auto cl = ignite_client::start(cfg, 5'000'000ms);

    auto resp = cl.get_cluster_nodes();

    std::this_thread::sleep_for(1000s);
}