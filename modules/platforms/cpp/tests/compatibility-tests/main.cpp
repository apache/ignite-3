// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "ignite_runner.h"
#include "ignite_xml_unit_test_result_printer.h"
#include "test_utils.h"

#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <cstring>

namespace {

/** Shutdown handler that cleans up resources. */
std::function<void(int)> shutdown_handler;

/**
 * Receives OS signal and handles it.
 *
 * @param signum Signal value.
 */
void signal_handler(int signum) {
    shutdown_handler(signum);
    signal(signum, SIG_DFL);
    raise(signum);
}

} // namespace

/**
 * Sets process abortion (SIGABRT, SIGINT, SIGSEGV signals) handler.
 *
 * @param handler Abortion handler.
 */
void set_process_abort_handler(std::function<void(int)> handler) {
    shutdown_handler = std::move(handler);

    // Install signal handlers to clean up resources on early exit.
    signal(SIGABRT, signal_handler);
    signal(SIGINT, signal_handler);
    signal(SIGSEGV, signal_handler);

#ifndef _WIN32
    // Ignore SIGPIPE to prevent process termination when writing to a closed socket.
    signal(SIGPIPE, SIG_IGN);
#endif
}

using namespace ignite;

const std::vector<std::string> DEFAULT_VERSIONS = {"3.0.0", "3.1.0"};

/**
 * Structure to store argument values for automatic memory management.
 */
struct ArgumentValuesHolder {
private:
    int m_argc;
    std::vector<std::string> m_vals;
    char **m_argv;

public:
    ArgumentValuesHolder(int argc, std::vector<std::string> vals)
        : m_argc(argc)
        , m_vals(std::move(vals))
        , m_argv(new char *[argc])
    {

        int idx = 0;
        for (auto& val: m_vals) {
            m_argv[idx++] = &val.front();
        }
    }

    ~ArgumentValuesHolder() {
        delete[] m_argv;
    }

    ArgumentValuesHolder(const ArgumentValuesHolder &other) = delete;

    ArgumentValuesHolder(ArgumentValuesHolder &&other) = delete;

    ArgumentValuesHolder &operator=(const ArgumentValuesHolder &other) = delete;

    ArgumentValuesHolder &operator=(ArgumentValuesHolder &&other) = delete;

    char** get_argv() {
        return m_argv;
    }
};

/**
 * Creates modified copy of argument values in order to influence test framework behavior for different versions.
 * @param argc Argument count.
 * @param argv Argument values.
 * @param version Compatibility version.
 * @return An object which contains modified argument values.
 */
ArgumentValuesHolder override_xml_output_parameter(int argc, char **argv, const std::string &version) {
    std::vector<std::string> vals;
    vals.reserve(argc);
    for (int i = 0; i < argc; ++i) {
        std::string s = argv[i];

        if (s.find("--gtest_output=xml:") == 0) {
            if (auto pos = s.rfind(".xml"); pos == s.size() - 4) {
                s.insert(pos, version);
            }
        }

        vals.push_back(s);
    }

    return ArgumentValuesHolder{argc, std::move(vals)};
}

/**
 * Replaces default xml printer with ours which adds version information to report.
 * @param version Compatibility server version
 */
void replace_xml_print(const std::string &version) {
    auto &test_event_listeners = ::testing::UnitTest::GetInstance()->listeners();

    ::testing::TestEventListener *xml = test_event_listeners.default_xml_generator();

    if (xml) {
        test_event_listeners.Release(xml);

        test_event_listeners.Append(new detail::ignite_xml_unit_test_result_printer(xml, version));
    }
}

int run_tests(int argc, char **argv, const std::string &version) {
    // In case when RUN_ALL_TESTS() not started;
    int res = 4;
    try {
        std::cout << "[**********] " << "Compatibility suite for version " << version << " started." << std::endl;

        auto holder = override_xml_output_parameter(argc, argv, version);

        ::testing::InitGoogleTest(&argc, holder.get_argv());

        replace_xml_print(version);

        res = RUN_ALL_TESTS();

        std::cout << "[**********] " << "Compatibility suite for version " << version << " finished" << std::endl;
    } catch (const std::exception &err) {
        std::cout << "[" << version << "] Uncaught error: " << err.what() << std::endl;
        res = std::min(res, 2);
    } catch (...) {
        std::cout << "[" << version << "] Unknown uncaught error" << std::endl;
        res = std::min(res, 3);
    }

    return res;
}

int main(int argc, char **argv) {
    auto ver_override = detail::get_env("IGNITE_CPP_TESTS_COMPATIBILITY_VERSIONS_OVERRIDE");

    const auto &versions = ver_override
    ? [&ver_override]() {
        std::cout << "Parsing compatibility version override: " << *ver_override << "\n";

        std::vector<std::string> v;
        size_t l = 0;
        size_t r;
        do {
            r = ver_override->find(',', l);
            v.push_back(ver_override->substr(l, r - l));
            l = r + 1;
        } while (r != std::string::npos);

        return v;
    }()
    : DEFAULT_VERSIONS;

    int res = 0;
    for (size_t i = 0; i < versions.size(); ++i) {
        const std::string& version = versions[i];

        ignite_runner runner{version};

        set_process_abort_handler([&](int signal) {
            std::cout << "Caught signal " << signal << " during tests" << std::endl;

            runner.stop();
        });

        if (!check_test_node_connectable(std::chrono::seconds(5))) {
            runner.start();
            ensure_node_connectable(std::chrono::seconds(90));
        }
        int run_res;
#ifdef _WIN32
        run_res = run_tests(argc, argv, version);
#else

        // When debugging we most likely would choose one version
        // For debug reasons last version will run in the same process
        if (i == versions.size() - 1) {
            run_res = run_tests(argc, argv, version);
        } else {
            // We fork because GTest initializes a lot of global variables which interfere with subsequent runs.
            int pid = ::fork();

            if (pid == -1) {
                std::stringstream ss;
                ss << "Can't fork process for version " << version;
                throw std::runtime_error(ss.str());
            }

            if (pid == 0) {
                run_res = run_tests(argc, argv, version);
                break;
            }

            waitpid(pid, &run_res, 0);
        }
#endif
        res = std::max(res, run_res);
    }

    return res;
}