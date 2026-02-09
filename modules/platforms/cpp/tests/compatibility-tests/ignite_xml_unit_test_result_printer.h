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

#pragma once

#include <gtest/gtest.h>

namespace ignite::detail {
/**
 * Wrapper for default GTest event listener responsible for xml report.
 * We override test suite name to include version information in order to distinguish results on TeamCity output.
 * GTest do not provide any API to manipulate test information so some hacks were introduced
 * at ignite::detail::ignite_xml_unit_test_result_printer::OnTestIterationEnd
 * Name requirement for test suite is enforced: test suite should end with '_ign_version' which would be replaced
 * with actual version in xml report.
 */
class ignite_xml_unit_test_result_printer : public ::testing::EmptyTestEventListener {
    TestEventListener *m_delegate;
    std::string m_version;
public:
    ignite_xml_unit_test_result_printer(::testing::TestEventListener *delegate, std::string version);

    ~ignite_xml_unit_test_result_printer() override {
        delete m_delegate;
    }

    void OnTestProgramStart(const testing::UnitTest &) override;
    void OnTestIterationStart(const testing::UnitTest &, int) override;
    void OnEnvironmentsSetUpStart(const testing::UnitTest &) override;
    void OnEnvironmentsSetUpEnd(const testing::UnitTest &) override;
    void OnTestSuiteStart(const testing::TestSuite &) override;
    void OnTestCaseStart(const testing::TestCase &) override;
    void OnTestStart(const testing::TestInfo &) override;
    void OnTestDisabled(const testing::TestInfo &) override;
    void OnTestPartResult(const testing::TestPartResult &) override;
    void OnTestEnd(const testing::TestInfo &) override;
    void OnTestSuiteEnd(const testing::TestSuite &) override;
    void OnTestCaseEnd(const testing::TestCase &) override;
    void OnEnvironmentsTearDownStart(const testing::UnitTest &) override;
    void OnEnvironmentsTearDownEnd(const testing::UnitTest &) override;
    void OnTestIterationEnd(const testing::UnitTest &, int) override;
    void OnTestProgramEnd(const testing::UnitTest &) override;
};
} // namespace ignite::detail