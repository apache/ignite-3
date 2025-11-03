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

#include "ignite_xml_unit_test_result_printer.h"

namespace ignite::detail {

ignite_xml_unit_test_result_printer::ignite_xml_unit_test_result_printer(
    testing::TestEventListener *delegate, std::string version)
        : m_delegate(delegate)
        , m_version(std::move(version)){}

void ignite_xml_unit_test_result_printer::OnTestProgramStart(const ::testing::UnitTest &unit_test) {
    m_delegate->OnTestProgramStart(unit_test);
}

void ignite_xml_unit_test_result_printer::OnTestIterationStart(const ::testing::UnitTest &unit_test, int iteration) {
    m_delegate->OnTestIterationStart(unit_test, iteration);
}

void ignite_xml_unit_test_result_printer::OnEnvironmentsSetUpStart(const ::testing::UnitTest &unit_test) {
    m_delegate->OnEnvironmentsSetUpStart(unit_test);
}

void ignite_xml_unit_test_result_printer::OnEnvironmentsSetUpEnd(const ::testing::UnitTest &unit_test) {
    m_delegate->OnEnvironmentsSetUpEnd(unit_test);
}

void ignite_xml_unit_test_result_printer::OnTestSuiteStart(const ::testing::TestSuite &test_suite) {
    m_delegate->OnTestSuiteStart(test_suite);
}

void ignite_xml_unit_test_result_printer::OnTestSuiteEnd(const ::testing::TestSuite &test_suite) {
    m_delegate->OnTestSuiteEnd(test_suite);
}

void ignite_xml_unit_test_result_printer::OnTestCaseStart(const ::testing::TestCase &test_case) {
    m_delegate->OnTestCaseStart(test_case);
}

void ignite_xml_unit_test_result_printer::OnTestCaseEnd(const ::testing::TestCase &test_case) {
    m_delegate->OnTestCaseEnd(test_case);
}

void ignite_xml_unit_test_result_printer::OnTestStart(const ::testing::TestInfo &test_info) {
    m_delegate->OnTestStart(test_info);
}

void ignite_xml_unit_test_result_printer::OnTestDisabled(const testing::TestInfo &test_info) {
    m_delegate->OnTestDisabled(test_info);
}

void ignite_xml_unit_test_result_printer::OnTestPartResult(const ::testing::TestPartResult &test_part_result) {
    m_delegate->OnTestPartResult(test_part_result);
}

void ignite_xml_unit_test_result_printer::OnTestEnd(const ::testing::TestInfo &test_info) {
    m_delegate->OnTestEnd(test_info);
}

void ignite_xml_unit_test_result_printer::OnEnvironmentsTearDownStart(const ::testing::UnitTest &unit_test) {
    m_delegate->OnEnvironmentsTearDownStart(unit_test);
}

void ignite_xml_unit_test_result_printer::OnEnvironmentsTearDownEnd(const ::testing::UnitTest &unit_test) {
    m_delegate->OnEnvironmentsTearDownEnd(unit_test);
}

void ignite_xml_unit_test_result_printer::OnTestIterationEnd(const ::testing::UnitTest &unit_test, int iteration) {
    for (int i = 0; i < unit_test.total_test_case_count(); ++i) {
        // We are extracting test suite info to add version info
        const testing::TestSuite *ts = unit_test.GetTestSuite(i);

        // because underlying storage is std::string we able to override it content without changing length.
        char *s = const_cast<char *>(ts->name());

        std::string_view sw = s;

        std::string_view suffix = "_ign_version";

        if (sw.rfind(suffix) != sw.size() - suffix.size()) {
            std::stringstream ss;
            ss << "Expected test suite name to have suffix '"<< suffix <<"' but got [name = "<< sw << "]";
            throw std::runtime_error(ss.str());
        }

        // it is possible to have more complex version text like 9.1.18-p3, etc.
        if (m_version.size() >= suffix.size()) {
            std::stringstream ss;
            ss << "Expected version string to be shorter than a suffix but got "
               << "[version = " << m_version << "; suf  fix = "<< suffix <<"]";
            throw std::runtime_error(ss.str());
        }

        auto s_it = s + (sw.size() - suffix.size() + 1 /*skip leading '_'*/);
        auto s_end = s + sw.size();
        for (auto it = m_version.begin(); it != m_version.end(); ++it, ++s_it) {
            char c = *it;
            c = c == '.' ? '_' : c;// Teamcity treats '.' specifically.
            *s_it = c;
        }

        while (s_it != s_end) {
            *s_it = '_';
            ++s_it;
        }
    }
    m_delegate->OnTestIterationEnd(unit_test, iteration);
}

void ignite_xml_unit_test_result_printer::OnTestProgramEnd(const ::testing::UnitTest &unit_test) {
    m_delegate->OnTestProgramEnd(unit_test);
}
} // namespace ignite::detail