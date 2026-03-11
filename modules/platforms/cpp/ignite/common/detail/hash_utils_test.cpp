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

#include <ignite/common/detail/hash_utils.h>
#include <ignite/common/detail/string_extensions.h>

#include <cmath>

#include <gtest/gtest.h>

using namespace ignite;
using namespace ignite::detail;

class hash_utils_bool_test : public ::testing::Test {};

TEST_F(hash_utils_bool_test, true_value) {
    ASSERT_EQ(-105209210, hash(true));
}


TEST_F(hash_utils_bool_test, false_value) {
    ASSERT_EQ(686815056, hash(false));
}

template<typename T>
struct test_case {
    T arg;
    std::int32_t expected;
};

template<typename T>
void PrintTo(const test_case<T>& tc, std::ostream* os) {
    using std::to_string;
    using ignite::detail::to_string;

    *os << "arg = " << to_string(tc.arg);
}

class hash_utils_uint8_t_test: public ::testing::TestWithParam<test_case<std::uint8_t>> {};

TEST_P(hash_utils_uint8_t_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_uint8_t_test,
    ::testing::Values(
        test_case<uint8_t>{0, 686815056},
        test_case<uint8_t>{1, -105209210},
        test_case<uint8_t>{255, -482826348},
        test_case<uint8_t>{127, -2036967051},
        test_case<uint8_t>{128, 49585133},
        test_case<uint8_t>{42, 1990634712}
    )
);

class hash_utils_int8_t_test: public ::testing::TestWithParam<test_case<std::int8_t>> {};

TEST_P(hash_utils_int8_t_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_int8_t_test,
    ::testing::Values(
        test_case<std::int8_t>{0, 686815056},
        test_case<std::int8_t>{1, -105209210},
        test_case<std::int8_t>{-1, -482826348},
        test_case<std::int8_t>{127, -2036967051},
        test_case<std::int8_t>{-128, 49585133},
        test_case<std::int8_t>{42, 1990634712}
    )
);

class hash_utils_uint16_t_test: public ::testing::TestWithParam<test_case<std::uint16_t>> {};

TEST_P(hash_utils_uint16_t_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_uint16_t_test,
    ::testing::Values(
        test_case<std::uint16_t>{0,1076422130},
        test_case<std::uint16_t>{1,  1765994081},
        test_case<std::uint16_t>{std::numeric_limits<std::uint16_t>::max(), 22229479},
        test_case<std::uint16_t>{42,-461853154}
    )
);

class hash_utils_int16_t_test: public ::testing::TestWithParam<test_case<std::int16_t>> {};

TEST_P(hash_utils_int16_t_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_int16_t_test,
    ::testing::Values(
        test_case<std::int16_t>{0,1076422130},
        test_case<std::int16_t>{1, 1765994081},
        test_case<std::int16_t>{-1, 22229479},
        test_case<std::int16_t>{std::numeric_limits<std::int16_t>::max(), -1586500059},
        test_case<std::int16_t>{std::numeric_limits<std::int16_t>::min(), -667322922},
        test_case<std::int16_t>{42,-461853154}
    )
);

class hash_utils_uint32_t_test: public ::testing::TestWithParam<test_case<std::uint32_t>> {};

TEST_P(hash_utils_uint32_t_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_uint32_t_test,
    ::testing::Values(
        test_case<std::uint32_t>{0,401375585},
        test_case<std::uint32_t>{1,  666724619},
        test_case<std::uint32_t>{std::numeric_limits<std::uint32_t>::max(), 2008810410},
        test_case<std::uint32_t>{42,872512553}
    )
);

class hash_utils_int32_t_test: public ::testing::TestWithParam<test_case<std::int32_t>> {};

TEST_P(hash_utils_int32_t_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_int32_t_test,
    ::testing::Values(
        test_case<std::int32_t>{0, 401375585},
        test_case<std::int32_t>{1,666724619},
        test_case<std::int32_t>{-1,2008810410},
        test_case<std::int32_t>{std::numeric_limits<std::int32_t>::max(), -1452754491},
        test_case<std::int32_t>{std::numeric_limits<std::int32_t>::min(), 694163409},
        test_case<std::int32_t>{42,872512553}
    )
);

class hash_utils_uint64_t_test: public ::testing::TestWithParam<test_case<std::uint64_t>> {};

TEST_P(hash_utils_uint64_t_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_uint64_t_test,
    ::testing::Values(
        test_case<std::uint64_t>{0,-460808068},
        test_case<std::uint64_t>{1,  -79575043},
        test_case<std::uint64_t>{std::numeric_limits<std::uint64_t>::max(), -1168220407},
        test_case<std::uint64_t>{42,1065270881}
    )
);

class hash_utils_int64_t_test: public ::testing::TestWithParam<test_case<std::int64_t>> {};

TEST_P(hash_utils_int64_t_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_int64_t_test,
    ::testing::Values(
        test_case<std::int64_t>{0, -460808068},
        test_case<std::int64_t>{1,-79575043},
        test_case<std::int64_t>{-1,-1168220407},
        test_case<std::int64_t>{std::numeric_limits<std::int64_t>::max(), -1230994913},
        test_case<std::int64_t>{std::numeric_limits<std::int64_t>::min(), -1253265447},
        test_case<std::int64_t>{42,1065270881}
    )
);

class hash_utils_float_test: public ::testing::TestWithParam<test_case<float>> {};

TEST_P(hash_utils_float_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_float_test,
    ::testing::Values(
        test_case<float>{0.0, 401375585},
        test_case<float>{1.0,-132467364},
        test_case<float>{-1.0,-2101035902},
        test_case<float>{std::numeric_limits<float>::max(), 1780074225},
        test_case<float>{42.0F,1227011061},
        test_case<float>{float{M_PI},-2142471555}
    )
);

class hash_utils_double_test: public ::testing::TestWithParam<test_case<double>> {};

TEST_P(hash_utils_double_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_double_test,
    ::testing::Values(
        test_case<double>{0.0, -460808068},
        test_case<double>{1.0,-2124529649},
        test_case<double>{-1.0,210701226},
        test_case<double>{std::numeric_limits<double>::max(), -1921889547},
        test_case<double>{42.0F,2109601987},
        test_case<double>{double{M_PI},-521675661}
    )
);

class hash_utils_big_decimal_test: public ::testing::TestWithParam<test_case<big_decimal>> {};

TEST_P(hash_utils_big_decimal_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg, 10));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_big_decimal_test,
    ::testing::Values(
        test_case<big_decimal>{big_decimal{0}, 686815056},
        test_case<big_decimal>{big_decimal{1},904832652},
        test_case<big_decimal>{big_decimal{-1},-1497790521},
        test_case<big_decimal>{big_decimal{42},1347451647},
        test_case<big_decimal>{big_decimal::from_double(M_PI),572053262},
        test_case<big_decimal>{big_decimal::from_double(M_PI*-1),-2078930604}
    )
);

class hash_utils_uuid_test: public ::testing::TestWithParam<test_case<uuid>> {};

TEST_P(hash_utils_uuid_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_uuid_test,
    ::testing::Values(
        test_case<uuid>{uuid{0,0}, -177475040},
        test_case<uuid>{uuid{420000042, 420042}, -2034825191}
    )
);

class hash_utils_date_test: public ::testing::TestWithParam<test_case<ignite_date>> {};

TEST_P(hash_utils_date_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_date_test,
    ::testing::Values(
        test_case<ignite_date>{ignite_date{1970, 1, 1}, -1730003579},
        test_case<ignite_date>{ignite_date{1905, 9, 26}, -1268080282},
        test_case<ignite_date>{ignite_date{2036, 3, 17}, 435798353}
    )
);

class hash_utils_time_test: public ::testing::TestWithParam<test_case<ignite_time>> {};

TEST_P(hash_utils_time_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg, 5));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_time_test,
    ::testing::Values(
        test_case<ignite_time>{ignite_time{0, 0}, -1321305871},
        test_case<ignite_time>{ignite_time{0,0, 1}, -975644488},
        test_case<ignite_time>{ignite_time{0, 0, 0, 1}, -1321305871},
        test_case<ignite_time>{ignite_time{0, 0, 0, 10'000}, -1417871408},
        test_case<ignite_time>{ignite_time{0, 0, 0, 10'001}, -1417871408},
        test_case<ignite_time>{ignite_time{19, 42, 21, 42'042}, 57055814}
    )
);

class hash_utils_date_time_test: public ::testing::TestWithParam<test_case<ignite_date_time>> {};

TEST_P(hash_utils_date_time_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg, 5));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_date_time_test,
    ::testing::Values(
        test_case<ignite_date_time>{ignite_date_time{{1970, 1, 1}, {0,0}}, -986160737},
        test_case<ignite_date_time>{ignite_date_time{{1905, 9, 26}, {19, 42, 21}}, -1487071575},
        test_case<ignite_date_time>{ignite_date_time{{2036, 3, 17}, {21, 21, 21, 12'000}}, 1007809846},
        test_case<ignite_date_time>{ignite_date_time{{2036, 3, 17}, {21, 21, 21, 12'001}}, 1007809846}
    )
);

class hash_utils_timestamp_test: public ::testing::TestWithParam<test_case<ignite_timestamp>> {};

TEST_P(hash_utils_timestamp_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg, 5));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_timestamp_test,
    ::testing::Values(
        test_case<ignite_timestamp>{{0,0}, -1028348915},
        test_case<ignite_timestamp>{{0, 420'000}, 789406184},
        test_case<ignite_timestamp>{{1773221571, 21'123}, 670786278},
        test_case<ignite_timestamp>{{1773221571, 21'000}, 670786278}
    )
);


template<>
void PrintTo(const test_case<std::string>& tc, std::ostream* os) {
    *os << "arg = " << tc.arg;
}

class hash_utils_string_test: public ::testing::TestWithParam<test_case<std::string>> {};

TEST_P(hash_utils_string_test, simple) {
    auto param = GetParam();

    ASSERT_EQ(param.expected, hash(param.arg));
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_string_test,
    ::testing::Values(
        test_case<std::string>{"", 0},
        test_case<std::string>{" ", -2039187927},
        test_case<std::string>{"foo", -477838538},
        test_case<std::string>{"foo ", 1516292748},
        test_case<std::string>{" foo", -1406774036},
        test_case<std::string>{"Foo", 2010420341},
        test_case<std::string>{"bar", 1863106271},
        test_case<std::string>{"baR", -1753335891},
        test_case<std::string>{"the quick brown fox jumped over the lazy dog", 1452923692},
        test_case<std::string>{"Карл у Клары украл кораллы", -1909478343}
    )
);


template<>
void PrintTo(const test_case<std::vector<std::byte>>& tc, std::ostream* os) {
    *os << "arg = " << "[";
    *os << std::hex;
    for (auto b : tc.arg) {
         *os << "0x" << static_cast<uint32_t>(b) << ",";
    }

    *os << "]";
}

class hash_utils_bytes_test: public ::testing::TestWithParam<test_case<std::vector<std::byte>>> {};

TEST_P(hash_utils_bytes_test, simple) {
    auto param = GetParam();
    ASSERT_EQ(param.expected, hash(param.arg));
}

std::vector<std::byte> from_ints(std::vector<int> ints) {
    std::vector<std::byte> res;
    res.reserve(ints.size());

    std::transform(
        ints.begin(),
        ints.end(),
        std::back_insert_iterator(res),
        [](int src) {
            return static_cast<std::byte>(src);
        }
        );

    return res;
}

INSTANTIATE_TEST_SUITE_P(
    simple,
    hash_utils_bytes_test,
    ::testing::Values(
        test_case<std::vector<std::byte>>{{},0},
        test_case<std::vector<std::byte>>{from_ints({1,2,3}),343345478},
        test_case<std::vector<std::byte>>{from_ints({3,2,1}),-1822332709},
        test_case<std::vector<std::byte>>{from_ints({42,42,42}),246686034},
        test_case<std::vector<std::byte>>{from_ints({-1,1}),-1645252459},
        test_case<std::vector<std::byte>>{std::vector(4, std::byte{0}),401375585},
        test_case<std::vector<std::byte>>{std::vector(5, std::byte{0}),65263045},
        test_case<std::vector<std::byte>>{std::vector(8, std::byte{0}),-460808068},
        test_case<std::vector<std::byte>>{std::vector(1000, std::byte{0x20}),838145700}
    )
);