/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define _USE_MATH_DEFINES
#include <cmath>

#include "binary_tuple_builder.h"
#include "binary_tuple_parser.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <optional>
#include <random>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

using namespace std::string_literals;
using namespace ignite;

/** A set of binary values for a whole or partial row. */
using tuple_view = std::vector<bytes_view>;

template<typename T>
T get_value(bytes_view data) {
    T res;
    if constexpr (std::is_same<T, std::int8_t>::value) {
        res = binary_tuple_parser::get_int8(data);
    } else if constexpr (std::is_same<T, std::int16_t>::value) {
        res = binary_tuple_parser::get_int16(data);
    } else if constexpr (std::is_same<T, std::int32_t>::value) {
        res = binary_tuple_parser::get_int32(data);
    } else if constexpr (std::is_same<T, std::int64_t>::value) {
        res = binary_tuple_parser::get_int64(data);
    } else if constexpr (std::is_same<T, float>::value) {
        res = binary_tuple_parser::get_float(data);
    } else if constexpr (std::is_same<T, double>::value) {
        res = binary_tuple_parser::get_double(data);
    } else if constexpr (std::is_same<T, big_integer>::value) {
        res = binary_tuple_parser::get_number(data);
    } else if constexpr (std::is_same<T, ignite_date>::value) {
        res = binary_tuple_parser::get_date(data);
    } else if constexpr (std::is_same<T, ignite_time>::value) {
        res = binary_tuple_parser::get_time(data);
    } else if constexpr (std::is_same<T, ignite_date_time>::value) {
        res = binary_tuple_parser::get_date_time(data);
    } else if constexpr (std::is_same<T, ignite_timestamp>::value) {
        res = binary_tuple_parser::get_timestamp(data);
    } else if constexpr (std::is_same<T, uuid>::value) {
        res = binary_tuple_parser::get_uuid(data);
    } else if constexpr (std::is_same<T, std::string>::value) {
        res = std::string(binary_tuple_parser::get_varlen(data));
    } else if constexpr (std::is_same<T, big_decimal>::value) {
        res = big_decimal(binary_tuple_parser::get_decimal(data));
    } else if constexpr (std::is_same<T, bool>::value) {
        res = binary_tuple_parser::get_bool(data);
    } else if constexpr (std::is_same<T, ignite_period>::value) {
        res = binary_tuple_parser::get_period(data);
    } else if constexpr (std::is_same<T, ignite_duration>::value) {
        res = binary_tuple_parser::get_duration(data);
    } else {
        throw std::logic_error("Type is not supported");
    }
    return res;
}

big_decimal get_decimal(bytes_view data, int16_t scale) {
    return binary_tuple_parser::get_decimal(data, scale);
}

struct builder : binary_tuple_builder {
    using binary_tuple_builder::binary_tuple_builder;

    template<typename T>
    void claim_value(const T &value) {
        if constexpr (std::is_same<T, std::nullopt_t>::value) {
            claim_null();
        } else if constexpr (std::is_same<T, std::int8_t>::value) {
            claim_int8(value);
        } else if constexpr (std::is_same<T, std::int16_t>::value) {
            claim_int16(value);
        } else if constexpr (std::is_same<T, std::int32_t>::value) {
            claim_int32(value);
        } else if constexpr (std::is_same<T, std::int64_t>::value) {
            claim_int64(value);
        } else if constexpr (std::is_same<T, float>::value) {
            claim_float(value);
        } else if constexpr (std::is_same<T, double>::value) {
            claim_double(value);
        } else if constexpr (std::is_same<T, std::string>::value) {
            claim_varlen(value);
        } else if constexpr (std::is_same<T, big_decimal>::value || std::is_same<T, big_integer>::value) {
            claim_number(value);
        } else {
            throw std::logic_error("Type is not supported");
        }
    }

    template<typename T>
    void append_value(const T &value) {
        if constexpr (std::is_same<T, std::nullopt_t>::value) {
            append_null();
        } else if constexpr (std::is_same<T, std::int8_t>::value) {
            append_int8(value);
        } else if constexpr (std::is_same<T, std::int16_t>::value) {
            append_int16(value);
        } else if constexpr (std::is_same<T, std::int32_t>::value) {
            append_int32(value);
        } else if constexpr (std::is_same<T, std::int64_t>::value) {
            append_int64(value);
        } else if constexpr (std::is_same<T, float>::value) {
            append_float(value);
        } else if constexpr (std::is_same<T, double>::value) {
            append_double(value);
        } else if constexpr (std::is_same<T, std::string>::value) {
            append_varlen(value);
        } else if constexpr (std::is_same<T, big_decimal>::value || std::is_same<T, big_integer>::value) {
            append_number(value);
        } else {
            throw std::logic_error("Type is not supported");
        }
    }

    /**
     * @brief Assembles and returns a tuple in binary format.
     *
     * @tparam Ts Types parameter pack.
     * @param tupleArgs Elements to be appended to column.
     * @return Byte buffer with tuple in the binary form.
     */
    template<typename... Ts>
    const auto &build(std::tuple<Ts...> const &tuple) {
        start();
        std::apply([&](auto &&...args) { ((claim_value(args)), ...); }, tuple);
        layout();
        std::apply([&](auto &&...args) { ((append_value(args)), ...); }, tuple);
        return binary_tuple_builder::build();
    }
};

struct parser : binary_tuple_parser {
    using binary_tuple_parser::binary_tuple_parser;

    parser(tuple_num_t num_elements, const std::vector<int8_t> &data)
        : binary_tuple_parser(num_elements, {reinterpret_cast<const std::byte *>(data.data()), data.size()}) {}

    tuple_view parse() {
        assert(num_parsed_elements() == 0);

        tuple_num_t num = num_elements();

        tuple_view tuple;
        tuple.reserve(num);
        while (num_parsed_elements() < num) {
            tuple.emplace_back(get_next());
        }

        return tuple;
    }
};

struct single_value_check {
    explicit single_value_check(binary_tuple_parser &parser)
        : parser{parser} {}

    binary_tuple_parser &parser;

    template<typename T>
    void operator()(T value) {
        auto res = get_value<T>(parser.get_next());
        EXPECT_EQ(value, res);
    }

    void operator()(std::nullopt_t /*null*/) { EXPECT_TRUE(parser.get_next().empty()); }
};

template<typename... Ts, int size>
void check_reader_writer_equality(
    const std::tuple<Ts...> &values, const std::int8_t (&data)[size], bool skip_assembler_check = false) {
    constexpr tuple_num_t NUM_ELEMENTS = sizeof...(Ts);

    bytes_view bytes{reinterpret_cast<const std::byte *>(data), size};

    if (!skip_assembler_check) {
        builder ta(NUM_ELEMENTS);

        bytes_view built = ta.build(values);

        EXPECT_THAT(built, testing::ContainerEq(bytes));
    }

    binary_tuple_parser tp(NUM_ELEMENTS, bytes);

    std::apply([&tp](const Ts... args) { ((single_value_check(tp)(args)), ...); }, values);

    EXPECT_EQ(tp.num_elements(), tp.num_parsed_elements());
}

template<typename T>
void check_reader_writer_equality(const T &value) {
    builder ta(1);

    bytes_view built = ta.build(std::make_tuple(value));

    binary_tuple_parser tp(1, built);

    single_value_check check(tp);
    check(value);

    EXPECT_EQ(tp.num_elements(), tp.num_parsed_elements());
}

TEST(tuple, AllTypes) {
    static constexpr tuple_num_t NUM_ELEMENTS = 17;

    int8_t v1 = 1;
    int16_t v2 = 2;
    int32_t v3 = 3;
    int64_t v4 = 4;
    big_integer v5(12345);
    big_decimal v6("12345.06789");
    float v7 = .5f;
    double v8 = .7;
    ignite_date v9(2020, 12, 12);
    ignite_time v10(23, 59);
    ignite_date_time v11(v9, v10);
    ignite_timestamp v12(100, 10000);
    std::string v13("hello");
    uuid v14;

    std::string v15_tmp("\1\2\3"s);
    bytes_view v15{reinterpret_cast<const std::byte *>(v15_tmp.data()), v15_tmp.size()};

    big_decimal v16("-1");
    bool v17(false);

    binary_tuple_builder tb(NUM_ELEMENTS);
    tb.start();

    tb.claim_int8(v1);
    tb.claim_int16(v2);
    tb.claim_int32(v3);
    tb.claim_int64(v4);
    tb.claim_number(v5);
    tb.claim_number(v6);
    tb.claim_float(v7);
    tb.claim_double(v8);
    tb.claim_date(v9);
    tb.claim_time(v10);
    tb.claim_date_time(v11);
    tb.claim_timestamp(v12);
    tb.claim_varlen(v13);
    tb.claim_uuid(v14);
    tb.claim_varlen(v15);
    tb.claim_number(v16);
    tb.claim_bool(v17);

    tb.layout();

    tb.append_int8(v1);
    tb.append_int16(v2);
    tb.append_int32(v3);
    tb.append_int64(v4);
    tb.append_number(v5);
    tb.append_number(v6);
    tb.append_float(v7);
    tb.append_double(v8);
    tb.append_date(v9);
    tb.append_time(v10);
    tb.append_date_time(v11);
    tb.append_timestamp(v12);
    tb.append_varlen(v13);
    tb.append_uuid(v14);
    tb.append_varlen(v15);
    tb.append_number(v16);
    tb.append_bool(v17);

    const std::vector<std::byte> &binary_tuple = tb.build();
    binary_tuple_parser tp(NUM_ELEMENTS, binary_tuple);

    EXPECT_EQ(v1, get_value<int8_t>(tp.get_next()));
    EXPECT_EQ(v2, get_value<int16_t>(tp.get_next()));
    EXPECT_EQ(v3, get_value<int32_t>(tp.get_next()));
    EXPECT_EQ(v4, get_value<int64_t>(tp.get_next()));
    EXPECT_EQ(v5, get_value<big_integer>(tp.get_next()));
    EXPECT_EQ(v6, get_decimal(tp.get_next(), v6.get_scale()));
    EXPECT_EQ(v7, get_value<float>(tp.get_next()));
    EXPECT_EQ(v8, get_value<double>(tp.get_next()));
    EXPECT_EQ(v9, get_value<ignite_date>(tp.get_next()));
    EXPECT_EQ(v10, get_value<ignite_time>(tp.get_next()));
    EXPECT_EQ(v11, get_value<ignite_date_time>(tp.get_next()));
    EXPECT_EQ(v12, get_value<ignite_timestamp>(tp.get_next()));
    EXPECT_EQ(v13, get_value<std::string>(tp.get_next()));
    EXPECT_EQ(v14, get_value<uuid>(tp.get_next()));
    EXPECT_EQ(v15, tp.get_next());
    EXPECT_EQ(v16, get_decimal(tp.get_next(), v16.get_scale()));
    EXPECT_EQ(v17, get_value<bool>(tp.get_next()));
}

TEST(tuple, AllTypesExtended) {
    static constexpr tuple_num_t NUM_ELEMENTS = 19;

    static constexpr std::size_t COUNT = 1000;

    std::random_device rd;
    std::uniform_int_distribution<short> dist;

    for (std::size_t i = 0; i < COUNT; i++) {
        int sign = (i % 2) ? -1 : 1;
        std::string big_num;
        std::string big_dec;
        big_num.resize(i % 100 + 10);
        big_dec.resize(i % 100 + 10);
        std::generate(big_num.begin(), big_num.end(), [&]() { return '1' + (dist(rd) % 9); });
        std::generate(big_dec.begin(), big_dec.end(), [&]() { return '1' + (dist(rd) % 9); });
        big_dec.insert(big_dec.end() - (i % 8 + 1), '.');

        if (sign == -1) {
            big_num.insert(big_num.begin(), '-');
            big_dec.insert(big_dec.begin(), '-');
        }

        std::string buffer;
        buffer.resize(i % 100 + 1);
        std::generate(buffer.begin(), buffer.end(), [&]() { return char(dist(rd) % 128); });

        bool v1 = (i % 2 == 0);
        int8_t v2 = int8_t(i % std::numeric_limits<int8_t>::max()) * sign;
        int16_t v3 = int16_t(i * i % std::numeric_limits<int16_t>::max()) * sign;
        int32_t v4 = int32_t(i * i * i % std::numeric_limits<int32_t>::max()) * sign;
        int64_t v5 = int64_t(i * i * i * i) * sign;
        float v6 = float(i) * float(i) * M_PI * sign;
        double v7 = double(i) * double(i) * M_PI * sign;
        big_decimal v8(big_dec);
        ignite_date v9(i % 10000, i % 12 + 1, i % 28 + 1);
        ignite_time v10(i % 24, i % 60, i % 60, i % 1000000000);
        ignite_date_time v11(v9, v10);
        ignite_timestamp v12(i, i % 1000000000);
        uuid v13(i, i << 1);
        bytes_view v14(reinterpret_cast<std::byte *>(buffer.data()), buffer.size());
        std::string v15(buffer.data(), buffer.size());
        ignite_period v16(i, i, i);
        ignite_duration v17(i, i % 1000000000);
        big_integer v18(big_num);

        binary_tuple_builder tb(NUM_ELEMENTS);
        tb.start();

        tb.claim_null();
        tb.claim_bool(v1);
        tb.claim_int8(v2);
        tb.claim_int16(v3);
        tb.claim_int32(v4);
        tb.claim_int64(v5);
        tb.claim_float(v6);
        tb.claim_double(v7);
        tb.claim_number(v8);
        tb.claim_date(v9);
        tb.claim_time(v10);
        tb.claim_date_time(v11);
        tb.claim_timestamp(v12);
        tb.claim_uuid(v13);
        tb.claim_varlen(v14);
        tb.claim_varlen(v15);
        tb.claim_period(v16);
        tb.claim_duration(v17);
        tb.claim_number(v18);

        tb.layout();

        tb.append_null();
        tb.append_bool(v1);
        tb.append_int8(v2);
        tb.append_int16(v3);
        tb.append_int32(v4);
        tb.append_int64(v5);
        tb.append_float(v6);
        tb.append_double(v7);
        tb.append_number(v8);
        tb.append_date(v9);
        tb.append_time(v10);
        tb.append_date_time(v11);
        tb.append_timestamp(v12);
        tb.append_uuid(v13);
        tb.append_varlen(v14);
        tb.append_varlen(v15);
        tb.append_period(v16);
        tb.append_duration(v17);
        tb.append_number(v18);

        const std::vector<std::byte> &binary_tuple = tb.build();
        binary_tuple_parser tp(NUM_ELEMENTS, binary_tuple);

        EXPECT_EQ(bytes_view{}, tp.get_next());
        EXPECT_EQ(v1, get_value<bool>(tp.get_next()));
        EXPECT_EQ(v2, get_value<int8_t>(tp.get_next()));
        EXPECT_EQ(v3, get_value<int16_t>(tp.get_next()));
        EXPECT_EQ(v4, get_value<int32_t>(tp.get_next()));
        EXPECT_EQ(v5, get_value<int64_t>(tp.get_next()));
        EXPECT_EQ(v6, get_value<float>(tp.get_next()));
        EXPECT_EQ(v7, get_value<double>(tp.get_next()));
        EXPECT_EQ(v8, get_value<big_decimal>(tp.get_next()));
        EXPECT_EQ(v9, get_value<ignite_date>(tp.get_next()));
        EXPECT_EQ(v10, get_value<ignite_time>(tp.get_next()));
        EXPECT_EQ(v11, get_value<ignite_date_time>(tp.get_next()));
        EXPECT_EQ(v12, get_value<ignite_timestamp>(tp.get_next()));
        EXPECT_EQ(v13, get_value<uuid>(tp.get_next()));
        EXPECT_EQ(v14, tp.get_next());
        EXPECT_EQ(v15, get_value<std::string>(tp.get_next()));
        EXPECT_EQ(v16, get_value<ignite_period>(tp.get_next()));
        EXPECT_EQ(v17, get_value<ignite_duration>(tp.get_next()));
        EXPECT_EQ(v18, get_value<big_integer>(tp.get_next()));
    }
}

TEST(tuple, TwoFixedValues) { // NOLINT(cert-err58-cpp)
    {
        auto values = std::make_tuple<int32_t, int32_t>(33, -71);

        int8_t binary[] = {0, 1, 2, 33, -71};

        check_reader_writer_equality(values, binary);
    }

    {
        auto values = std::make_tuple(int32_t{77}, std::nullopt);

        int8_t binary[] = {0, 1, 1, 77};

        check_reader_writer_equality(values, binary);
    }
}

TEST(tuple, FixedAndVarlenValue) { // NOLINT(cert-err58-cpp)
    {
        auto values = std::make_tuple(int16_t{-33}, std::string{"val"});

        int8_t binary[] = {0, 1, 4, -33, 118, 97, 108};

        check_reader_writer_equality(values, binary);
    }

    {
        auto values = std::make_tuple(int16_t{33}, std::nullopt);

        int8_t binary[] = {0, 1, 1, 33};

        check_reader_writer_equality(values, binary);
    }
}

TEST(tuple, TwoVarlenValues) { // NOLINT(cert-err58-cpp)
    // With key and value.
    {
        auto values = std::make_tuple("key"s, "val"s);

        int8_t binary[] = {0, 3, 6, 107, 101, 121, 118, 97, 108};

        check_reader_writer_equality(values, binary);
    }

    // Null key.
    {
        auto values = std::make_tuple(std::nullopt, "val"s);

        int8_t binary[] = {0, 0, 3, 118, 97, 108};

        check_reader_writer_equality(values, binary);
    }

    // Null value.
    {
        auto values = std::make_tuple("key"s, std::nullopt);

        int8_t binary[] = {0, 3, 3, 107, 101, 121};

        check_reader_writer_equality(values, binary);
    }

    // Null both.
    {
        auto values = std::make_tuple(std::nullopt, std::nullopt);

        int8_t binary[] = {0, 0, 0};

        check_reader_writer_equality(values, binary);
    }
}

TEST(tuple, VarlenEmptyEscape) {
    // Empty value.
    {
        auto values = std::make_tuple("key"s, ""s);

        int8_t binary[] = {0, 3, 4, 107, 101, 121, -128};

        check_reader_writer_equality(values, binary);
    }

    // Normal non-empty value.
    {
        auto values = std::make_tuple("key"s, "\xff"s);

        int8_t binary[] = {0, 3, 4, 107, 101, 121, -1};

        check_reader_writer_equality(values, binary);
    }

    // Clashing non-empty value.
    {
        auto values = std::make_tuple("key"s, "\x80"s);

        int8_t binary[] = {0, 3, 5, 107, 101, 121, -128, -128};

        check_reader_writer_equality(values, binary);
    }

    // Another clashing non-empty value.
    {
        auto values = std::make_tuple("key"s, "\x80\xff"s);

        int8_t binary[] = {0, 3, 6, 107, 101, 121, -128, -128, -1};

        check_reader_writer_equality(values, binary);
    }
}

TEST(tuple, FourMixedValues) { // NOLINT(cert-err58-cpp)
    // With value.
    {
        auto values = std::make_tuple(int16_t{33}, std::string{"keystr"}, int32_t{73}, std::string{"valstr"});

        int8_t binary[] = {0, 1, 7, 8, 14, 33, 107, 101, 121, 115, 116, 114, 73, 118, 97, 108, 115, 116, 114};

        check_reader_writer_equality(values, binary);
    }

    // Null value.
    {
        auto values = std::make_tuple(int16_t{33}, std::string{"keystr2"}, std::nullopt, std::nullopt);

        int8_t binary[] = {0, 1, 8, 8, 8, 33, 107, 101, 121, 115, 116, 114, 50};

        check_reader_writer_equality(values, binary);
    }
}

TEST(tuple, FixedNullableColumns) { // NOLINT(cert-err58-cpp)
    // Last column null.
    {
        auto values = std::make_tuple(int8_t{11}, int16_t{22}, std::nullopt, int8_t{-44}, int16_t{-66}, std::nullopt);

        int8_t binary[] = {0, 1, 2, 2, 3, 4, 4, 11, 22, -44, -66};

        check_reader_writer_equality(values, binary);
    }

    // First column null.
    {
        auto values = std::make_tuple(std::nullopt, int16_t{22}, int32_t{33}, std::nullopt, int16_t{-55}, int32_t{-66});

        int8_t binary[] = {0, 0, 1, 2, 2, 3, 4, 22, 33, -55, -66};

        check_reader_writer_equality(values, binary);
    }

    // Middle column null.
    {
        auto values = std::make_tuple(int8_t{11}, std::nullopt, int32_t{33}, int8_t{-44}, std::nullopt, int32_t{-66});

        int8_t binary[] = {0, 1, 1, 2, 3, 3, 4, 11, 33, -44, -66};

        check_reader_writer_equality(values, binary);
    }

    // All columns null.
    {
        auto values =
            std::make_tuple(std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt);

        int8_t binary[] = {0, 0, 0, 0, 0, 0, 0};

        check_reader_writer_equality(values, binary);
    }
}

TEST(tuple, VarlenNullableColumns) { // NOLINT(cert-err58-cpp)
    // Last column null.
    {
        auto values = std::make_tuple("abc"s, "ascii"s, std::nullopt, "yz"s, "我愛Java"s, std::nullopt);

        int8_t binary[] = {0, 3, 8, 8, 10, 20, 20, 97, 98, 99, 97, 115, 99, 105, 105, 121, 122, -26, -120, -111, -26,
            -124, -101, 74, 97, 118, 97};

        check_reader_writer_equality(values, binary);
    }

    // First column null.
    {
        auto values = std::make_tuple(std::nullopt, "ascii"s, "我愛Java"s, std::nullopt, "我愛Java"s, "ascii"s);

        int8_t binary[] = {0, 0, 5, 15, 15, 25, 30, 97, 115, 99, 105, 105, -26, -120, -111, -26, -124, -101, 74, 97,
            118, 97, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97, 97, 115, 99, 105, 105};

        check_reader_writer_equality(values, binary);
    }

    // Middle column null.
    {
        auto values = std::make_tuple("abc"s, std::nullopt, "我愛Java"s, "yz"s, std::nullopt, "ascii"s);

        int8_t binary[] = {0, 3, 3, 13, 15, 15, 20, 97, 98, 99, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97, 121,
            122, 97, 115, 99, 105, 105};

        check_reader_writer_equality(values, binary);
    }

    // All columns null.
    {
        auto values =
            std::make_tuple(std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt);

        int8_t binary[] = {0, 0, 0, 0, 0, 0, 0};

        check_reader_writer_equality(values, binary);
    }
}

TEST(tuple, FixedAndVarlenNullableColumns) { // NOLINT(cert-err58-cpp)
    // Check null/non-null all fixed/varlen.
    {
        auto values = std::make_tuple(
            int8_t{11}, int16_t{22}, std::nullopt, std::nullopt, std::nullopt, std::nullopt, "yz"s, "ascii"s);

        int8_t binary[] = {0, 1, 2, 2, 2, 2, 2, 4, 9, 11, 22, 121, 122, 97, 115, 99, 105, 105};

        check_reader_writer_equality(values, binary);
    }

    // Check null/non-null single fixed.
    {
        auto values =
            std::make_tuple(std::nullopt, int16_t{22}, "ab"s, "我愛Java"s, int8_t{55}, std::nullopt, "yz"s, "ascii"s);

        int8_t binary[] = {0, 0, 1, 3, 13, 14, 14, 16, 21, 22, 97, 98, -26, -120, -111, -26, -124, -101, 74, 97, 118,
            97, 55, 121, 122, 97, 115, 99, 105, 105};

        check_reader_writer_equality(values, binary);
    }

    // Check null/non-null single varlen.
    {
        auto values = std::make_tuple(
            int8_t{11}, int16_t{22}, std::nullopt, "我愛Java"s, int8_t{55}, int16_t{66}, "yz"s, std::nullopt);

        int8_t binary[] = {0, 1, 2, 2, 12, 13, 14, 16, 16, 11, 22, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97,
            55, 66, 121, 122};

        check_reader_writer_equality(values, binary);
    }

    // Check null/non-null mixed.
    {
        auto values = std::make_tuple(
            int8_t{11}, std::nullopt, std::nullopt, "我愛Java"s, std::nullopt, int16_t{22}, "yz"s, std::nullopt);

        int8_t binary[] = {
            0, 1, 1, 1, 11, 11, 12, 14, 14, 11, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97, 22, 121, 122};

        check_reader_writer_equality(values, binary);
    }

    // Check all null/non-null.
    {
        auto values = std::make_tuple(
            int8_t{11}, int16_t{22}, "ab"s, "我愛Java"s, std::nullopt, std::nullopt, std::nullopt, std::nullopt);

        int8_t binary[] = {
            0, 1, 2, 4, 14, 14, 14, 14, 14, 11, 22, 97, 98, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97};

        check_reader_writer_equality(values, binary);
    }
}

TEST(tuple, ZeroLengthVarlen) { // NOLINT(cert-err58-cpp)
    // Single zero-length vector of bytes.
    {
        auto values = std::make_tuple(int32_t{0}, ""s);

        int8_t binary[] = {0, 1, 2, 0, -128};

        check_reader_writer_equality(values, binary);
    }

    // Two zero-length vectors of bytes.
    {
        auto values = std::make_tuple(int32_t{0}, ""s, ""s);

        int8_t binary[] = {0, 1, 2, 3, 0, -128, -128};

        check_reader_writer_equality(values, binary);
    }

    // Two zero-length vectors of bytes and single integer value.
    {
        auto values = std::make_tuple(int32_t{0}, int32_t{123}, ""s, ""s);

        int8_t binary[] = {0, 1, 2, 3, 4, 0, 123, -128, -128};

        check_reader_writer_equality(values, binary);
    }
}

TEST(tuple, SingleVarlen) { // NOLINT(cert-err58-cpp)
    auto values = std::make_tuple(int32_t{0}, "\1\2\3"s);

    int8_t binary[] = {0, 1, 4, 0, 1, 2, 3};

    check_reader_writer_equality(values, binary);
}

TEST(tuple, TinyVarlenFormatOverflowLarge) { // NOLINT(cert-err58-cpp)
    static constexpr tuple_num_t NUM_ELEMENTS = 301;

    // Flags - 1 zero byte;
    // Varlen table - 1 byte for key column + 300 bytes for value columns;
    // Key - 1 zero byte.
    std::vector<std::byte> reference(303);
    std::fill(reference.begin() + 1, reference.end() - 1, std::byte{1});

    binary_tuple_parser tp(NUM_ELEMENTS, {reinterpret_cast<std::byte *>(reference.data()), reference.size()});

    auto first = tp.get_next();
    EXPECT_EQ(1, first.size());
    EXPECT_EQ(0, get_value<int8_t>(first));

    for (int i = 1; i < NUM_ELEMENTS; i++) {
        auto next = tp.get_next();
        EXPECT_TRUE(next.empty());
    }
}

TEST(tuple, TinyVarlenFormatOverflowMedium) { // NOLINT(cert-err58-cpp)
    static constexpr tuple_num_t NUM_ELEMENTS = 301;

    // Flags - 1 zero byte;
    // Varlen table - 301 bytes;
    // Key - 1 zero byte;
    // Varlen values - 3 bytes.
    std::vector<int8_t> reference(306);
    reference[1] = 1;
    reference[2] = 2;
    reference[3] = 3;
    reference[4] = 4;

    // Offsets for zero-length arrays.
    for (int i = 0; i < 300 - 3; i++) {
        reference[5 + i] = 4;
    }

    // Non-empty arrays:
    reference[303] = 1;
    reference[304] = 2;
    reference[305] = 3;

    binary_tuple_parser tp(NUM_ELEMENTS, {reinterpret_cast<std::byte *>(reference.data()), reference.size()});

    auto first = tp.get_next();
    EXPECT_EQ(1, first.size());
    EXPECT_EQ(0, get_value<int8_t>(first));

    for (int i = 1; i <= 3; i++) {
        auto next = tp.get_next();
        EXPECT_EQ(1, next.size());
        EXPECT_EQ(i, get_value<int8_t>(next));
    }
    for (tuple_num_t i = 4; i < NUM_ELEMENTS; i++) {
        auto next = tp.get_next();
        EXPECT_EQ(0, next.size());
    }
}

// Check same binaries that used in testExpectedVarlenTupleBinaries java side test.
TEST(tuple, ExpectedVarlenTupleBinaries) { // NOLINT(cert-err58-cpp)
    static constexpr tuple_num_t NUM_ELEMENTS = 7;

    // clang-format off
    std::vector<int8_t> binary_tiny {
        // Flags.
        0,
        // Offsets for values.
        4, 8, 12, 16, 21, 24, 27,
        // Key - integer zero.
        0, 0, 0, 0,
        // Integer values - 1, 2, 3.
        1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
        // Byte[] value.
        1, 2, 3, 4, 5,
        // Byte[] value.
        1, 2, 3,
        // String "abc".
        97, 98, 99};
    // clang-format on

    tuple_view tiny = parser(NUM_ELEMENTS, binary_tiny).parse();

    EXPECT_EQ(4, tiny[0].size());
    EXPECT_EQ(0, get_value<int32_t>(tiny[0]));

    EXPECT_EQ(4, tiny[1].size());
    EXPECT_EQ(1, get_value<int32_t>(tiny[1]));

    EXPECT_EQ(4, tiny[2].size());
    EXPECT_EQ(2, get_value<int32_t>(tiny[2]));

    EXPECT_EQ(4, tiny[3].size());
    EXPECT_EQ(3, get_value<int32_t>(tiny[3]));

    bytes_view varlen = tiny[4];
    EXPECT_EQ(5, varlen.size());
    EXPECT_EQ(1, std::to_integer<int>(varlen[0]));
    EXPECT_EQ(2, std::to_integer<int>(varlen[1]));
    EXPECT_EQ(3, std::to_integer<int>(varlen[2]));
    EXPECT_EQ(4, std::to_integer<int>(varlen[3]));
    EXPECT_EQ(5, std::to_integer<int>(varlen[4]));

    varlen = tiny[5];
    EXPECT_EQ(3, varlen.size());
    EXPECT_EQ(1, std::to_integer<int>(varlen[0]));
    EXPECT_EQ(2, std::to_integer<int>(varlen[1]));
    EXPECT_EQ(3, std::to_integer<int>(varlen[2]));

    varlen = tiny[6];
    EXPECT_EQ(3, varlen.size());
    std::string str(varlen);
    EXPECT_EQ("abc", str);

    // Make data that doesn't fit to tiny format tuple but fits to medium format tuple.
    std::vector<int8_t> medium_value(256 * 2);
    for (size_t i = 0; i < medium_value.size(); i++) {
        medium_value[i] = (int8_t) ((i * 127) % 256);
    }

    // Construct reference tuple binary independently of tuple assembler.
    // clang-format off
    std::vector<int8_t> binary_medium {
        // Flags.
        1,
        // Offsets for values.
        4, 0, 8, 0, 12, 0, 16, 0, 21, 0,
        int8_t(21 + medium_value.size()), int8_t((21 + medium_value.size()) >> 8),
        int8_t(24 + medium_value.size()), int8_t((24 + medium_value.size()) >> 8),
        // Key - integer zero.
        0, 0, 0, 0,
        // Integer values - 1, 2, 3.
        1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
        // Byte[] value.
        1, 2, 3, 4, 5,
        // Byte[] value -- skipped for now, inserted below.
        // String "abc".
        97, 98, 99};
    // clang-format on

    // Copy varlen array that does not fit to tiny format.
    binary_medium.insert(binary_medium.end() - 3, medium_value.begin(), medium_value.end());

    tuple_view medium = parser(NUM_ELEMENTS, binary_medium).parse();

    EXPECT_EQ(4, medium[0].size());
    EXPECT_EQ(0, get_value<int32_t>(medium[0]));

    EXPECT_EQ(4, medium[1].size());
    EXPECT_EQ(1, get_value<int32_t>(medium[1]));

    EXPECT_EQ(4, medium[2].size());
    EXPECT_EQ(2, get_value<int32_t>(medium[2]));

    EXPECT_EQ(4, medium[3].size());
    EXPECT_EQ(3, get_value<int32_t>(medium[3]));

    varlen = medium[4];
    EXPECT_EQ(5, varlen.size());
    EXPECT_EQ(1, std::to_integer<int>(varlen[0]));
    EXPECT_EQ(2, std::to_integer<int>(varlen[1]));
    EXPECT_EQ(3, std::to_integer<int>(varlen[2]));
    EXPECT_EQ(4, std::to_integer<int>(varlen[3]));
    EXPECT_EQ(5, std::to_integer<int>(varlen[4]));

    varlen = medium[5];
    EXPECT_TRUE(medium_value
        == std::vector<int8_t>(reinterpret_cast<const int8_t *>(varlen.data()),
            reinterpret_cast<const int8_t *>(varlen.data() + varlen.size())));

    varlen = medium[6];
    str = std::string(varlen);
    EXPECT_EQ("abc", str);

    // Make data that doesn't fit to medium format tuple but fits to large format tuple.
    std::vector<int8_t> large_value(64 * 2 * 1024);
    for (size_t i = 0; i < large_value.size(); i++) {
        large_value[i] = (int8_t) ((i * 127) % 256);
    }

    // Construct reference tuple binary independently of tuple assembler.
    // clang-format off
    std::vector<int8_t> binary_large {
        // Flags.
        2,
        // Offsets for values.
        4, 0, 0, 0, 8, 0, 0, 0, 12, 0, 0, 0, 16, 0, 0, 0, 21, 0, 0, 0,
        int8_t(21 + large_value.size()), int8_t((21 + large_value.size()) >> 8),
        int8_t((21 + large_value.size()) >> 16), int8_t((21 + large_value.size()) >> 24),
        int8_t(24 + large_value.size()), int8_t((24 + large_value.size()) >> 8),
        int8_t((24 + large_value.size()) >> 16), int8_t((24 + large_value.size()) >> 24),
        // Key - integer zero.
        0, 0, 0, 0,
        // Integer values - 1, 2, 3.
        1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0,
        // Byte[] value.
        1, 2, 3, 4, 5,
        // Byte[] value -- skipped for now, inserted below.
        // String "abc".
        97, 98, 99};
    // clang-format on

    // Copy varlen array that does not fit to tiny and medium format.
    binary_large.insert(binary_large.end() - 3, large_value.begin(), large_value.end());

    tuple_view large = parser(NUM_ELEMENTS, binary_large).parse();

    EXPECT_EQ(4, large[0].size());
    EXPECT_EQ(0, get_value<int32_t>(large[0]));

    EXPECT_EQ(4, large[1].size());
    EXPECT_EQ(1, get_value<int32_t>(large[1]));

    EXPECT_EQ(4, large[2].size());
    EXPECT_EQ(2, get_value<int32_t>(large[2]));

    EXPECT_EQ(4, large[3].size());
    EXPECT_EQ(3, get_value<int32_t>(large[3]));

    varlen = large[4];
    EXPECT_EQ(5, varlen.size());
    EXPECT_EQ(1, std::to_integer<int>(varlen[0]));
    EXPECT_EQ(2, std::to_integer<int>(varlen[1]));
    EXPECT_EQ(3, std::to_integer<int>(varlen[2]));
    EXPECT_EQ(4, std::to_integer<int>(varlen[3]));
    EXPECT_EQ(5, std::to_integer<int>(varlen[4]));

    varlen = large[5];
    EXPECT_TRUE(large_value
        == std::vector<int8_t>(reinterpret_cast<const int8_t *>(varlen.data()),
            reinterpret_cast<const int8_t *>(varlen.data() + varlen.size())));

    varlen = large[6];
    str = std::string(varlen);
    EXPECT_EQ("abc", str);
}

TEST(tuple, StringAfterNull) {
    static constexpr tuple_num_t NUM_ELEMENTS = 3;

    // 101, null, "Bob"
    std::vector<std::byte> tuple;
    for (int i : {0, 1, 1, 4, 101, 66, 111, 98}) {
        tuple.push_back(static_cast<std::byte>(i));
    }

    binary_tuple_parser tp(NUM_ELEMENTS, tuple);

    EXPECT_EQ(101, get_value<int32_t>(tp.get_next()));
    EXPECT_TRUE(tp.get_next().empty());
    EXPECT_EQ("Bob", get_value<std::string>(tp.get_next()));
}

TEST(tuple, SingleValueTupleAssembler) { // NOLINT(cert-err58-cpp)
    static constexpr tuple_num_t NUM_ELEMENTS = 1;

    builder ta(NUM_ELEMENTS);

    auto tuple = ta.build(std::tuple<int32_t>(123));

    binary_tuple_parser tp(NUM_ELEMENTS, tuple);

    auto slice = tp.get_next();
    ASSERT_FALSE(slice.empty());
    EXPECT_EQ(123, get_value<int32_t>(slice));
}

TEST(tuple, TupleWriteRead) { // NOLINT(cert-err58-cpp)
    static constexpr tuple_num_t NUM_ELEMENTS = 6;

    for (bool nullable : {false, true}) {
        binary_tuple_builder tb(NUM_ELEMENTS);

        tb.start();

        tb.claim_int32(1234);
        tb.claim_double(5.0);
        tb.claim_int8(6);
        tb.claim_int16(7);
        if (nullable) {
            tb.claim_null();
        } else {
            tb.claim_int64(8);
        }
        tb.claim_int8(9);

        tb.layout();

        tb.append_int32(1234);
        tb.append_double(5.0);
        tb.append_int8(6);
        tb.append_int16(7);
        if (nullable) {
            tb.append_null();
        } else {
            tb.append_int64(8);
        }
        tb.append_int8(9);

        auto &tuple = tb.build();

        tuple_view t = parser(NUM_ELEMENTS, tuple).parse();

        ASSERT_FALSE(t[0].empty());
        EXPECT_EQ(1234, get_value<int32_t>(t[0]));

        ASSERT_FALSE(t[1].empty());
        EXPECT_EQ(5.0, get_value<double>(t[1]));

        ASSERT_FALSE(t[2].empty());
        EXPECT_EQ(6, get_value<int8_t>(t[2]));

        ASSERT_FALSE(t[3].empty());
        EXPECT_EQ(7, get_value<int16_t>(t[3]));

        if (nullable) {
            EXPECT_TRUE(t[4].empty());
        } else {
            ASSERT_FALSE(t[4].empty());
            EXPECT_EQ(8, get_value<int64_t>(t[4]));
        }

        ASSERT_FALSE(t[5].empty());
        EXPECT_EQ(9, get_value<int8_t>(t[5]));
    }
}

TEST(tuple, Int8TupleWriteRead) { // NOLINT(cert-err58-cpp)
    static constexpr tuple_num_t NUM_ELEMENTS = 6;

    for (bool nullable : {false, true}) {
        binary_tuple_builder tb(NUM_ELEMENTS);

        tb.start();

        tb.claim_int8(0);
        tb.claim_int8(1);
        tb.claim_int8(2);
        tb.claim_int8(3);
        if (nullable) {
            tb.claim_null();
        } else {
            tb.claim_int8(4);
        }
        tb.claim_int8(5);

        tb.layout();

        tb.append_int8(0);
        tb.append_int8(1);
        tb.append_int8(2);
        tb.append_int8(3);
        if (nullable) {
            tb.append_null();
        } else {
            tb.append_int8(4);
        }
        tb.append_int8(5);

        auto &tuple = tb.build();

        tuple_view t = parser(NUM_ELEMENTS, tuple).parse();

        ASSERT_FALSE(t[0].empty());
        EXPECT_EQ(0, get_value<int8_t>(t[0]));

        ASSERT_FALSE(t[1].empty());
        EXPECT_EQ(1, get_value<int8_t>(t[1]));

        ASSERT_FALSE(t[2].empty());
        EXPECT_EQ(2, get_value<int8_t>(t[2]));

        ASSERT_FALSE(t[3].empty());
        EXPECT_EQ(3, get_value<int8_t>(t[3]));

        if (nullable) {
            EXPECT_TRUE(t[4].empty());
        } else {
            ASSERT_FALSE(t[4].empty());
            EXPECT_EQ(4, get_value<int8_t>(t[4]));
        }

        ASSERT_FALSE(t[5].empty());
        EXPECT_EQ(5, get_value<int8_t>(t[5]));
    }
}

TEST(tuple, VarlenLargeTest) { // NOLINT(cert-err58-cpp)
    static constexpr tuple_num_t NUM_ELEMENTS = 3;

    {
        auto values = std::make_tuple(std::string(100'000, 'a'), "b"s, std::nullopt);

        builder tuple_assembler(NUM_ELEMENTS);

        bytes_view tuple = tuple_assembler.build(values);

        binary_tuple_parser tp(NUM_ELEMENTS, tuple);

        EXPECT_EQ(std::get<0>(values), get_value<std::string>(tp.get_next()));
        EXPECT_EQ(std::get<1>(values), get_value<std::string>(tp.get_next()));
        EXPECT_TRUE(tp.get_next().empty());
    }
    {
        auto values = std::make_tuple(std::string(100'000, 'a'), "b"s, "c"s);

        builder tuple_assembler(NUM_ELEMENTS);

        bytes_view tuple = tuple_assembler.build(values);

        binary_tuple_parser tp(NUM_ELEMENTS, tuple);

        EXPECT_EQ(std::get<0>(values), get_value<std::string>(tp.get_next()));
        EXPECT_EQ(std::get<1>(values), get_value<std::string>(tp.get_next()));
        EXPECT_EQ(std::get<2>(values), get_value<std::string>(tp.get_next()));
    }
    {
        auto values = std::make_tuple(std::nullopt, std::string(100'000, 'a'), "b"s);

        builder tuple_assembler(NUM_ELEMENTS);

        bytes_view tuple = tuple_assembler.build(values);

        binary_tuple_parser tp(NUM_ELEMENTS, tuple);

        EXPECT_TRUE(tp.get_next().empty());
        EXPECT_EQ(std::get<1>(values), get_value<std::string>(tp.get_next()));
        EXPECT_EQ(std::get<2>(values), get_value<std::string>(tp.get_next()));
    }
}

TEST(tuple, VarlenMediumTest) { // NOLINT(cert-err58-cpp)
    static constexpr tuple_num_t NUM_ELEMENTS = 301;

    {
        binary_tuple_builder tb(NUM_ELEMENTS);
        tb.start();
        tb.claim_int32(100500);

        // Create a 10-char value.
        std::string value("0123456789");

        for (int i = 1; i < NUM_ELEMENTS; i++) {
            tb.claim_varlen(value);
        }

        tb.layout();

        tb.append_int32(100500);

        for (int i = 1; i < NUM_ELEMENTS; i++) {
            tb.append_varlen(value);
        }

        bytes_view tuple = tb.build();

        // The varlen area will take (10 * 300) = 3000 bytes. So the varlen table entry will take 2 bytes.
        // The flags field must be equal to log2(2) = 1.
        EXPECT_EQ(std::byte{1}, tuple[0]);

        binary_tuple_parser tp(NUM_ELEMENTS, tuple);

        EXPECT_EQ(100500, get_value<int32_t>(tp.get_next()));
        for (tuple_num_t i = 1; i < NUM_ELEMENTS; i++) {
            EXPECT_EQ(value, get_value<std::string>(tp.get_next()));
        }
    }

    {
        binary_tuple_builder tb(NUM_ELEMENTS);
        tb.start();
        tb.claim_int32(100500);

        // Create a 300-char value.
        std::string value;
        for (int i = 0; i < 30; i++) {
            value += "0123456789";
        }

        for (int i = 1; i < NUM_ELEMENTS; i++) {
            tb.claim_varlen(value);
        }

        tb.layout();

        tb.append_int32(100500);

        for (int i = 1; i < NUM_ELEMENTS; i++) {
            tb.append_varlen(value);
        }

        bytes_view tuple = tb.build();

        // The varlen area will take (300 * 300) = 90000 bytes. The size value does not fit to
        // one or two bytes. So the varlen table entry will take 4 bytes. The flags field must
        // be equal to log2(4) = 2.
        EXPECT_EQ(std::byte{2}, tuple[0]);

        binary_tuple_parser tp(NUM_ELEMENTS, tuple);

        EXPECT_EQ(100500, get_value<int32_t>(tp.get_next()));
        for (tuple_num_t i = 1; i < NUM_ELEMENTS; i++) {
            EXPECT_EQ(value, get_value<std::string>(tp.get_next()));
        }
    }
}

class tuple_big_decimal_zeros : public testing::TestWithParam<big_decimal> {};

TEST_P(tuple_big_decimal_zeros, DecimalZerosTest) { // NOLINT(cert-err58-cpp)
    int8_t binary[] = {0, 3, 0, 0, 0};

    check_reader_writer_equality(std::make_tuple(GetParam()), binary);
}

INSTANTIATE_TEST_SUITE_P(zeros, tuple_big_decimal_zeros,
    testing::Values(big_decimal("0"), big_decimal("-0"), big_decimal("0E1000"), big_decimal("0E-1000"), big_decimal(0L),
        big_decimal(0, std::int16_t(10))));

class tuple_big_decimal : public testing::TestWithParam<big_decimal> {};

TEST_P(tuple_big_decimal, WriteReadEqualityTest) { // NOLINT(cert-err58-cpp)
    check_reader_writer_equality(GetParam());
}

INSTANTIATE_TEST_SUITE_P(positive_ints, tuple_big_decimal,
    testing::Values(big_decimal(1L), big_decimal(9L), big_decimal(10L), big_decimal(11L), big_decimal(19L),
        big_decimal(123L), big_decimal(1234L), big_decimal(12345L), big_decimal(123456L), big_decimal(1234567L),
        big_decimal(12345678L), big_decimal(123456789L), big_decimal(1234567890L), big_decimal(12345678909),
        big_decimal(123456789098), big_decimal(1234567890987), big_decimal(12345678909876),
        big_decimal(123456789098765), big_decimal(1234567890987654), big_decimal(12345678909876543),
        big_decimal(123456789098765432), big_decimal(1234567890987654321), big_decimal(999999999999999999L),
        big_decimal(999999999099999999L), big_decimal(1000000000000000000L), big_decimal(1000000000000000001L),
        big_decimal(1000000005000000000L), big_decimal(INT64_MAX)));

INSTANTIATE_TEST_SUITE_P(negative_ints, tuple_big_decimal,
    testing::Values(big_decimal(-1L), big_decimal(-9L), big_decimal(-10L), big_decimal(-11L), big_decimal(-19L),
        big_decimal(-123L), big_decimal(-1234L), big_decimal(-12345L), big_decimal(-123456L), big_decimal(-1234567L),
        big_decimal(-12345678L), big_decimal(-123456789L), big_decimal(-1234567890L), big_decimal(-12345678909),
        big_decimal(-123456789098), big_decimal(-1234567890987), big_decimal(-12345678909876),
        big_decimal(-123456789098765), big_decimal(-1234567890987654), big_decimal(-12345678909876543),
        big_decimal(-123456789098765432), big_decimal(-1234567890987654321), big_decimal(-999999999999999999L),
        big_decimal(-999999999099999999L), big_decimal(-1000000000000000000L), big_decimal(-1000000000000000001L),
        big_decimal(-1000000005000000000L), big_decimal(INT64_MIN)));

INSTANTIATE_TEST_SUITE_P(strings, tuple_big_decimal,
    testing::Values(big_decimal("1"), big_decimal("2"), big_decimal("9"), big_decimal("10"), big_decimal("1123"),
        big_decimal("64539472569345602304"), big_decimal("2376926357280573482539570263854"),
        big_decimal("4078460509739485762306457364875609364258763498578235876432589345693645872686453947256"
                    "93456023046037490024067294087609279"),
        big_decimal("78386624911263451156418774267"), big_decimal("-65281"), big_decimal("-4278190081"),
        big_decimal("-78918762577726953281363576967"), big_decimal("-43724369233562317116122583882241974163428"),
        big_decimal("-123456654321"), big_decimal("5266375549962187335354865963678516735728443929239383984473"),

        big_decimal("1000000000000"), big_decimal("1000000000000000000000000000"),
        big_decimal("100000000000000000000000000000000000000000000000000000000000"),

        big_decimal("99999999999999"), big_decimal("99999999999999999999999999999999"),
        big_decimal("9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999"),

        big_decimal("-1"), big_decimal("-2"), big_decimal("-9"), big_decimal("-10"), big_decimal("-1123"),
        big_decimal("-64539472569345602304"), big_decimal("-2376926357280573482539570263854"),
        big_decimal("-407846050973948576230645736487560936425876349857823587643258934569364587268645394725"
                    "693456023046037490024067294087609279"),
        big_decimal("-1391368497633429418536934798133412971345891865982"),
        big_decimal("-78918762577726953281363576967"),

        big_decimal("-1000000000000"), big_decimal("-1000000000000000000000000000"),
        big_decimal("-100000000000000000000000000000000000000000000000000000000000"),

        big_decimal("-99999999999999"), big_decimal("-99999999999999999999999999999999"),
        big_decimal("-9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999"),

        big_decimal("0.1"), big_decimal("0.2"), big_decimal("0.3"), big_decimal("0.4"), big_decimal("0.5"),
        big_decimal("0.6"), big_decimal("0.7"), big_decimal("0.8"), big_decimal("0.9"), big_decimal("0.01"),
        big_decimal("0.001"), big_decimal("0.0001"), big_decimal("0.00001"), big_decimal("0.000001"),
        big_decimal("0.0000001"),

        big_decimal("0.00000000000000000000000000000000001"), big_decimal("0.10000000000000000000000000000000001"),
        big_decimal("0.10101010101010101010101010101010101"), big_decimal("0.99999999999999999999999999999999999"),
        big_decimal("0.79287502687354897253590684568634528762"),

        big_decimal("0.00000000000000000000000000000000000000000000000000000001"),
        big_decimal("0.10000000000000000000000000000000000000000000000000000001"),
        big_decimal("0.1111111111111111111111111111111111111111111111111111111111"),
        big_decimal("0.9999999999999999999999999999999999999999999999999999999999999999999"),
        big_decimal("0.436589746389567836745873648576289634589763845768268457683762864587684635892768346589629"),

        big_decimal("0."
                    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
                    "0000000000000000000000000000000000001"),
        big_decimal("0."
                    "1000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
                    "0000000000000000000000000000000000001"),
        big_decimal("0."
                    "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111"
                    "11111111111111111111111111111111111111111"),
        big_decimal("0."
                    "9999999999999999999999999999999999999999999999999999999999999999999999999999999999999"
                    "99999999999999999999999999999999999999999999999999"),
        big_decimal("0."
                    "4365897463895678367458736485762896345897638457682684576837628645876846358927683465493"
                    "85700256032605603246580726384075680247634627357023645889629"),

        big_decimal("-0.1"), big_decimal("-0.2"), big_decimal("-0.3"), big_decimal("-0.4"), big_decimal("-0.5"),
        big_decimal("-0.6"), big_decimal("-0.7"), big_decimal("-0.8"), big_decimal("-0.9"), big_decimal("-0.01"),
        big_decimal("-0.001"), big_decimal("-0.0001"), big_decimal("-0.00001"), big_decimal("-0.000001"),
        big_decimal("-0.0000001"),

        big_decimal("-0.00000000000000000000000000000000001"), big_decimal("-0.10000000000000000000000000000000001"),
        big_decimal("-0.10101010101010101010101010101010101"), big_decimal("-0.99999999999999999999999999999999999"),
        big_decimal("-0.79287502687354897253590684568634528762"),

        big_decimal("-0.00000000000000000000000000000000000000000000000000000001"),
        big_decimal("-0.10000000000000000000000000000000000000000000000000000001"),
        big_decimal("-0.1111111111111111111111111111111111111111111111111111111111"),
        big_decimal("-0.9999999999999999999999999999999999999999999999999999999999999999999"),
        big_decimal("-0.436589746389567836745873648576289634589763845768268457683762864587684635892768346589629"),

        big_decimal("-0."
                    "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
                    "0000000000000000000000000000000000001"),
        big_decimal("-0."
                    "1000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
                    "0000000000000000000000000000000000001"),
        big_decimal("-0."
                    "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111"
                    "11111111111111111111111111111111111111111"),
        big_decimal("-0."
                    "9999999999999999999999999999999999999999999999999999999999999999999999999999999999999"
                    "99999999999999999999999999999999999999999999999999"),
        big_decimal("-0."
                    "4365897463895678367458736485762896345897638457682684576837628645876846358927683465493"
                    "85700256032605603246580726384075680247634627357023645889629"),

        big_decimal("1.1"), big_decimal("12.21"), big_decimal("123.321"), big_decimal("1234.4321"),
        big_decimal("12345.54321"), big_decimal("123456.654321"), big_decimal("1234567.7654321"),
        big_decimal("12345678.87654321"), big_decimal("123456789.987654321"), big_decimal("1234567890.0987654321"),
        big_decimal("12345678909.90987654321"), big_decimal("123456789098.890987654321"),
        big_decimal("1234567890987.7890987654321"), big_decimal("12345678909876.67890987654321"),
        big_decimal("123456789098765.567890987654321"), big_decimal("1234567890987654.4567890987654321"),
        big_decimal("12345678909876543.34567890987654321"), big_decimal("123456789098765432.234567890987654321"),
        big_decimal("1234567890987654321.1234567890987654321"),
        big_decimal("12345678909876543210.01234567890987654321"),
        big_decimal("10000000000000000000000000000000000000000000000000000000000000."
                    "000000000000000000000000000000000000000000000000000000000000001"),
        big_decimal("111111111111111111111111111111111111111111111111111111111111111111111."
                    "11111111111111111111111111111111111111111111111111111111111111"),
        big_decimal("99999999999999999999999999999999999999999999999999999999999999999999."
                    "99999999999999999999999999999999999999999999999999999999999999999999"),
        big_decimal("458796987658934265896483756892638456782376482605002747502306790283640563."
                    "12017054126750641065780784583204650763485718064875683468568360506340563042567"),

        big_decimal("-1.1"), big_decimal("-12.21"), big_decimal("-123.321"), big_decimal("-1234.4321"),
        big_decimal("-12345.54321"), big_decimal("-123456.654321"), big_decimal("-1234567.7654321"),
        big_decimal("-12345678.87654321"), big_decimal("-123456789.987654321"), big_decimal("-1234567890.0987654321"),
        big_decimal("-12345678909.90987654321"), big_decimal("-123456789098.890987654321"),
        big_decimal("-1234567890987.7890987654321"), big_decimal("-12345678909876.67890987654321"),
        big_decimal("-123456789098765.567890987654321"), big_decimal("-1234567890987654.4567890987654321"),
        big_decimal("-12345678909876543.34567890987654321"), big_decimal("-123456789098765432.234567890987654321"),
        big_decimal("-1234567890987654321.1234567890987654321"),
        big_decimal("-12345678909876543210.01234567890987654321"),
        big_decimal("-10000000000000000000000000000000000000000000000000000000000000."
                    "000000000000000000000000000000000000000000000000000000000000001"),
        big_decimal("-111111111111111111111111111111111111111111111111111111111111111111111."
                    "11111111111111111111111111111111111111111111111111111111111111"),
        big_decimal("-99999999999999999999999999999999999999999999999999999999999999999999."
                    "99999999999999999999999999999999999999999999999999999999999999999999"),
        big_decimal("-458796987658934265896483756892638456782376482605002747502306790283640563."
                    "12017054126750641065780784583204650763485718064875683468568360506340563042567")));
