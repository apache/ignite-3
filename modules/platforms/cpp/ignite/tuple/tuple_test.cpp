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

#include "binary_tuple_builder.h"
#include "binary_tuple_parser.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <tuple>
#include <type_traits>
#include <vector>

using namespace std::string_literals;
using namespace ignite;

/** Binary value for a potentially nullable column. */
using value_view = std::optional<bytes_view>;

/** A set of binary values for a whole or partial row. */
using tuple_view = std::vector<value_view>;

template<typename T>
T get_value(value_view data) {
    T res;
    if constexpr (std::is_same<T, std::int8_t>::value) {
        res = binary_tuple_parser::get_int8(data.value());
    } else if constexpr (std::is_same<T, std::int16_t>::value) {
        res = binary_tuple_parser::get_int16(data.value());
    } else if constexpr (std::is_same<T, std::int32_t>::value) {
        res = binary_tuple_parser::get_int32(data.value());
    } else if constexpr (std::is_same<T, std::int64_t>::value) {
        res = binary_tuple_parser::get_int64(data.value());
    } else if constexpr (std::is_same<T, float>::value) {
        res = binary_tuple_parser::get_float(data.value());
    } else if constexpr (std::is_same<T, double>::value) {
        res = binary_tuple_parser::get_double(data.value());
    } else if constexpr (std::is_same<T, big_integer>::value) {
        res = binary_tuple_parser::get_number(data.value());
    } else if constexpr (std::is_same<T, ignite_date>::value) {
        res = binary_tuple_parser::get_date(data.value());
    } else if constexpr (std::is_same<T, ignite_time>::value) {
        res = binary_tuple_parser::get_time(data.value());
    } else if constexpr (std::is_same<T, ignite_date_time>::value) {
        res = binary_tuple_parser::get_date_time(data.value());
    } else if constexpr (std::is_same<T, ignite_timestamp>::value) {
        res = binary_tuple_parser::get_timestamp(data.value());
    } else if constexpr (std::is_same<T, uuid>::value) {
        res = binary_tuple_parser::get_uuid(data.value());
    } else if constexpr (std::is_same<T, std::string>::value) {
        res = {reinterpret_cast<const char *>(data->data()), data->size()};
    }
    return res;
}

big_decimal get_decimal(value_view data, int32_t scale) {
    return binary_tuple_parser::get_decimal(data.value(), scale);
}

struct builder : binary_tuple_builder {
    using binary_tuple_builder::binary_tuple_builder;

    template<typename T>
    void claim_value(const T &value) {
        if constexpr (std::is_same<T, std::nullopt_t>::value) {
            claim(value);
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
            claim_string(value);
        }
    }

    template<typename T>
    void append_value(const T &value) {
        if constexpr (std::is_same<T, std::nullopt_t>::value) {
            append(value);
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
            append_string(value);
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
        EXPECT_EQ(value, get_value<T>(parser.get_next()));
    }

    void operator()(std::nullopt_t /*null*/) { EXPECT_FALSE(parser.get_next()); }
};

template<typename... Ts, int size>
void check_reader_writer_equality(const std::tuple<Ts...> &values, const std::int8_t (&data)[size], bool skip_assembler_check = false) {
    constexpr tuple_num_t NUM_ELEMENTS = sizeof...(Ts);

    bytes_view bytes {reinterpret_cast<const std::byte *>(data), size};

    binary_tuple_parser tp(NUM_ELEMENTS, bytes);

    std::apply([&tp](const Ts... args) { ((single_value_check(tp)(args)), ...); }, values);

    EXPECT_EQ(tp.num_elements(), tp.num_parsed_elements());

    if (!skip_assembler_check) {
        builder ta(NUM_ELEMENTS);

        bytes_view built = ta.build(values);

        EXPECT_THAT(built, testing::ContainerEq(bytes));
    }
}

TEST(tuple, AllTypes) {
    static constexpr tuple_num_t NUM_ELEMENTS = 15;

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
    tb.claim_string(v13);
    tb.claim_uuid(v14);
    tb.claim_bytes(v15);

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
    tb.append_string(v13);
    tb.append_uuid(v14);
    tb.append_bytes(v15);

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
}

TEST(tuple, TwoFixedValues) { // NOLINT(cert-err58-cpp)
    {
        auto values = std::make_tuple<int32_t, int32_t>(33, -71);

        int8_t data[] = {0, 1, 2, 33, -71};

        check_reader_writer_equality(values, data);
    }
    {
        auto values = std::make_tuple(int32_t{77}, std::nullopt);

        int8_t data[] = {4, 2, 1, 1, 77};

        check_reader_writer_equality(values, data);
    }
}

TEST(tuple, FixedAndVarlenValue) { // NOLINT(cert-err58-cpp)
    {
        auto values = std::make_tuple(int16_t{-33}, std::string{"val"});

        int8_t data[] = {0, 1, 4, -33, 118, 97, 108};

        check_reader_writer_equality(values, data);
    }
    {
        auto values = std::make_tuple(int16_t{33}, std::nullopt);

        int8_t data[] = {4, 2, 1, 1, 33};

        check_reader_writer_equality(values, data);
    }
}

TEST(tuple, TwoVarlenValues) { // NOLINT(cert-err58-cpp)
    // With key and value.
    {
        auto values = std::make_tuple(std::string{"key"}, std::string{"val"});

        int8_t data[] = {0, 3, 6, 107, 101, 121, 118, 97, 108};

        check_reader_writer_equality(values, data);
    }

    // Null key.
    {
        auto values = std::make_tuple(std::nullopt, std::string{"val"});

        int8_t data[] = {4, 1, 0, 3, 118, 97, 108};

        check_reader_writer_equality(values, data);
    }

    // Null value.
    {
        auto values = std::make_tuple(std::string{"key"}, std::nullopt);

        int8_t data[] = {4, 2, 3, 3, 107, 101, 121};

        check_reader_writer_equality(values, data);
    }

    // Null both.
    {
        auto values = std::make_tuple(std::nullopt, std::nullopt);

        int8_t data[] = {4, 3, 0, 0};

        check_reader_writer_equality(values, data);
    }
}

TEST(tuple, FourMixedValues) { // NOLINT(cert-err58-cpp)
    // With value.
    {
        auto values = std::make_tuple(int16_t{33}, std::string{"keystr"}, int32_t{73}, std::string{"valstr"});

        int8_t data[] = {0, 1, 7, 8, 14, 33, 107, 101, 121, 115, 116, 114, 73, 118, 97, 108, 115, 116, 114};

        check_reader_writer_equality(values, data);
    }

    // Null value.
    {
        auto values = std::make_tuple(int16_t{33}, std::string{"keystr2"}, std::nullopt, std::nullopt);

        int8_t data[] = {4, 12, 1, 8, 8, 8, 33, 107, 101, 121, 115, 116, 114, 50};

        check_reader_writer_equality(values, data);
    }
}

TEST(tuple, FixedNullableColumns) { // NOLINT(cert-err58-cpp)
    // Last column null.
    {
        auto values = std::make_tuple(int8_t{11}, int16_t{22}, std::nullopt, int8_t{-44}, int16_t{-66}, std::nullopt);

        int8_t data[] = {4, 36, 1, 2, 2, 3, 4, 4, 11, 22, -44, -66};

        check_reader_writer_equality(values, data);
    }

    // First column null.
    {
        auto values = std::make_tuple(std::nullopt, int16_t{22}, int32_t{33}, std::nullopt, int16_t{-55}, int32_t{-66});

        int8_t data[] = {4, 9, 0, 1, 2, 2, 3, 4, 22, 33, -55, -66};

        check_reader_writer_equality(values, data);
    }

    // Middle column null.
    {
        auto values = std::make_tuple(int8_t{11}, std::nullopt, int32_t{33}, int8_t{-44}, std::nullopt, int32_t{-66});

        int8_t data[] = {4, 18, 1, 1, 2, 3, 3, 4, 11, 33, -44, -66};

        check_reader_writer_equality(values, data);
    }

    // All columns null.
    {
        auto values =
            std::make_tuple(std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt);

        int8_t data[] = {4, 63, 0, 0, 0, 0, 0, 0};

        check_reader_writer_equality(values, data);
    }
}

TEST(tuple, VarlenNullableColumns) { // NOLINT(cert-err58-cpp)
    // Last column null.
    {
        auto values = std::make_tuple("abc"s, "ascii"s, std::nullopt, "yz"s, "我愛Java"s, std::nullopt);

        int8_t data[] = {4, 36, 3, 8, 8, 10, 20, 20, 97, 98, 99, 97, 115, 99, 105, 105, 121, 122, -26, -120, -111, -26,
            -124, -101, 74, 97, 118, 97};

        check_reader_writer_equality(values, data);
    }

    // First column null.
    {
        auto values = std::make_tuple(std::nullopt, "ascii"s, "我愛Java"s, std::nullopt, "我愛Java"s, "ascii"s);

        int8_t data[] = {4, 9, 0, 5, 15, 15, 25, 30, 97, 115, 99, 105, 105, -26, -120, -111, -26, -124, -101, 74, 97,
            118, 97, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97, 97, 115, 99, 105, 105};

        check_reader_writer_equality(values, data);
    }

    // Middle column null.
    {
        auto values = std::make_tuple("abc"s, std::nullopt, "我愛Java"s, "yz"s, std::nullopt, "ascii"s);

        int8_t data[] = {4, 18, 3, 3, 13, 15, 15, 20, 97, 98, 99, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97,
            121, 122, 97, 115, 99, 105, 105};

        check_reader_writer_equality(values, data);
    }

    // All columns null.
    {
        auto values =
            std::make_tuple(std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt);

        int8_t data[] = {4, 63, 0, 0, 0, 0, 0, 0};

        check_reader_writer_equality(values, data);
    }
}

TEST(tuple, FixedAndVarlenNullableColumns) { // NOLINT(cert-err58-cpp)
    // Check null/non-null all fixed/varlen.
    {
        auto values = std::make_tuple(
            int8_t{11}, int16_t{22}, std::nullopt, std::nullopt, std::nullopt, std::nullopt, "yz"s, "ascii"s);

        int8_t data[] = {4, 60, 1, 2, 2, 2, 2, 2, 4, 9, 11, 22, 121, 122, 97, 115, 99, 105, 105};

        check_reader_writer_equality(values, data);
    }

    // Check null/non-null single fixed.
    {
        auto values =
            std::make_tuple(std::nullopt, int16_t{22}, "ab"s, "我愛Java"s, int8_t{55}, std::nullopt, "yz"s, "ascii"s);

        int8_t data[] = {4, 33, 0, 1, 3, 13, 14, 14, 16, 21, 22, 97, 98, -26, -120, -111, -26, -124, -101, 74, 97, 118,
            97, 55, 121, 122, 97, 115, 99, 105, 105};

        check_reader_writer_equality(values, data);
    }

    // Check null/non-null single varlen.
    {
        auto values = std::make_tuple(
            int8_t{11}, int16_t{22}, std::nullopt, "我愛Java"s, int8_t{55}, int16_t{66}, "yz"s, std::nullopt);

        int8_t data[] = {4, -124, 1, 2, 2, 12, 13, 14, 16, 16, 11, 22, -26, -120, -111, -26, -124, -101, 74, 97, 118,
            97, 55, 66, 121, 122};

        check_reader_writer_equality(values, data);
    }

    // Check null/non-null mixed.
    {
        auto values = std::make_tuple(
            int8_t{11}, std::nullopt, std::nullopt, "我愛Java"s, std::nullopt, int16_t{22}, "yz"s, std::nullopt);

        int8_t data[] = {
            4, -106, 1, 1, 1, 11, 11, 12, 14, 14, 11, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97, 22, 121, 122};

        check_reader_writer_equality(values, data);
    }

    // Check all null/non-null.
    {
        auto values = std::make_tuple(
            int8_t{11}, int16_t{22}, "ab"s, "我愛Java"s, std::nullopt, std::nullopt, std::nullopt, std::nullopt);

        int8_t data[] = {
            4, -16, 1, 2, 4, 14, 14, 14, 14, 14, 11, 22, 97, 98, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97};

        check_reader_writer_equality(values, data);
    }
}

TEST(tuple, ZeroLengthVarlen) { // NOLINT(cert-err58-cpp)
    // Single zero-length vector of bytes.
    {
        auto values = std::make_tuple(int32_t{0}, ""s);

        int8_t data[] = {0, 0, 0};

        check_reader_writer_equality(values, data);
    }

    // Two zero-length vectors of bytes.
    {
        auto values = std::make_tuple(int32_t{0}, ""s, ""s);

        int8_t data[] = {0, 0, 0, 0};

        check_reader_writer_equality(values, data);
    }

    // Two zero-length vectors of bytes and single integer value.
    {
        auto values = std::make_tuple(int32_t{0}, int32_t{123}, ""s, ""s);

        int8_t data[] = {0, 0, 1, 1, 1, 123};

        check_reader_writer_equality(values, data);
    }
}

TEST(tuple, SingleVarlen) { // NOLINT(cert-err58-cpp)
    auto values = std::make_tuple(int32_t{0}, "\1\2\3"s);

    int8_t data[] = {0, 0, 3, 1, 2, 3};

    check_reader_writer_equality(values, data);
}

TEST(tuple, TinyVarlenFormatOverflowLarge) { // NOLINT(cert-err58-cpp)
    static constexpr tuple_num_t NUM_ELEMENTS = 301;

    // Flags - 1 zero byte
    // Varlen table - 1 byte for key column + 300 bytes for value columns
    // Key - 4 zero bytes for int32 zero.
    std::vector<std::byte> reference(306);
    std::fill(reference.begin() + 1, reference.end() - 4, std::byte{4});

    binary_tuple_parser tp(NUM_ELEMENTS, {reinterpret_cast<std::byte *>(reference.data()), reference.size()});

    auto first = tp.get_next();
    EXPECT_TRUE(first.has_value());
    EXPECT_EQ(0, get_value<int32_t>(first.value()));

    for (int i = 1; i < NUM_ELEMENTS; i++) {
        auto next = tp.get_next();
        EXPECT_TRUE(next.has_value());
        EXPECT_EQ(0, next.value().size());
    }
}

TEST(tuple, TinyVarlenFormatOverflowMedium) { // NOLINT(cert-err58-cpp)
    static constexpr tuple_num_t NUM_ELEMENTS = 301;

    // Flags - 1 zero byte
    // Varlen table - 301 bytes
    // Key - 4 zero bytes for int32 zero.
    // Varlen values - 3 bytes
    std::vector<int8_t> reference(309);
    // Vartable, offsets in medium format - Short per offset.
    reference[1] = 4;
    reference[2] = 5;
    reference[3] = 6;
    reference[4] = 7;

    // Offsets for zero-length arrays.
    for (int i = 0; i < 300 - 3; i++) {
        reference[5 + i] = 7;
    }

    // Non-empty arrays:
    reference[306] = 1;
    reference[307] = 2;
    reference[308] = 3;

    binary_tuple_parser tp(NUM_ELEMENTS, {reinterpret_cast<std::byte *>(reference.data()), reference.size()});

    auto first = tp.get_next();
    EXPECT_TRUE(first.has_value());
    EXPECT_EQ(0, get_value<int32_t>(first.value()));

    for (int i = 1; i <= 3; i++) {
        auto next = tp.get_next();
        EXPECT_TRUE(next.has_value());
        EXPECT_EQ(1, next->size());
        EXPECT_EQ(i, get_value<int8_t>(next.value()));
    }
    for (tuple_num_t i = 4; i < NUM_ELEMENTS; i++) {
        auto next = tp.get_next();
        EXPECT_TRUE(next.has_value());
        EXPECT_EQ(0, next.value().size());
    }
}

// Check same binaries that used in testExpectedVarlenTupleBinaries java side test.
TEST(tuple, ExpectedVarlenTupleBinaries) { // NOLINT(cert-err58-cpp)
    static constexpr tuple_num_t NUM_ELEMENTS = 7;

    // clang-format off
    std::vector<int8_t> referenceTiny {
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

    tuple_view tiny = parser(NUM_ELEMENTS, referenceTiny).parse();

    EXPECT_TRUE(tiny[0].has_value());
    EXPECT_EQ(0, get_value<int32_t>(tiny[0].value()));

    EXPECT_TRUE(tiny[1].has_value());
    EXPECT_EQ(1, get_value<int32_t>(tiny[1].value()));

    EXPECT_TRUE(tiny[2].has_value());
    EXPECT_EQ(2, get_value<int32_t>(tiny[2].value()));

    EXPECT_TRUE(tiny[3].has_value());
    EXPECT_EQ(3, get_value<int32_t>(tiny[3].value()));

    EXPECT_TRUE(tiny[4].has_value());
    bytes_view varlen = tiny[4].value();

    EXPECT_EQ(5, varlen.size());
    EXPECT_EQ(1, std::to_integer<int>(varlen[0]));
    EXPECT_EQ(2, std::to_integer<int>(varlen[1]));
    EXPECT_EQ(3, std::to_integer<int>(varlen[2]));
    EXPECT_EQ(4, std::to_integer<int>(varlen[3]));
    EXPECT_EQ(5, std::to_integer<int>(varlen[4]));

    EXPECT_TRUE(tiny[5].has_value());
    varlen = tiny[5].value();

    EXPECT_EQ(3, varlen.size());
    EXPECT_EQ(1, std::to_integer<int>(varlen[0]));
    EXPECT_EQ(2, std::to_integer<int>(varlen[1]));
    EXPECT_EQ(3, std::to_integer<int>(varlen[2]));

    EXPECT_TRUE(tiny[6].has_value());
    varlen = tiny[6].value();
    std::string str(reinterpret_cast<const char *>(varlen.data()), varlen.size());
    EXPECT_EQ("abc", str);

    // Make data that doesn't fit to tiny format tuple but fits to medium format tuple.
    std::vector<int8_t> mediumArray(256 * 2);
    for (size_t i = 0; i < mediumArray.size(); i++) {
        mediumArray[i] = (int8_t) ((i * 127) % 256);
    }

    // Construct reference tuple binary independently of tuple assembler.
    // clang-format off
    std::vector<int8_t> referenceMedium {
        // Flags.
        1,
        // Offsets for values.
        4, 0, 8, 0, 12, 0, 16, 0, 21, 0,
        int8_t(21 + mediumArray.size()), int8_t((21 + mediumArray.size()) >> 8),
        int8_t(24 + mediumArray.size()), int8_t((24 + mediumArray.size()) >> 8),
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
    referenceMedium.insert(referenceMedium.end() - 3, mediumArray.begin(), mediumArray.end());

    tuple_view medium = parser(NUM_ELEMENTS, referenceMedium).parse();

    EXPECT_TRUE(medium[0].has_value());
    EXPECT_EQ(0, get_value<int32_t>(medium[0].value()));

    EXPECT_TRUE(medium[1].has_value());
    EXPECT_EQ(1, get_value<int32_t>(medium[1].value()));

    EXPECT_TRUE(medium[2].has_value());
    EXPECT_EQ(2, get_value<int32_t>(medium[2].value()));

    EXPECT_TRUE(medium[3].has_value());
    EXPECT_EQ(3, get_value<int32_t>(medium[3].value()));

    EXPECT_TRUE(medium[4].has_value());
    varlen = medium[4].value();

    EXPECT_EQ(5, varlen.size());
    EXPECT_EQ(1, std::to_integer<int>(varlen[0]));
    EXPECT_EQ(2, std::to_integer<int>(varlen[1]));
    EXPECT_EQ(3, std::to_integer<int>(varlen[2]));
    EXPECT_EQ(4, std::to_integer<int>(varlen[3]));
    EXPECT_EQ(5, std::to_integer<int>(varlen[4]));

    EXPECT_TRUE(medium[5].has_value());
    varlen = medium[5].value();

    EXPECT_TRUE(mediumArray
        == std::vector<int8_t>(reinterpret_cast<const int8_t *>(varlen.data()),
            reinterpret_cast<const int8_t *>(varlen.data() + varlen.size())));

    EXPECT_TRUE(medium[6].has_value());
    varlen = medium[6].value();
    str = std::string(reinterpret_cast<const char *>(varlen.data()), varlen.size());
    EXPECT_EQ("abc", str);

    // Make data that doesn't fit to medium format tuple but fits to large format tuple.
    std::vector<int8_t> largeArray(64 * 2 * 1024);
    for (size_t i = 0; i < largeArray.size(); i++) {
        largeArray[i] = (int8_t) ((i * 127) % 256);
    }

    // Construct reference tuple binary independently of tuple assembler.
    // clang-format off
    std::vector<int8_t> referenceLarge {
        // Flags.
        2,
        // Offsets for values.
        4, 0, 0, 0, 8, 0, 0, 0, 12, 0, 0, 0, 16, 0, 0, 0, 21, 0, 0, 0,
        int8_t(21 + largeArray.size()), int8_t((21 + largeArray.size()) >> 8),
        int8_t((21 + largeArray.size()) >> 16), int8_t((21 + largeArray.size()) >> 24),
        int8_t(24 + largeArray.size()), int8_t((24 + largeArray.size()) >> 8),
        int8_t((24 + largeArray.size()) >> 16), int8_t((24 + largeArray.size()) >> 24),
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
    referenceLarge.insert(referenceLarge.end() - 3, largeArray.begin(), largeArray.end());

    tuple_view large = parser(NUM_ELEMENTS, referenceLarge).parse();

    EXPECT_TRUE(large[0].has_value());
    EXPECT_EQ(0, get_value<int32_t>(large[0].value()));

    EXPECT_TRUE(large[1].has_value());
    EXPECT_EQ(1, get_value<int32_t>(large[1].value()));

    EXPECT_TRUE(large[2].has_value());
    EXPECT_EQ(2, get_value<int32_t>(large[2].value()));

    EXPECT_TRUE(large[3].has_value());
    EXPECT_EQ(3, get_value<int32_t>(large[3].value()));

    EXPECT_TRUE(large[4].has_value());
    varlen = large[4].value();

    EXPECT_EQ(5, varlen.size());
    EXPECT_EQ(1, std::to_integer<int>(varlen[0]));
    EXPECT_EQ(2, std::to_integer<int>(varlen[1]));
    EXPECT_EQ(3, std::to_integer<int>(varlen[2]));
    EXPECT_EQ(4, std::to_integer<int>(varlen[3]));
    EXPECT_EQ(5, std::to_integer<int>(varlen[4]));

    EXPECT_TRUE(large[5].has_value());
    varlen = large[5].value();

    EXPECT_TRUE(largeArray
        == std::vector<int8_t>(reinterpret_cast<const int8_t *>(varlen.data()),
            reinterpret_cast<const int8_t *>(varlen.data() + varlen.size())));

    EXPECT_TRUE(large[6].has_value());
    varlen = large[6].value();
    str = std::string(reinterpret_cast<const char *>(varlen.data()), varlen.size());
    EXPECT_EQ("abc", str);
}

TEST(tuple, StringAfterNull) {
    static constexpr tuple_num_t NUM_ELEMENTS = 3;

    // 101, null, "Bob"
    std::vector<std::byte> tuple;
    for (int i : {4, 2, 1, 1, 4, 101, 66, 111, 98}) {
        tuple.push_back(static_cast<std::byte>(i));
    }

    binary_tuple_parser tp(NUM_ELEMENTS, tuple);

    EXPECT_EQ(101, get_value<int32_t>(tp.get_next()));
    EXPECT_FALSE(tp.get_next().has_value());
    EXPECT_EQ("Bob", get_value<std::string>(tp.get_next()));
}

TEST(tuple, EmptyValueTupleAssembler) { // NOLINT(cert-err58-cpp)
    static constexpr tuple_num_t NUM_ELEMENTS = 1;

    builder ta(NUM_ELEMENTS);

    auto tuple = ta.build(std::tuple<int32_t>(123));

    binary_tuple_parser tp(NUM_ELEMENTS, tuple);

    auto slice = tp.get_next();
    ASSERT_TRUE(slice.has_value());
    EXPECT_EQ(123, get_value<int32_t>(slice.value()));
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
            tb.claim(std::nullopt);
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
            tb.append(std::nullopt);
        } else {
            tb.append_int64(8);
        }
        tb.append_int8(9);

        auto &tuple = tb.build();

        tuple_view t = parser(NUM_ELEMENTS, tuple).parse();

        ASSERT_TRUE(t[0].has_value());
        EXPECT_EQ(1234, get_value<int32_t>(t[0].value()));

        ASSERT_TRUE(t[1].has_value());
        EXPECT_EQ(5.0, get_value<double>(t[1].value()));

        ASSERT_TRUE(t[2].has_value());
        EXPECT_EQ(6, get_value<int8_t>(t[2].value()));

        ASSERT_TRUE(t[3].has_value());
        EXPECT_EQ(7, get_value<int16_t>(t[3].value()));

        if (nullable) {
            EXPECT_FALSE(t[4].has_value());
        } else {
            ASSERT_TRUE(t[4].has_value());
            EXPECT_EQ(8, get_value<int64_t>(t[4].value()));
        }

        ASSERT_TRUE(t[5].has_value());
        EXPECT_EQ(9, get_value<int8_t>(t[5].value()));
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
            tb.claim(std::nullopt);
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
            tb.append(std::nullopt);
        } else {
            tb.append_int8(4);
        }
        tb.append_int8(5);

        auto &tuple = tb.build();

        tuple_view t = parser(NUM_ELEMENTS, tuple).parse();

        ASSERT_TRUE(t[0].has_value());
        EXPECT_EQ(0, get_value<int8_t>(t[0].value()));

        ASSERT_TRUE(t[1].has_value());
        EXPECT_EQ(1, get_value<int8_t>(t[1].value()));

        ASSERT_TRUE(t[2].has_value());
        EXPECT_EQ(2, get_value<int8_t>(t[2].value()));

        ASSERT_TRUE(t[3].has_value());
        EXPECT_EQ(3, get_value<int8_t>(t[3].value()));

        if (nullable) {
            EXPECT_FALSE(t[4].has_value());
        } else {
            ASSERT_TRUE(t[4].has_value());
            EXPECT_EQ(4, get_value<int8_t>(t[4].value()));
        }

        ASSERT_TRUE(t[5].has_value());
        EXPECT_EQ(5, get_value<int8_t>(t[5].value()));
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
        EXPECT_FALSE(tp.get_next().has_value());
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

        EXPECT_FALSE(tp.get_next().has_value());
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
            tb.claim_string(value);
        }

        tb.layout();

        tb.append_int32(100500);

        for (int i = 1; i < NUM_ELEMENTS; i++) {
            tb.append_string(value);
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
            tb.claim_string(value);
        }

        tb.layout();

        tb.append_int32(100500);

        for (int i = 1; i < NUM_ELEMENTS; i++) {
            tb.append_string(value);
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
