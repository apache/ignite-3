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

#include "binary_tuple_parser.h"
#include "column_info.h"
#include "tuple_assembler.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <array>
#include <string>

using namespace std::string_literals;
using namespace ignite;

template<typename T>
T read_tuple(std::optional<bytes_view> data);

template<>
int8_t read_tuple(std::optional<bytes_view> data) {
    return binary_tuple_parser::get_int8(data.value());
}

template<>
int16_t read_tuple(std::optional<bytes_view> data) {
    return binary_tuple_parser::get_int16(data.value());
}

template<>
int32_t read_tuple(std::optional<bytes_view> data) {
    return binary_tuple_parser::get_int32(data.value());
}

template<>
int64_t read_tuple(std::optional<bytes_view> data) {
    return binary_tuple_parser::get_int64(data.value());
}

template<>
double read_tuple(std::optional<bytes_view> data) {
    return binary_tuple_parser::get_double(data.value());
}

template<>
float read_tuple(std::optional<bytes_view> data) {
    return binary_tuple_parser::get_float(data.value());
}

template<>
big_integer read_tuple(std::optional<bytes_view> data) {
    return binary_tuple_parser::get_number(data.value());
}

big_decimal read_decimal(std::optional<bytes_view> data, int32_t scale) {
    return binary_tuple_parser::get_decimal(data.value(), scale);
}

template<>
ignite_date read_tuple(std::optional<bytes_view> data) {
    return binary_tuple_parser::get_date(data.value());
}

template<>
ignite_date_time read_tuple(std::optional<bytes_view> data) {
    return binary_tuple_parser::get_date_time(data.value());
}

template<>
uuid read_tuple(std::optional<bytes_view> data) {
    return binary_tuple_parser::get_uuid(data.value());
}

template<>
ignite_time read_tuple(std::optional<bytes_view> data) {
    return binary_tuple_parser::get_time(data.value());
}

template<>
ignite_timestamp read_tuple(std::optional<bytes_view> data) {
    return binary_tuple_parser::get_timestamp(data.value());
}

template<>
std::string read_tuple(std::optional<bytes_view> data) {
    return {reinterpret_cast<const char *>(data->data()), data->size()};
}

struct SchemaDescriptor {
    std::vector<column_info> columns;

    [[nodiscard]] number_t length() const { return number_t(columns.size()); }

    [[nodiscard]] binary_tuple_schema to_tuple_schema() const {
        return binary_tuple_schema({columns.begin(), columns.end()});
    }
};

struct SingleValueCheck {
    explicit SingleValueCheck(binary_tuple_parser &parser)
        : parser{parser} {}

    binary_tuple_parser &parser;

    template<typename T>
    void operator()(T value) {
        EXPECT_EQ(value, read_tuple<T>(parser.get_next()));
    }

    void operator()(std::nullopt_t /*null*/) { EXPECT_FALSE(parser.get_next()); }
};

template<typename... Ts, int size>
void checkReaderWriterEquality(const SchemaDescriptor &schema, const std::tuple<Ts...> &values,
    const int8_t (&data)[size], bool skipAssemblerCheck = false) {

    auto bytes = bytes_view{reinterpret_cast<const std::byte *>(data), size};

    binary_tuple_parser tp(schema.length(), bytes);

    std::apply([&tp](const Ts... args) { ((SingleValueCheck(tp)(args)), ...); }, values);

    EXPECT_EQ(tp.num_elements(), tp.num_parsed_elements());

    if (!skipAssemblerCheck) {
        tuple_assembler ta(schema.to_tuple_schema());

        bytes_view built = ta.build(values);

        EXPECT_THAT(built, testing::ContainerEq(bytes));
    }
}

TEST(tuple, FixedKeyFixedValue) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns = {
        {ignite_type::INT16, false},
        {ignite_type::INT16, false},
    };

    // With value.
    {
        const std::tuple<int16_t, int16_t> &values = std::make_tuple<int16_t, int16_t>(33, -71);

        int8_t data[] = {0, 1, 2, 33, -71};

        checkReaderWriterEquality(schema, values, data);
    }
}

TEST(tuple, AllTypes) {
    SchemaDescriptor schema;

    schema.columns = {
        {ignite_type::INT8, false},
        {ignite_type::INT16, false},
        {ignite_type::INT32, false},
        {ignite_type::INT64, false},
        {ignite_type::NUMBER, false},
        {ignite_type::DECIMAL, false},
        {ignite_type::FLOAT, false},
        {ignite_type::DOUBLE, false},
        {ignite_type::DATE, false},
        {ignite_type::TIME, false},
        {ignite_type::DATETIME, false},
        {ignite_type::TIMESTAMP, false},
        {ignite_type::STRING, false},
        {ignite_type::UUID, false},
        {ignite_type::BYTE_ARRAY, false},
    };

    binary_tuple_schema sch = schema.to_tuple_schema();

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

    binary_tuple_builder tb(sch.num_elements());
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
    binary_tuple_parser tp(schema.length(), binary_tuple);

    EXPECT_EQ(v1, read_tuple<int8_t>(tp.get_next()));
    EXPECT_EQ(v2, read_tuple<int16_t>(tp.get_next()));
    EXPECT_EQ(v3, read_tuple<int32_t>(tp.get_next()));
    EXPECT_EQ(v4, read_tuple<int64_t>(tp.get_next()));
    EXPECT_EQ(v5, read_tuple<big_integer>(tp.get_next()));
    EXPECT_EQ(v6, read_decimal(tp.get_next(), v6.get_scale()));
    EXPECT_EQ(v7, read_tuple<float>(tp.get_next()));
    EXPECT_EQ(v8, read_tuple<double>(tp.get_next()));
    EXPECT_EQ(v9, read_tuple<ignite_date>(tp.get_next()));
    EXPECT_EQ(v10, read_tuple<ignite_time>(tp.get_next()));
    EXPECT_EQ(v11, read_tuple<ignite_date_time>(tp.get_next()));
    EXPECT_EQ(v12, read_tuple<ignite_timestamp>(tp.get_next()));
    EXPECT_EQ(v13, read_tuple<std::string>(tp.get_next()));
    EXPECT_EQ(v14, read_tuple<uuid>(tp.get_next()));
    EXPECT_EQ(v15, tp.get_next());
}

TEST(tuple, FixedKeyFixedNullableValue) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns = {
        {ignite_type::INT32, false},
        {ignite_type::INT32, true},
    };

    // With value.
    {
        auto values = std::make_tuple<int32_t, int32_t>(33, -71);

        int8_t data[] = {0, 1, 2, 33, -71};

        checkReaderWriterEquality(schema, values, data);
    }

    // Null value.
    {
        auto values = std::make_tuple(int32_t{77}, std::nullopt);

        int8_t data[] = {4, 2, 1, 1, 77};

        checkReaderWriterEquality(schema, values, data);
    }
}

TEST(tuple, FixedKeyVarlenValue) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns = {
        {ignite_type::INT16, false},
        {ignite_type::STRING, true},
    };

    // With value.
    {
        auto values = std::make_tuple(int16_t{-33}, std::string{"val"});

        int8_t data[] = {0, 1, 4, -33, 118, 97, 108};

        checkReaderWriterEquality(schema, values, data);
    }
}

TEST(tuple, FixedKeyVarlenNullableValue) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns = {
        {ignite_type::INT16, false},
        {ignite_type::STRING, true},
    };

    // With value.
    {
        auto values = std::make_tuple(int16_t{-33}, std::string{"val"});

        int8_t data[] = {0, 1, 4, -33, 118, 97, 108};

        checkReaderWriterEquality(schema, values, data);
    }

    // Null value.
    {
        auto values = std::make_tuple(int16_t{33}, std::nullopt);

        int8_t data[] = {4, 2, 1, 1, 33};

        checkReaderWriterEquality(schema, values, data);
    }
}

TEST(tuple, VarlenNullableKeyVarlenNullableValue) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns = {
        {ignite_type::STRING, true},
        {ignite_type::STRING, true},
    };

    // With key and value.
    {
        auto values = std::make_tuple(std::string{"key"}, std::string{"val"});

        int8_t data[] = {0, 3, 6, 107, 101, 121, 118, 97, 108};

        checkReaderWriterEquality(schema, values, data);
    }

    // Null key.
    {
        auto values = std::make_tuple(std::nullopt, std::string{"val"});

        int8_t data[] = {4, 1, 0, 3, 118, 97, 108};

        checkReaderWriterEquality(schema, values, data);
    }

    // Null value.
    {
        auto values = std::make_tuple(std::string{"key"}, std::nullopt);

        int8_t data[] = {4, 2, 3, 3, 107, 101, 121};

        checkReaderWriterEquality(schema, values, data);
    }

    // Null both.
    {
        auto values = std::make_tuple(std::nullopt, std::nullopt);

        int8_t data[] = {4, 3, 0, 0};

        checkReaderWriterEquality(schema, values, data);
    }
}

TEST(tuple, FixedAndVarlenKeyFixedAndVarlenValue) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns = {
        {ignite_type::INT16, false},
        {ignite_type::STRING, false},
        {ignite_type::INT32, true},
        {ignite_type::STRING, true},
    };

    // With value.
    {
        auto values = std::make_tuple(int16_t{33}, std::string{"keystr"}, int32_t{73}, std::string{"valstr"});

        int8_t data[] = {0, 1, 7, 8, 14, 33, 107, 101, 121, 115, 116, 114, 73, 118, 97, 108, 115, 116, 114};

        checkReaderWriterEquality(schema, values, data);
    }

    // Null value.
    {
        auto values = std::make_tuple(int16_t{33}, std::string{"keystr2"}, std::nullopt, std::nullopt);

        int8_t data[] = {4, 12, 1, 8, 8, 8, 33, 107, 101, 121, 115, 116, 114, 50};

        checkReaderWriterEquality(schema, values, data);
    }
}

TEST(tuple, FixedNullableColumns) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns = {
        {ignite_type::INT8, true},
        {ignite_type::INT16, true},
        {ignite_type::INT32, true},
        {ignite_type::INT8, true},
        {ignite_type::INT16, true},
        {ignite_type::INT32, true},
    };

    // Last column null.
    {
        auto values = std::make_tuple(int8_t{11}, int16_t{22}, std::nullopt, int8_t{-44}, int16_t{-66}, std::nullopt);

        int8_t data[] = {4, 36, 1, 2, 2, 3, 4, 4, 11, 22, -44, -66};

        checkReaderWriterEquality(schema, values, data);
    }

    // First column null.
    {
        auto values = std::make_tuple(std::nullopt, int16_t{22}, int32_t{33}, std::nullopt, int16_t{-55}, int32_t{-66});

        int8_t data[] = {4, 9, 0, 1, 2, 2, 3, 4, 22, 33, -55, -66};

        checkReaderWriterEquality(schema, values, data);
    }

    // Middle column null.
    {
        auto values = std::make_tuple(int8_t{11}, std::nullopt, int32_t{33}, int8_t{-44}, std::nullopt, int32_t{-66});

        int8_t data[] = {4, 18, 1, 1, 2, 3, 3, 4, 11, 33, -44, -66};

        checkReaderWriterEquality(schema, values, data);
    }

    // All columns null.
    {
        auto values =
            std::make_tuple(std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt);

        int8_t data[] = {4, 63, 0, 0, 0, 0, 0, 0};

        checkReaderWriterEquality(schema, values, data);
    }
}

TEST(tuple, VarlenNullableColumns) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns = {
        {ignite_type::STRING, true},
        {ignite_type::STRING, true},
        {ignite_type::STRING, true},
        {ignite_type::STRING, true},
        {ignite_type::STRING, true},
        {ignite_type::STRING, true},
    };

    // Last column null.
    {
        auto values = std::make_tuple("abc"s, "ascii"s, std::nullopt, "yz"s, "我愛Java"s, std::nullopt);

        int8_t data[] = {4, 36, 3, 8, 8, 10, 20, 20, 97, 98, 99, 97, 115, 99, 105, 105, 121, 122, -26, -120, -111, -26,
            -124, -101, 74, 97, 118, 97};

        checkReaderWriterEquality(schema, values, data);
    }

    // First column null.
    {
        auto values = std::make_tuple(std::nullopt, "ascii"s, "我愛Java"s, std::nullopt, "我愛Java"s, "ascii"s);

        int8_t data[] = {4, 9, 0, 5, 15, 15, 25, 30, 97, 115, 99, 105, 105, -26, -120, -111, -26, -124, -101, 74, 97,
            118, 97, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97, 97, 115, 99, 105, 105};

        checkReaderWriterEquality(schema, values, data);
    }

    // Middle column null.
    {
        auto values = std::make_tuple("abc"s, std::nullopt, "我愛Java"s, "yz"s, std::nullopt, "ascii"s);

        int8_t data[] = {4, 18, 3, 3, 13, 15, 15, 20, 97, 98, 99, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97,
            121, 122, 97, 115, 99, 105, 105};

        checkReaderWriterEquality(schema, values, data);
    }

    // All columns null.
    {
        auto values =
            std::make_tuple(std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt);

        int8_t data[] = {4, 63, 0, 0, 0, 0, 0, 0};

        checkReaderWriterEquality(schema, values, data);
    }
}

TEST(tuple, FixedAndVarlenNullableColumns) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns = {
        {ignite_type::INT8, true},
        {ignite_type::INT16, true},
        {ignite_type::STRING, true},
        {ignite_type::STRING, true},
        {ignite_type::INT8, true},
        {ignite_type::INT16, true},
        {ignite_type::STRING, true},
        {ignite_type::STRING, true},
    };

    // Check null/non-null all fixed/varlen.
    {
        auto values = std::make_tuple(
            int8_t{11}, int16_t{22}, std::nullopt, std::nullopt, std::nullopt, std::nullopt, "yz"s, "ascii"s);

        int8_t data[] = {4, 60, 1, 2, 2, 2, 2, 2, 4, 9, 11, 22, 121, 122, 97, 115, 99, 105, 105};

        checkReaderWriterEquality(schema, values, data);
    }

    // Check null/non-null single fixed.
    {
        auto values =
            std::make_tuple(std::nullopt, int16_t{22}, "ab"s, "我愛Java"s, int8_t{55}, std::nullopt, "yz"s, "ascii"s);

        int8_t data[] = {4, 33, 0, 1, 3, 13, 14, 14, 16, 21, 22, 97, 98, -26, -120, -111, -26, -124, -101, 74, 97, 118,
            97, 55, 121, 122, 97, 115, 99, 105, 105};

        checkReaderWriterEquality(schema, values, data);
    }

    // Check null/non-null single varlen.
    {
        auto values = std::make_tuple(
            int8_t{11}, int16_t{22}, std::nullopt, "我愛Java"s, int8_t{55}, int16_t{66}, "yz"s, std::nullopt);

        int8_t data[] = {4, -124, 1, 2, 2, 12, 13, 14, 16, 16, 11, 22, -26, -120, -111, -26, -124, -101, 74, 97, 118,
            97, 55, 66, 121, 122};

        checkReaderWriterEquality(schema, values, data);
    }

    // Check null/non-null mixed.
    {
        auto values = std::make_tuple(
            int8_t{11}, std::nullopt, std::nullopt, "我愛Java"s, std::nullopt, int16_t{22}, "yz"s, std::nullopt);

        int8_t data[] = {
            4, -106, 1, 1, 1, 11, 11, 12, 14, 14, 11, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97, 22, 121, 122};

        checkReaderWriterEquality(schema, values, data);
    }

    // Check all null/non-null.
    {
        auto values = std::make_tuple(
            int8_t{11}, int16_t{22}, "ab"s, "我愛Java"s, std::nullopt, std::nullopt, std::nullopt, std::nullopt);

        int8_t data[] = {
            4, -16, 1, 2, 4, 14, 14, 14, 14, 14, 11, 22, 97, 98, -26, -120, -111, -26, -124, -101, 74, 97, 118, 97};

        checkReaderWriterEquality(schema, values, data);
    }
}

TEST(tuple, ZeroLengthVarlen) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns = {
        {ignite_type::INT32, false},
        {ignite_type::BYTE_ARRAY, false},
    };

    // Single zero-length vector of bytes.
    {
        auto values = std::make_tuple(int32_t{0}, ""s);

        int8_t data[] = {0, 0, 0};

        checkReaderWriterEquality(schema, values, data);
    }

    // Two zero-length vectors of bytes.
    {
        schema.columns.emplace_back(column_info{ignite_type::BYTE_ARRAY, false});

        auto values = std::make_tuple(int32_t{0}, ""s, ""s);

        int8_t data[] = {0, 0, 0, 0};

        checkReaderWriterEquality(schema, values, data);
    }

    // Two zero-length vectors of bytes and single integer value.
    {
        schema.columns.erase(schema.columns.begin() + 1, schema.columns.end());

        schema.columns.emplace_back(column_info{ignite_type::INT32, false});
        schema.columns.emplace_back(column_info{ignite_type::BYTE_ARRAY, false});
        schema.columns.emplace_back(column_info{ignite_type::BYTE_ARRAY, false});

        auto values = std::make_tuple(int32_t{0}, int32_t{123}, ""s, ""s);

        int8_t data[] = {0, 0, 1, 1, 1, 123};

        checkReaderWriterEquality(schema, values, data);
    }
}

TEST(tuple, SingleVarlen) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns = {
        {ignite_type::INT32, false},
        {ignite_type::BYTE_ARRAY, false},
    };

    auto values = std::make_tuple(int32_t{0}, "\1\2\3"s);

    int8_t data[] = {0, 0, 3, 1, 2, 3};

    checkReaderWriterEquality(schema, values, data);
}

TEST(tuple, TinyVarlenFormatOverflowLarge) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns.emplace_back(column_info{ignite_type::INT32, false});
    for (int i = 0; i < 300; i++) {
        schema.columns.emplace_back(column_info{ignite_type::BYTE_ARRAY, false});
    }

    // Flags - 1 zero byte
    // Varlen table - 1 byte for key column + 300 bytes for value columns
    // Key - 4 zero bytes for int32 zero.
    std::vector<std::byte> reference(306);
    std::fill(reference.begin() + 1, reference.end() - 4, std::byte{4});

    binary_tuple_parser tp(schema.length(), {reinterpret_cast<std::byte *>(reference.data()), reference.size()});

    auto first = tp.get_next();
    EXPECT_TRUE(first.has_value());
    EXPECT_EQ(0, read_tuple<int32_t>(first.value()));

    for (int i = 1; i < schema.length(); i++) {
        auto next = tp.get_next();
        EXPECT_TRUE(next.has_value());
        EXPECT_EQ(0, next.value().size());
    }
}

TEST(tuple, TinyVarlenFormatOverflowMedium) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns.emplace_back(column_info{ignite_type::INT32, false});
    for (int i = 0; i < 300; i++) {
        schema.columns.emplace_back(column_info{ignite_type::BYTE_ARRAY, false});
    }

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

    binary_tuple_parser tp(schema.length(), {reinterpret_cast<std::byte *>(reference.data()), reference.size()});

    auto first = tp.get_next();
    EXPECT_TRUE(first.has_value());
    EXPECT_EQ(0, read_tuple<int32_t>(first.value()));

    for (int i = 1; i <= 3; i++) {
        auto next = tp.get_next();
        EXPECT_TRUE(next.has_value());
        EXPECT_EQ(1, next->size());
        EXPECT_EQ(i, read_tuple<int8_t>(next.value()));
    }
    for (number_t i = 4; i < schema.length(); i++) {
        auto next = tp.get_next();
        EXPECT_TRUE(next.has_value());
        EXPECT_EQ(0, next.value().size());
    }
}

// Check same binaries that used in testExpectedVarlenTupleBinaries java side test.
TEST(tuple, ExpectedVarlenTupleBinaries) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    // Value columns are sorted here like they are sorted on java side during schema construction.
    // Fixed size columns go first, varlen columns last.
    schema.columns = {
        {ignite_type::INT32, false},
        {ignite_type::INT32, false},
        {ignite_type::INT32, false},
        {ignite_type::INT32, false},
        {ignite_type::BYTE_ARRAY, false},
        {ignite_type::BYTE_ARRAY, false},
        {ignite_type::STRING, false},
    };

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

    binary_tuple_parser tpTiny(
        schema.length(), {reinterpret_cast<std::byte *>(referenceTiny.data()), referenceTiny.size()});
    tuple_view tiny = tpTiny.parse();

    EXPECT_TRUE(tiny[0].has_value());
    EXPECT_EQ(0, read_tuple<int32_t>(tiny[0].value()));

    EXPECT_TRUE(tiny[1].has_value());
    EXPECT_EQ(1, read_tuple<int32_t>(tiny[1].value()));

    EXPECT_TRUE(tiny[2].has_value());
    EXPECT_EQ(2, read_tuple<int32_t>(tiny[2].value()));

    EXPECT_TRUE(tiny[3].has_value());
    EXPECT_EQ(3, read_tuple<int32_t>(tiny[3].value()));

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

    binary_tuple_parser tpMedium(
        schema.length(), {reinterpret_cast<std::byte *>(referenceMedium.data()), referenceMedium.size()});
    tuple_view medium = tpMedium.parse();

    EXPECT_TRUE(medium[0].has_value());
    EXPECT_EQ(0, read_tuple<int32_t>(medium[0].value()));

    EXPECT_TRUE(medium[1].has_value());
    EXPECT_EQ(1, read_tuple<int32_t>(medium[1].value()));

    EXPECT_TRUE(medium[2].has_value());
    EXPECT_EQ(2, read_tuple<int32_t>(medium[2].value()));

    EXPECT_TRUE(medium[3].has_value());
    EXPECT_EQ(3, read_tuple<int32_t>(medium[3].value()));

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

    binary_tuple_parser tpLarge(
        schema.length(), {reinterpret_cast<std::byte *>(referenceLarge.data()), referenceLarge.size()});
    tuple_view large = tpLarge.parse();

    EXPECT_TRUE(large[0].has_value());
    EXPECT_EQ(0, read_tuple<int32_t>(large[0].value()));

    EXPECT_TRUE(large[1].has_value());
    EXPECT_EQ(1, read_tuple<int32_t>(large[1].value()));

    EXPECT_TRUE(large[2].has_value());
    EXPECT_EQ(2, read_tuple<int32_t>(large[2].value()));

    EXPECT_TRUE(large[3].has_value());
    EXPECT_EQ(3, read_tuple<int32_t>(large[3].value()));

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
    SchemaDescriptor schema;

    schema.columns.emplace_back(column_info{ignite_type::INT32, false});
    schema.columns.emplace_back(column_info{ignite_type::BYTE_ARRAY, true});
    schema.columns.emplace_back(column_info{ignite_type::STRING, false});

    // 101, null, "Bob"
    std::vector<std::byte> tuple;
    for (int i : {4, 2, 1, 1, 4, 101, 66, 111, 98}) {
        tuple.push_back(static_cast<std::byte>(i));
    }

    binary_tuple_parser tp(schema.length(), tuple);

    EXPECT_EQ(101, read_tuple<int32_t>(tp.get_next()));
    EXPECT_FALSE(tp.get_next().has_value());
    EXPECT_EQ("Bob", read_tuple<std::string>(tp.get_next()));
}

TEST(tuple, EmptyValueTupleAssembler) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns.emplace_back(column_info{ignite_type::INT32, false});

    tuple_assembler ta(schema.to_tuple_schema());

    auto tuple = ta.build(std::tuple<int32_t>(123));

    binary_tuple_parser tp(schema.length(), tuple);

    auto slice = tp.get_next();
    ASSERT_TRUE(slice.has_value());
    EXPECT_EQ(123, read_tuple<int32_t>(slice.value()));
}

TEST(tuple, TupleWriteRead) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns.emplace_back(column_info{ignite_type::INT32, false});
    schema.columns.emplace_back(column_info{ignite_type::DOUBLE, false});
    schema.columns.emplace_back(column_info{ignite_type::INT8, false});
    schema.columns.emplace_back(column_info{ignite_type::INT16, false});
    schema.columns.emplace_back(column_info{ignite_type::INT64, false});
    schema.columns.emplace_back(column_info{ignite_type::INT8, false});

    for (bool nullable : {false, true}) {
        for (number_t i = 0; i < schema.length(); i++) {
            schema.columns[i].nullable = nullable;
        }

        binary_tuple_schema sch = schema.to_tuple_schema();
        binary_tuple_builder tb(sch.num_elements());

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

        binary_tuple_parser tp(schema.length(), tuple);
        tuple_view t = tp.parse();

        ASSERT_TRUE(t[0].has_value());
        EXPECT_EQ(1234, read_tuple<int32_t>(t[0].value()));

        ASSERT_TRUE(t[1].has_value());
        EXPECT_EQ(5.0, read_tuple<double>(t[1].value()));

        ASSERT_TRUE(t[2].has_value());
        EXPECT_EQ(6, read_tuple<int8_t>(t[2].value()));

        ASSERT_TRUE(t[3].has_value());
        EXPECT_EQ(7, read_tuple<int16_t>(t[3].value()));

        if (nullable) {
            EXPECT_FALSE(t[4].has_value());
        } else {
            ASSERT_TRUE(t[4].has_value());
            EXPECT_EQ(8, read_tuple<int64_t>(t[4].value()));
        }

        ASSERT_TRUE(t[5].has_value());
        EXPECT_EQ(9, read_tuple<int8_t>(t[5].value()));
    }
}

TEST(tuple, Int8TupleWriteRead) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns.emplace_back(column_info{ignite_type::INT8, false});
    schema.columns.emplace_back(column_info{ignite_type::INT8, false});
    schema.columns.emplace_back(column_info{ignite_type::INT8, false});
    schema.columns.emplace_back(column_info{ignite_type::INT8, false});
    schema.columns.emplace_back(column_info{ignite_type::INT8, false});
    schema.columns.emplace_back(column_info{ignite_type::INT8, false});

    for (bool nullable : {false, true}) {
        for (number_t i = 0; i < schema.length(); i++) {
            schema.columns[i].nullable = nullable;
        }

        binary_tuple_schema sch = schema.to_tuple_schema();
        binary_tuple_builder tb(sch.num_elements());

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

        binary_tuple_parser tp(schema.length(), tuple);
        tuple_view t = tp.parse();

        ASSERT_TRUE(t[0].has_value());
        EXPECT_EQ(0, read_tuple<int8_t>(t[0].value()));

        ASSERT_TRUE(t[1].has_value());
        EXPECT_EQ(1, read_tuple<int8_t>(t[1].value()));

        ASSERT_TRUE(t[2].has_value());
        EXPECT_EQ(2, read_tuple<int8_t>(t[2].value()));

        ASSERT_TRUE(t[3].has_value());
        EXPECT_EQ(3, read_tuple<int8_t>(t[3].value()));

        if (nullable) {
            EXPECT_FALSE(t[4].has_value());
        } else {
            ASSERT_TRUE(t[4].has_value());
            EXPECT_EQ(4, read_tuple<int8_t>(t[4].value()));
        }

        ASSERT_TRUE(t[5].has_value());
        EXPECT_EQ(5, read_tuple<int8_t>(t[5].value()));
    }
}

TEST(tuple, NullKeyTupleAssembler) { // NOLINT(cert-err58-cpp)
    GTEST_SKIP() << "Skip, as nullability is not checked during tuple assembly at the moment";
    SchemaDescriptor schema;

    schema.columns.emplace_back(column_info{ignite_type::INT32, false});

    {
        tuple_assembler ta(schema.to_tuple_schema());

        ta.start();

        ASSERT_THROW(ta.append_null(), std::runtime_error);
    }

    schema.columns[0].nullable = true;

    tuple_assembler ta(schema.to_tuple_schema());

    ta.start();

    ta.append_null();

    auto &tuple = ta.build(std::make_tuple(std::nullopt));

    binary_tuple_parser tp(schema.length(), tuple);

    ASSERT_FALSE(tp.get_next().has_value());
}

TEST(tuple, VarlenLargeTest) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns = {{ignite_type::STRING, true}, {ignite_type::STRING, true}, {ignite_type::STRING, true}};

    {
        auto values = std::make_tuple(std::string(100'000, 'a'), "b"s, std::nullopt);

        tuple_assembler tuple_assembler(schema.to_tuple_schema());

        bytes_view tuple = tuple_assembler.build(values);

        binary_tuple_parser tp(schema.length(), tuple);

        EXPECT_EQ(std::get<0>(values), read_tuple<std::string>(tp.get_next()));
        EXPECT_EQ(std::get<1>(values), read_tuple<std::string>(tp.get_next()));
        EXPECT_FALSE(tp.get_next().has_value());
    }
    {
        auto values = std::make_tuple(std::string(100'000, 'a'), "b"s, "c"s);

        tuple_assembler tuple_assembler(schema.to_tuple_schema());

        bytes_view tuple = tuple_assembler.build(values);

        binary_tuple_parser tp(schema.length(), tuple);

        EXPECT_EQ(std::get<0>(values), read_tuple<std::string>(tp.get_next()));
        EXPECT_EQ(std::get<1>(values), read_tuple<std::string>(tp.get_next()));
        EXPECT_EQ(std::get<2>(values), read_tuple<std::string>(tp.get_next()));
    }
    {
        schema.columns = {{ignite_type::INT8, true}, {ignite_type::STRING, true}, {ignite_type::STRING, true}};

        auto values = std::make_tuple(std::nullopt, std::string(100'000, 'a'), "b"s);

        tuple_assembler tuple_assembler(schema.to_tuple_schema());

        bytes_view tuple = tuple_assembler.build(values);

        binary_tuple_parser tp(schema.length(), tuple);

        EXPECT_FALSE(tp.get_next().has_value());
        EXPECT_EQ(std::get<1>(values), read_tuple<std::string>(tp.get_next()));
        EXPECT_EQ(std::get<2>(values), read_tuple<std::string>(tp.get_next()));
    }
}

TEST(tuple, VarlenMediumTest) { // NOLINT(cert-err58-cpp)
    SchemaDescriptor schema;

    schema.columns.emplace_back(column_info{ignite_type::INT32, false});
    for (int i = 0; i < 300; i++) {
        schema.columns.emplace_back(column_info{ignite_type::BYTE_ARRAY, false});
    }

    {
        binary_tuple_schema sch = schema.to_tuple_schema();
        binary_tuple_builder tb(sch.num_elements());
        tb.start();
        tb.claim_int32(100500);

        // Create a 10-char value.
        std::string value("0123456789");

        for (int i = 1; i < schema.length(); i++) {
            tb.claim_string(value);
        }

        tb.layout();

        tb.append_int32(100500);

        for (int i = 1; i < schema.length(); i++) {
            tb.append_string(value);
        }

        bytes_view tuple = tb.build();

        // The varlen area will take (10 * 300) = 3000 bytes. So the varlen table entry will take 2 bytes.
        // The flags field must be equal to log2(2) = 1.
        EXPECT_EQ(std::byte{1}, tuple[0]);

        binary_tuple_parser tp(schema.length(), tuple);

        EXPECT_EQ(100500, read_tuple<int32_t>(tp.get_next()));
        for (number_t i = 1; i < schema.length(); i++) {
            EXPECT_EQ(value, read_tuple<std::string>(tp.get_next()));
        }
    }

    {
        binary_tuple_schema sch({schema.columns.begin(), schema.columns.end()});
        binary_tuple_builder tb(sch.num_elements());
        tb.start();
        tb.claim_int32(100500);

        // Create a 300-char value.
        std::string value;
        for (int i = 0; i < 30; i++) {
            value += "0123456789";
        }

        for (int i = 1; i < schema.length(); i++) {
            tb.claim_string(value);
        }

        tb.layout();

        tb.append_int32(100500);

        for (int i = 1; i < schema.length(); i++) {
            tb.append_string(value);
        }

        bytes_view tuple = tb.build();

        // The varlen area will take (300 * 300) = 90000 bytes. The size value does not fit to
        // one or two bytes. So the varlen table entry will take 4 bytes. The flags field must
        // be equal to log2(4) = 2.
        EXPECT_EQ(std::byte{2}, tuple[0]);

        binary_tuple_parser tp(schema.length(), tuple);

        EXPECT_EQ(100500, read_tuple<int32_t>(tp.get_next()));
        for (number_t i = 1; i < schema.length(); i++) {
            EXPECT_EQ(value, read_tuple<std::string>(tp.get_next()));
        }
    }
}
