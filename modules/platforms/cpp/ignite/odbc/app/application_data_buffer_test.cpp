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

#include <ignite/common/big_decimal.h>
#include <ignite/common/uuid.h>
#include <ignite/odbc/app/application_data_buffer.h>
#include <ignite/odbc/system/odbc_constants.h>
#include <ignite/odbc/utility.h>

#include <gtest/gtest.h>

using namespace ignite;

/**
 * Make timestamp instance.
 */
ignite_timestamp make_timestamp(int year, int month, int day, int hour, int min, int sec, int nano) {
    tm tm_time{};
    tm_time.tm_year = year - 1900;
    tm_time.tm_mon = month - 1;
    tm_time.tm_mday = day;
    tm_time.tm_hour = hour;
    tm_time.tm_min = min;
    tm_time.tm_sec = sec;
    tm_time.tm_isdst = -1;

    auto ctime = mktime(&tm_time);
    return {ctime, nano};
}

class application_data_buffer_test : public ::testing::Test {};

TEST_F(application_data_buffer_test, put_int_to_string) {
    char buffer[1024];
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, buffer, sizeof(buffer), &res_len);

    app_buf.put_int8(12);
    EXPECT_TRUE(!strcmp(buffer, "12"));
    EXPECT_TRUE(res_len == strlen("12"));

    app_buf.put_int8(-12);
    EXPECT_TRUE(!strcmp(buffer, "-12"));
    EXPECT_TRUE(res_len == strlen("-12"));

    app_buf.put_int16(9876);
    EXPECT_TRUE(!strcmp(buffer, "9876"));
    EXPECT_TRUE(res_len == strlen("9876"));

    app_buf.put_int16(-9876);
    EXPECT_TRUE(!strcmp(buffer, "-9876"));
    EXPECT_TRUE(res_len == strlen("-9876"));

    app_buf.put_int32(1234567);
    EXPECT_TRUE(!strcmp(buffer, "1234567"));
    EXPECT_TRUE(res_len == strlen("1234567"));

    app_buf.put_int32(-1234567);
    EXPECT_TRUE(!strcmp(buffer, "-1234567"));
    EXPECT_TRUE(res_len == strlen("-1234567"));
}

TEST_F(application_data_buffer_test, put_float_to_string) {
    char buffer[1024];
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, buffer, sizeof(buffer), &res_len);

    app_buf.put_float(12.42f);
    EXPECT_TRUE(!strcmp(buffer, "12.42"));
    EXPECT_TRUE(res_len == strlen("12.42"));

    app_buf.put_float(-12.42f);
    EXPECT_TRUE(!strcmp(buffer, "-12.42"));
    EXPECT_TRUE(res_len == strlen("-12.42"));

    app_buf.put_double(1000.21);
    EXPECT_TRUE(!strcmp(buffer, "1000.21"));
    EXPECT_TRUE(res_len == strlen("1000.21"));

    app_buf.put_double(-1000.21);
    EXPECT_TRUE(!strcmp(buffer, "-1000.21"));
    EXPECT_TRUE(res_len == strlen("-1000.21"));
}

TEST_F(application_data_buffer_test, put_guid_to_string) {
    char buffer[1024];
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, buffer, sizeof(buffer), &res_len);

    uuid value(0x1da1ef8f39ff4d62LL, 0x8b72e8e9f3371801LL);

    app_buf.put_uuid(value);

    EXPECT_TRUE(!strcmp(buffer, "1da1ef8f-39ff-4d62-8b72-e8e9f3371801"));
    EXPECT_TRUE(res_len == strlen("1da1ef8f-39ff-4d62-8b72-e8e9f3371801"));
}

TEST_F(application_data_buffer_test, put_binary_to_string) {
    char buffer[1024];
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, buffer, sizeof(buffer), &res_len);

    uint8_t binary[] = {0x21, 0x84, 0xF4, 0xDC, 0x01, 0x00, 0xFF, 0xF0};

    int32_t written = 0;

    app_buf.put_binary_data(binary, sizeof(binary), written);

    EXPECT_TRUE(!strcmp(buffer, "2184f4dc0100fff0"));
    EXPECT_TRUE(res_len == strlen("2184f4dc0100fff0"));
}

TEST_F(application_data_buffer_test, put_string_to_string) {
    char buffer[1024];
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, buffer, sizeof(buffer), &res_len);

    std::string test_string("Test string");

    app_buf.put_string(test_string);

    EXPECT_TRUE(!strcmp(buffer, test_string.c_str()));
    EXPECT_EQ(static_cast<size_t>(res_len), test_string.size());
}

TEST_F(application_data_buffer_test, put_string_to_wstring) {
    wchar_t buffer[1024];
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_WCHAR, buffer, sizeof(buffer), &res_len);

    std::string test_string("Test string");

    app_buf.put_string(test_string);
    EXPECT_TRUE(!wcscmp(buffer, L"Test string"));
}

TEST_F(application_data_buffer_test, put_string_to_long) {
    SQLINTEGER num_buf;
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_SIGNED_LONG, &num_buf, sizeof(num_buf), &res_len);

    app_buf.put_string("424242424");
    EXPECT_TRUE(num_buf == 424242424L);

    app_buf.put_string("-424242424");
    EXPECT_TRUE(num_buf == -424242424L);
}

TEST_F(application_data_buffer_test, put_string_to_tiny) {
    std::int8_t num_buf;
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_SIGNED_TINYINT, &num_buf, sizeof(num_buf), &res_len);

    app_buf.put_string("12");
    EXPECT_TRUE(num_buf == 12);

    app_buf.put_string("-12");
    EXPECT_TRUE(num_buf == -12);
}

TEST_F(application_data_buffer_test, put_string_to_float) {
    float num_buf;
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_FLOAT, &num_buf, sizeof(num_buf), &res_len);

    app_buf.put_string("12.21");
    EXPECT_FLOAT_EQ(num_buf, 12.21);

    app_buf.put_string("-12.21");
    EXPECT_FLOAT_EQ(num_buf, -12.21);
}

TEST_F(application_data_buffer_test, put_int_to_float) {
    float num_buf;
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_FLOAT, &num_buf, sizeof(num_buf), &res_len);

    app_buf.put_int8(5);
    EXPECT_FLOAT_EQ(num_buf, 5.0);

    app_buf.put_int8(-5);
    EXPECT_FLOAT_EQ(num_buf, -5.0);

    app_buf.put_int16(4242);
    EXPECT_FLOAT_EQ(num_buf, 4242.0);

    app_buf.put_int16(-4242);
    EXPECT_FLOAT_EQ(num_buf, -4242.0);

    app_buf.put_int32(1234567);
    EXPECT_FLOAT_EQ(num_buf, 1234567.0);

    app_buf.put_int32(-1234567);
    EXPECT_FLOAT_EQ(num_buf, -1234567.0);
}

TEST_F(application_data_buffer_test, put_float_to_short) {
    short num_buf;
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_SIGNED_SHORT, &num_buf, sizeof(num_buf), &res_len);

    app_buf.put_double(5.42);
    EXPECT_EQ(num_buf, 5);

    app_buf.put_double(-5.42);
    EXPECT_EQ(num_buf, -5.0);

    app_buf.put_float(42.99f);
    EXPECT_EQ(num_buf, 42);

    app_buf.put_float(-42.99f);
    EXPECT_EQ(num_buf, -42);
}

TEST_F(application_data_buffer_test, put_decimal_to_double) {
    double num_buf;
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_DOUBLE, &num_buf, sizeof(num_buf), &res_len);

    big_decimal decimal;

    EXPECT_FLOAT_EQ(static_cast<double>(decimal), 0.0);

    app_buf.put_decimal(decimal);
    EXPECT_FLOAT_EQ(num_buf, 0.0);

    std::int8_t mag1[] = {1, 0};

    decimal = {mag1, sizeof(mag1), 0, 1};

    app_buf.put_decimal(decimal);
    EXPECT_FLOAT_EQ(num_buf, 256.0);

    int8_t mag2[] = {2, 23};

    decimal = {mag2, sizeof(mag2), 1, -1};

    app_buf.put_decimal(decimal);
    EXPECT_FLOAT_EQ(num_buf, -53.5);
}

TEST_F(application_data_buffer_test, put_decimal_to_long) {
    SQLINTEGER num_buf;
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_SIGNED_LONG, &num_buf, sizeof(num_buf), &res_len);

    big_decimal decimal;

    app_buf.put_decimal(decimal);
    EXPECT_EQ(num_buf, 0);

    std::int8_t mag1[] = {1, 0};

    decimal = {mag1, sizeof(mag1), 0, 1};

    app_buf.put_decimal(decimal);
    EXPECT_EQ(num_buf, 256);

    std::int8_t mag2[] = {2, 23};

    decimal = {mag2, sizeof(mag2), 1, -1};

    app_buf.put_decimal(decimal);
    EXPECT_EQ(num_buf, -53);
}

TEST_F(application_data_buffer_test, put_decimal_to_string) {
    char str_buf[64];
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, &str_buf, sizeof(str_buf), &res_len);

    big_decimal decimal;

    app_buf.put_decimal(decimal);
    EXPECT_EQ(std::string(str_buf, res_len), "0");

    std::int8_t mag1[] = {1, 0};

    decimal = {mag1, sizeof(mag1), 0, 1};

    app_buf.put_decimal(decimal);
    EXPECT_EQ(std::string(str_buf, res_len), "256");

    std::int8_t mag2[] = {2, 23};

    decimal = {mag2, sizeof(mag2), 1, -1};

    app_buf.put_decimal(decimal);
    EXPECT_EQ(std::string(str_buf, res_len), "-53.5");
}

TEST_F(application_data_buffer_test, put_decimal_to_numeric) {
    SQL_NUMERIC_STRUCT buf;
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_NUMERIC, &buf, sizeof(buf), &res_len);

    big_decimal decimal;

    app_buf.put_decimal(decimal);
    EXPECT_EQ(1, buf.sign); // Positive
    EXPECT_EQ(0, buf.scale); // Scale is 0 by default according to specification
    EXPECT_EQ(1, buf.precision); // Precision is 1 for default constructed Decimal (0).

    for (unsigned char i : buf.val)
        EXPECT_EQ(0, i);

    // Trying to store 123.45 => 12345 => 0x3039 => [0x30, 0x39].
    std::uint8_t mag1[] = {0x30, 0x39};

    decimal = {reinterpret_cast<int8_t *>(mag1), sizeof(mag1), 2, 1};

    app_buf.put_decimal(decimal);
    EXPECT_EQ(1, buf.sign); // Positive
    EXPECT_EQ(0, buf.scale); // Scale is 0 by default according to specification
    EXPECT_EQ(3, buf.precision); // Precision is 3, as the scale is set to 0.

    // 123.45 => (scale=0) 123 => 0x7B => [0x7B].
    EXPECT_EQ(buf.val[0], 0x7B);

    for (int i = 1; i < SQL_MAX_NUMERIC_LEN; ++i)
        EXPECT_EQ(0, buf.val[i]);

    // Trying to store 12345.678 => 12345678 => 0xBC614E => [0xBC, 0x61, 0x4E].
    std::uint8_t mag2[] = {0xBC, 0x61, 0x4E};

    decimal = {reinterpret_cast<int8_t *>(mag2), sizeof(mag2), 3, -1};

    app_buf.put_decimal(decimal);
    EXPECT_EQ(0, buf.sign); // Negative
    EXPECT_EQ(0, buf.scale); // Scale is 0 by default according to specification
    EXPECT_EQ(5, buf.precision); // Precision is 5, as the scale is set to 0.

    // 12345.678 => (scale=0) 12345 => 0x3039 => [0x39, 0x30].
    EXPECT_EQ(buf.val[0], 0x39);
    EXPECT_EQ(buf.val[1], 0x30);

    for (int i = 2; i < SQL_MAX_NUMERIC_LEN; ++i)
        EXPECT_EQ(0, buf.val[i]);
}

TEST_F(application_data_buffer_test, put_date_to_string) {
    char str_buf[64];
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, &str_buf, sizeof(str_buf), &res_len);

    ignite_date date{1999, 2, 22};

    app_buf.put_date(date);

    EXPECT_EQ(std::string(str_buf, res_len - 1), std::string("1999-02-22"));
}

TEST_F(application_data_buffer_test, put_date_to_date) {
    SQL_DATE_STRUCT buf;
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TDATE, &buf, sizeof(buf), &res_len);

    ignite_date date{1984, 5, 27};

    app_buf.put_date(date);

    EXPECT_EQ(1984, buf.year);
    EXPECT_EQ(5, buf.month);
    EXPECT_EQ(27, buf.day);
}

TEST_F(application_data_buffer_test, put_date_to_timestamp) {
    SQL_TIMESTAMP_STRUCT buf;

    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TTIMESTAMP, &buf, sizeof(buf), &res_len);

    ignite_date date{1984, 5, 27};

    app_buf.put_date(date);

    EXPECT_EQ(1984, buf.year);
    EXPECT_EQ(5, buf.month);
    EXPECT_EQ(27, buf.day);
    EXPECT_EQ(0, buf.hour);
    EXPECT_EQ(0, buf.minute);
    EXPECT_EQ(0, buf.second);
    EXPECT_EQ(0, buf.fraction);
}

TEST_F(application_data_buffer_test, put_time_to_string) {
    char str_buf[64];
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, &str_buf, sizeof(str_buf), &res_len);

    ignite_time time{7, 15, 0};

    app_buf.put_time(time);

    EXPECT_EQ(std::string(str_buf, res_len - 1), std::string("07:15:00"));
}

TEST_F(application_data_buffer_test, put_time_to_time) {
    SQL_TIME_STRUCT buf;
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TTIME, &buf, sizeof(buf), &res_len);

    ignite_time time{23, 51, 1};

    app_buf.put_time(time);

    EXPECT_EQ(23, buf.hour);
    EXPECT_EQ(51, buf.minute);
    EXPECT_EQ(1, buf.second);
}

TEST_F(application_data_buffer_test, put_timestamp_to_string) {
    char str_buf[64];
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, &str_buf, sizeof(str_buf), &res_len);

    auto ts = make_timestamp(2018, 11, 1, 13, 45, 59, 346'598'326);

    app_buf.put_timestamp(ts);

    EXPECT_EQ(std::string(str_buf, res_len - 1), std::string("2018-11-01 13:45:59"));
}

TEST_F(application_data_buffer_test, put_timestamp_to_date) {
    SQL_DATE_STRUCT buf;
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TDATE, &buf, sizeof(buf), &res_len);

    auto ts = make_timestamp(2018, 11, 1, 13, 45, 59, 346'598'326);

    app_buf.put_timestamp(ts);

    EXPECT_EQ(2018, buf.year);
    EXPECT_EQ(11, buf.month);
    EXPECT_EQ(1, buf.day);
}

TEST_F(application_data_buffer_test, put_timestamp_to_time) {
    SQL_TIME_STRUCT buf;
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TTIME, &buf, sizeof(buf), &res_len);

    auto ts = make_timestamp(2018, 11, 1, 13, 45, 59, 346'598'326);

    app_buf.put_timestamp(ts);

    EXPECT_EQ(13, buf.hour);
    EXPECT_EQ(45, buf.minute);
    EXPECT_EQ(59, buf.second);
}

TEST_F(application_data_buffer_test, put_timestamp_to_timestamp) {
    SQL_TIMESTAMP_STRUCT buf;
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TTIMESTAMP, &buf, sizeof(buf), &res_len);

    auto ts = make_timestamp(2018, 11, 1, 13, 45, 59, 346'598'326);

    app_buf.put_timestamp(ts);

    EXPECT_EQ(2018, buf.year);
    EXPECT_EQ(11, buf.month);
    EXPECT_EQ(1, buf.day);
    EXPECT_EQ(13, buf.hour);
    EXPECT_EQ(45, buf.minute);
    EXPECT_EQ(59, buf.second);
    EXPECT_EQ(346'598'326, buf.fraction);
}

TEST_F(application_data_buffer_test, put_date_time_to_string) {
    char str_buf[64];
    SQLLEN res_len = 0;

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, &str_buf, sizeof(str_buf), &res_len);

    ignite_date_time date_time{{2018, 11, 1}, {13, 45, 59, 346'598'326}};

    app_buf.put_date_time(date_time);

    EXPECT_EQ(std::string(str_buf, res_len - 1), std::string("2018-11-01 13:45:59"));
}

TEST_F(application_data_buffer_test, put_date_time_to_date) {
    SQL_DATE_STRUCT buf;
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TDATE, &buf, sizeof(buf), &res_len);

    ignite_date_time date_time{{2018, 11, 1}, {13, 45, 59, 346'598'326}};

    app_buf.put_date_time(date_time);

    EXPECT_EQ(2018, buf.year);
    EXPECT_EQ(11, buf.month);
    EXPECT_EQ(1, buf.day);
}

TEST_F(application_data_buffer_test, put_date_time_to_time) {
    SQL_TIME_STRUCT buf;
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TTIME, &buf, sizeof(buf), &res_len);

    ignite_date_time date_time{{2018, 11, 1}, {13, 45, 59, 346'598'326}};

    app_buf.put_date_time(date_time);

    EXPECT_EQ(13, buf.hour);
    EXPECT_EQ(45, buf.minute);
    EXPECT_EQ(59, buf.second);
}

TEST_F(application_data_buffer_test, put_date_time_to_timestamp) {
    SQL_TIMESTAMP_STRUCT buf;
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TTIMESTAMP, &buf, sizeof(buf), &res_len);

    ignite_date_time date_time{{2018, 11, 1}, {13, 45, 59, 346'598'326}};

    app_buf.put_date_time(date_time);

    EXPECT_EQ(2018, buf.year);
    EXPECT_EQ(11, buf.month);
    EXPECT_EQ(1, buf.day);
    EXPECT_EQ(13, buf.hour);
    EXPECT_EQ(45, buf.minute);
    EXPECT_EQ(59, buf.second);
    EXPECT_EQ(346'598'326, buf.fraction);
}

TEST_F(application_data_buffer_test, get_uuid_from_string) {
    char buffer[] = "1da1ef8f-39ff-4d62-8b72-e8e9f3371801";
    SQLLEN res_len = sizeof(buffer) - 1;

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, buffer, sizeof(buffer) - 1, &res_len);

    uuid guid = app_buf.get_uuid();

    EXPECT_EQ(guid, uuid(0x1da1ef8f39ff4d62UL, 0x8b72e8e9f3371801UL));
}

TEST_F(application_data_buffer_test, get_string_from_long) {
    long num_buf = 42;
    SQLLEN res_len = sizeof(num_buf);

    application_data_buffer app_buf(odbc_native_type::AI_SIGNED_LONG, &num_buf, res_len, &res_len);

    std::string res = app_buf.get_string(32);

    EXPECT_TRUE(res == "42");

    num_buf = -77;

    res = app_buf.get_string(32);

    EXPECT_TRUE(res == "-77");
}

TEST_F(application_data_buffer_test, get_string_from_double) {
    double num_buf = 43.36;
    SQLLEN res_len = sizeof(num_buf);

    application_data_buffer app_buf(odbc_native_type::AI_DOUBLE, &num_buf, res_len, &res_len);

    std::string res = app_buf.get_string(32);

    EXPECT_EQ(res, "43.36");

    num_buf = -58.91;

    res = app_buf.get_string(32);

    EXPECT_EQ(res, "-58.91");
}

TEST_F(application_data_buffer_test, get_string_from_string) {
    char buf[] = "Some data 32d2d5hs";
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, &buf, res_len, &res_len);

    std::string res = app_buf.get_string(res_len);

    EXPECT_EQ(res.compare(buf), 0);
}

TEST_F(application_data_buffer_test, get_float_from_ushort) {
    unsigned short num_buf = 7162;
    SQLLEN res_len = sizeof(num_buf);

    application_data_buffer app_buf(odbc_native_type::AI_UNSIGNED_SHORT, &num_buf, res_len, &res_len);

    float res_float = app_buf.get_float();

    EXPECT_FLOAT_EQ(res_float, 7162.0f);

    double res_double = app_buf.get_double();

    EXPECT_DOUBLE_EQ(res_double, 7162.0);
}

TEST_F(application_data_buffer_test, get_float_from_string) {
    char buf[] = "28.562";
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, &buf, res_len, &res_len);

    float res_float = app_buf.get_float();

    EXPECT_FLOAT_EQ(res_float, 28.562f);

    double res_double = app_buf.get_double();

    EXPECT_DOUBLE_EQ(res_double, 28.562);
}

TEST_F(application_data_buffer_test, get_float_from_float) {
    float buf = 207.49f;
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_FLOAT, &buf, res_len, &res_len);

    float res_float = app_buf.get_float();

    EXPECT_FLOAT_EQ(res_float, 207.49f);

    double res_double = app_buf.get_double();

    EXPECT_FLOAT_EQ(res_double, 207.49);
}

TEST_F(application_data_buffer_test, get_float_from_double) {
    double buf = 893.162;
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_DOUBLE, &buf, res_len, &res_len);

    float res_float = app_buf.get_float();

    EXPECT_FLOAT_EQ(res_float, 893.162f);

    double res_double = app_buf.get_double();

    EXPECT_DOUBLE_EQ(res_double, 893.162);
}

TEST_F(application_data_buffer_test, get_int_from_string) {
    char buf[] = "39";
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, &buf, res_len, &res_len);

    int64_t res_int64 = app_buf.get_int64();

    EXPECT_EQ(res_int64, 39);

    int32_t res_int32 = app_buf.get_int32();

    EXPECT_EQ(res_int32, 39);

    int16_t res_int16 = app_buf.get_int16();

    EXPECT_EQ(res_int16, 39);

    int8_t res_int8 = app_buf.get_int8();

    EXPECT_EQ(res_int8, 39);
}

TEST_F(application_data_buffer_test, get_int_from_float) {
    float buf = -107.49f;
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_FLOAT, &buf, res_len, &res_len);

    int64_t res_int64 = app_buf.get_int64();

    EXPECT_EQ(res_int64, -107);

    int32_t res_int32 = app_buf.get_int32();

    EXPECT_EQ(res_int32, -107);

    int16_t res_int16 = app_buf.get_int16();

    EXPECT_EQ(res_int16, -107);

    int8_t res_int8 = app_buf.get_int8();

    EXPECT_EQ(res_int8, -107);
}

TEST_F(application_data_buffer_test, get_int_from_double) {
    double buf = 42.97f;
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_DOUBLE, &buf, res_len, &res_len);

    int64_t res_int64 = app_buf.get_int64();

    EXPECT_EQ(res_int64, 42);

    int32_t res_int32 = app_buf.get_int32();

    EXPECT_EQ(res_int32, 42);

    int16_t res_int16 = app_buf.get_int16();

    EXPECT_EQ(res_int16, 42);

    int8_t res_int8 = app_buf.get_int8();

    EXPECT_EQ(res_int8, 42);
}

TEST_F(application_data_buffer_test, get_int_from_bigint) {
    uint64_t buf = 19;
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_UNSIGNED_BIGINT, &buf, res_len, &res_len);

    int64_t res_int64 = app_buf.get_int64();

    EXPECT_EQ(res_int64, 19);

    int32_t res_int32 = app_buf.get_int32();

    EXPECT_EQ(res_int32, 19);

    int16_t res_int16 = app_buf.get_int16();

    EXPECT_EQ(res_int16, 19);

    int8_t res_int8 = app_buf.get_int8();

    EXPECT_EQ(res_int8, 19);
}

TEST_F(application_data_buffer_test, get_int_with_offset) {
    struct test_struct {
        uint64_t val;
        SQLLEN res_len;
    };

    test_struct buf[2] = {{12, sizeof(uint64_t)}, {42, sizeof(uint64_t)}};

    application_data_buffer app_buf(
        odbc_native_type::AI_UNSIGNED_BIGINT, &buf[0].val, sizeof(buf[0].val), &buf[0].res_len);

    int64_t val = app_buf.get_int64();

    EXPECT_EQ(val, 12);

    app_buf.set_byte_offset(sizeof(test_struct));

    val = app_buf.get_int64();

    EXPECT_EQ(val, 42);

    app_buf.set_byte_offset(0);

    val = app_buf.get_int64();

    EXPECT_EQ(val, 12);
}

TEST_F(application_data_buffer_test, set_string_with_offset) {
    struct test_struct {
        char val[64];
        SQLLEN res_len;
    };

    test_struct buf[2] = {{"", 0}, {"", 0}};

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, &buf[0].val, sizeof(buf[0].val), &buf[0].res_len);

    app_buf.put_string("Hello Ignite!");

    std::string res(buf[0].val, buf[0].res_len);

    EXPECT_EQ(buf[0].res_len, strlen("Hello Ignite!"));
    EXPECT_EQ(res, "Hello Ignite!");
    EXPECT_EQ(res.size(), strlen("Hello Ignite!"));

    app_buf.set_byte_offset(sizeof(test_struct));

    app_buf.put_string("Hello with offset!");

    res.assign(buf[0].val, buf[0].res_len);

    EXPECT_EQ(res, "Hello Ignite!");
    EXPECT_EQ(res.size(), strlen("Hello Ignite!"));
    EXPECT_EQ(buf[0].res_len, strlen("Hello Ignite!"));

    res.assign(buf[1].val, buf[1].res_len);

    EXPECT_EQ(res, "Hello with offset!");
    EXPECT_EQ(res.size(), strlen("Hello with offset!"));
    EXPECT_EQ(buf[1].res_len, strlen("Hello with offset!"));
}

TEST_F(application_data_buffer_test, get_date_from_string) {
    char buf[] = "1999-02-22";
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, &buf[0], sizeof(buf), &res_len);

    auto date = app_buf.get_date();

    EXPECT_EQ(1999, date.get_year());
    EXPECT_EQ(2, date.get_month());
    EXPECT_EQ(22, date.get_day_of_month());
}

TEST_F(application_data_buffer_test, get_time_from_string) {
    char buf[] = "17:5:59";
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, &buf[0], sizeof(buf), &res_len);

    auto time = app_buf.get_time();

    EXPECT_EQ(17, time.get_hour());
    EXPECT_EQ(5, time.get_minute());
    EXPECT_EQ(59, time.get_second());
}

TEST_F(application_data_buffer_test, get_timestamp_from_string) {
    char buf[] = "2018-11-01 13:45:59";
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, &buf[0], sizeof(buf), &res_len);

    auto ts = app_buf.get_timestamp();

    auto expected = make_timestamp(2018, 11, 1, 13, 45, 59, 0);
    EXPECT_EQ(expected.get_epoch_second(), ts.get_epoch_second());
    EXPECT_EQ(expected.get_nano(), ts.get_nano());
}

TEST_F(application_data_buffer_test, get_date_time_from_string) {
    char buf[] = "2018-11-01 13:45:59.123456789";
    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_CHAR, &buf[0], sizeof(buf), &res_len);

    auto date_time = app_buf.get_date_time();

    EXPECT_EQ(2018, date_time.get_year());
    EXPECT_EQ(11, date_time.get_month());
    EXPECT_EQ(1, date_time.get_day_of_month());
    EXPECT_EQ(13, date_time.get_hour());
    EXPECT_EQ(45, date_time.get_minute());
    EXPECT_EQ(59, date_time.get_second());
    EXPECT_EQ(123456789, date_time.get_nano());
}

TEST_F(application_data_buffer_test, get_date_from_date) {
    SQL_DATE_STRUCT buf;

    buf.year = 1984;
    buf.month = 5;
    buf.day = 27;

    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TDATE, &buf, sizeof(buf), &res_len);

    auto date = app_buf.get_date();

    EXPECT_EQ(1984, date.get_year());
    EXPECT_EQ(5, date.get_month());
    EXPECT_EQ(27, date.get_day_of_month());
}

TEST_F(application_data_buffer_test, get_timestamp_from_date) {
    SQL_DATE_STRUCT buf;

    buf.year = 1984;
    buf.month = 5;
    buf.day = 27;

    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TDATE, &buf, sizeof(buf), &res_len);

    auto ts = app_buf.get_timestamp();

    tm tm_time{};
    tm_time.tm_year = 84;
    tm_time.tm_mon = 4;
    tm_time.tm_mday = 27;

    EXPECT_EQ(mktime(&tm_time), ts.get_epoch_second());
    EXPECT_EQ(0, ts.get_nano());
}

TEST_F(application_data_buffer_test, get_date_time_from_date) {
    SQL_DATE_STRUCT buf;

    buf.year = 1984;
    buf.month = 5;
    buf.day = 27;

    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TDATE, &buf, sizeof(buf), &res_len);

    auto date_time = app_buf.get_date_time();

    EXPECT_EQ(1984, date_time.get_year());
    EXPECT_EQ(5, date_time.get_month());
    EXPECT_EQ(27, date_time.get_day_of_month());
    EXPECT_EQ(0, date_time.get_nano());
}

TEST_F(application_data_buffer_test, get_time_from_time) {
    SQL_TIME_STRUCT buf;

    buf.hour = 6;
    buf.minute = 34;
    buf.second = 51;

    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TTIME, &buf, sizeof(buf), &res_len);

    auto time = app_buf.get_time();

    EXPECT_EQ(6, time.get_hour());
    EXPECT_EQ(34, time.get_minute());
    EXPECT_EQ(51, time.get_second());
    EXPECT_EQ(0, time.get_nano());
}

TEST_F(application_data_buffer_test, get_timestamp_from_timestamp) {
    SQL_TIMESTAMP_STRUCT buf;

    buf.year = 2004;
    buf.month = 8;
    buf.day = 14;
    buf.hour = 6;
    buf.minute = 34;
    buf.second = 51;
    buf.fraction = 573948623;

    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TTIMESTAMP, &buf, sizeof(buf), &res_len);

    auto ts = app_buf.get_timestamp();

    tm tm_time{};
    tm_time.tm_year = 104;
    tm_time.tm_mon = 7;
    tm_time.tm_mday = 14;
    tm_time.tm_hour = 6;
    tm_time.tm_min = 34;
    tm_time.tm_sec = 51;

    EXPECT_EQ(mktime(&tm_time), ts.get_epoch_second());
    EXPECT_EQ(573948623, ts.get_nano());
}

TEST_F(application_data_buffer_test, get_date_time_from_timestamp) {
    SQL_TIMESTAMP_STRUCT buf;

    buf.year = 2004;
    buf.month = 8;
    buf.day = 14;
    buf.hour = 6;
    buf.minute = 34;
    buf.second = 51;
    buf.fraction = 573948623;

    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TTIMESTAMP, &buf, sizeof(buf), &res_len);

    auto date_time = app_buf.get_date_time();

    EXPECT_EQ(2004, date_time.get_year());
    EXPECT_EQ(8, date_time.get_month());
    EXPECT_EQ(14, date_time.get_day_of_month());
    EXPECT_EQ(6, date_time.get_hour());
    EXPECT_EQ(34, date_time.get_minute());
    EXPECT_EQ(51, date_time.get_second());
    EXPECT_EQ(573948623, date_time.get_nano());
}

TEST_F(application_data_buffer_test, get_date_from_timestamp) {
    SQL_TIMESTAMP_STRUCT buf;

    buf.year = 2004;
    buf.month = 8;
    buf.day = 14;
    buf.hour = 6;
    buf.minute = 34;
    buf.second = 51;
    buf.fraction = 573948623;

    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TTIMESTAMP, &buf, sizeof(buf), &res_len);

    auto date = app_buf.get_date();

    EXPECT_EQ(2004, date.get_year());
    EXPECT_EQ(8, date.get_month());
    EXPECT_EQ(14, date.get_day_of_month());
}

TEST_F(application_data_buffer_test, get_time_from_timestamp) {
    SQL_TIMESTAMP_STRUCT buf;

    buf.year = 2004;
    buf.month = 8;
    buf.day = 14;
    buf.hour = 6;
    buf.minute = 34;
    buf.second = 51;
    buf.fraction = 573948623;

    SQLLEN res_len = sizeof(buf);

    application_data_buffer app_buf(odbc_native_type::AI_TTIMESTAMP, &buf, sizeof(buf), &res_len);

    auto time = app_buf.get_time();

    EXPECT_EQ(6, time.get_hour());
    EXPECT_EQ(34, time.get_minute());
    EXPECT_EQ(51, time.get_second());
    EXPECT_EQ(573948623, time.get_nano());
}
