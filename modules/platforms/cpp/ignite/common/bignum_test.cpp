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

#include "big_decimal.h"
#include "big_integer.h"

#include <gtest/gtest.h>

using namespace ignite;

template<typename T>
void CheckOutputSimple(int64_t val) {
    T dec(val);

    std::stringstream ss1;
    std::stringstream ss2;

    ss1 << val;
    ss2 << dec;

    EXPECT_EQ(ss1.str(), ss2.str());
}

template<typename T>
void CheckInputOutput(const std::string &val) {
    T dec;
    std::string res;

    std::stringstream ss1;
    ss1 << val;
    ss1 >> dec;

    std::stringstream ss2;
    ss2 << dec;
    res = ss2.str();

    EXPECT_EQ(val, res);
}

template<typename T>
void CheckOutputInput(const T &val) {
    T dec;
    std::stringstream ss;
    ss << val;
    ss >> dec;

    EXPECT_EQ(val, dec);
}

void CheckDoubleCast(double val) {
    big_decimal dec1(val);
    big_decimal dec2;

    dec2.assign_double(val);

    EXPECT_NEAR(val, dec1.to_double(), 1E-10);
    EXPECT_NEAR(val, dec2.to_double(), 1E-10);
}

TEST(bignum, TestMultiplyBigIntegerArguments) {
    big_integer bigInt(12345);

    big_integer res;

    // 152399025
    bigInt.multiply(big_integer(12345), res);

    {
        const big_integer::mag_array_view mag = res.get_magnitude();

        EXPECT_EQ(mag.size(), 1);

        EXPECT_EQ(mag[0], 152399025UL);
    }

    // 152399025
    bigInt.assign_int64(12345);
    bigInt.multiply(bigInt, res);

    {
        const big_integer::mag_array_view mag = res.get_magnitude();

        EXPECT_EQ(mag.size(), 1);

        EXPECT_EQ(mag[0], 152399025UL);
    }

    // 152399025
    bigInt.assign_int64(12345);
    bigInt.multiply(big_integer(12345), bigInt);

    {
        const big_integer::mag_array_view mag = bigInt.get_magnitude();

        EXPECT_EQ(mag.size(), 1);

        EXPECT_EQ(mag[0], 152399025UL);
    }

    // 152399025
    bigInt.assign_int64(12345);
    bigInt.multiply(bigInt, bigInt);

    {
        const big_integer::mag_array_view mag = bigInt.get_magnitude();

        EXPECT_EQ(mag.size(), 1);

        EXPECT_EQ(mag[0], 152399025UL);
    }
}

TEST(bignum, TestMultiplyBigIntegerBigger) {
    big_integer bigInt(12345);

    // 152399025
    bigInt.multiply(bigInt, bigInt);

    // 3539537889086624823140625
    // 0002 ED86  BBC3 30D1  DDC6 6111
    big_integer buf = bigInt;
    bigInt.multiply(buf, bigInt);
    bigInt.multiply(buf, bigInt);

    {
        const big_integer::mag_array_view mag = bigInt.get_magnitude();

        EXPECT_EQ(mag.size(), 3);

        EXPECT_EQ(mag[0], 0xDDC66111);
        EXPECT_EQ(mag[1], 0xBBC330D1);
        EXPECT_EQ(mag[2], 0x0002ED86);
    }

    // 2698355789040138398691723863616167551412718750 ==
    // 0078 FF9A  F760 4E12  4A1F 3179  D038 D455  630F CC9E
    bigInt.multiply(big_integer(32546826734), bigInt);
    bigInt.multiply(big_integer(23423079641), bigInt);

    {
        const big_integer::mag_array_view mag = bigInt.get_magnitude();

        EXPECT_EQ(mag.size(), 5);

        EXPECT_EQ(mag[0], 0x630FCC9E);
        EXPECT_EQ(mag[1], 0xD038D455);
        EXPECT_EQ(mag[2], 0x4A1F3179);
        EXPECT_EQ(mag[3], 0xF7604E12);
        EXPECT_EQ(mag[4], 0x0078FF9A);
    }
}

TEST(bignum, TestPowBigInteger) {
    big_integer bigInt(12345);

    {
        const big_integer::mag_array_view mag = bigInt.get_magnitude();

        EXPECT_EQ(mag.size(), 1);

        EXPECT_EQ(mag[0], 12345);
    }

    // 152399025
    bigInt.pow(2);

    {
        const big_integer::mag_array_view mag = bigInt.get_magnitude();

        EXPECT_EQ(mag.size(), 1);

        EXPECT_EQ(mag[0], 152399025UL);
    }

    // 3539537889086624823140625
    // 0002 ED86  BBC3 30D1  DDC6 6111
    bigInt.pow(3);

    {
        const big_integer::mag_array_view mag = bigInt.get_magnitude();

        EXPECT_EQ(mag.size(), 3);

        EXPECT_EQ(mag[0], 0xDDC66111);
        EXPECT_EQ(mag[1], 0xBBC330D1);
        EXPECT_EQ(mag[2], 0x0002ED86);
    }

    // 3086495556566025694024226933269611093366465997140345415945924110519533775
    // 2241867322136254278528975546698722592953892009291022792452635153187272387
    // 9105398830363346664660724134489229239181447334384883937966927158758068117
    // 094808258116245269775390625
    //
    //                                              0000 B4D0  1355 772E
    //  C174 C5F3  B840 74ED  6A54 B544  48E1 E308  6A80 6050  7D37 A56F
    //  54E6 FF91  13FF 7B0A  455C F649  F4CD 37D0  C5B0 0507  1BFD 9083
    //  8F13 08B4  D962 08FC  FBC0 B5AB  F9F9 06C9  94B3 9715  8C43 C94F
    //  4891 09E5  57AA 66C9  A4F4 3494  A938 89FE  87AF 9056  7D90 17A1
    bigInt.pow(10);

    {
        const big_integer::mag_array_view &mag = bigInt.get_magnitude();

        EXPECT_EQ(mag.size(), 26);

        EXPECT_EQ(mag[0], 0x7D9017A1);
        EXPECT_EQ(mag[1], 0x87AF9056);
        EXPECT_EQ(mag[2], 0xA93889FE);
        EXPECT_EQ(mag[3], 0xA4F43494);
        EXPECT_EQ(mag[4], 0x57AA66C9);
        EXPECT_EQ(mag[5], 0x489109E5);
        EXPECT_EQ(mag[6], 0x8C43C94F);
        EXPECT_EQ(mag[7], 0x94B39715);
        EXPECT_EQ(mag[8], 0xF9F906C9);
        EXPECT_EQ(mag[9], 0xFBC0B5AB);
        EXPECT_EQ(mag[10], 0xD96208FC);
        EXPECT_EQ(mag[11], 0x8F1308B4);
        EXPECT_EQ(mag[12], 0x1BFD9083);
        EXPECT_EQ(mag[13], 0xC5B00507);
        EXPECT_EQ(mag[14], 0xF4CD37D0);
        EXPECT_EQ(mag[15], 0x455CF649);
        EXPECT_EQ(mag[16], 0x13FF7B0A);
        EXPECT_EQ(mag[17], 0x54E6FF91);
        EXPECT_EQ(mag[18], 0x7D37A56F);
        EXPECT_EQ(mag[19], 0x6A806050);
        EXPECT_EQ(mag[20], 0x48E1E308);
        EXPECT_EQ(mag[21], 0x6A54B544);
        EXPECT_EQ(mag[22], 0xB84074ED);
        EXPECT_EQ(mag[23], 0xC174C5F3);
        EXPECT_EQ(mag[24], 0x1355772E);
        EXPECT_EQ(mag[25], 0x0000B4D0);
    }

    bigInt.assign_int64(-1);

    bigInt.pow(57298735);
    EXPECT_EQ(bigInt.to_int64(), -1);

    bigInt.pow(325347312);
    EXPECT_EQ(bigInt.to_int64(), 1);

    bigInt.assign_int64(2);

    bigInt.pow(10);
    EXPECT_EQ(bigInt.to_int64(), 1024);

    bigInt.assign_int64(-2);

    bigInt.pow(10);
    EXPECT_EQ(bigInt.to_int64(), 1024);

    bigInt.assign_int64(2);

    bigInt.pow(11);
    EXPECT_EQ(bigInt.to_int64(), 2048);

    bigInt.assign_int64(-2);

    bigInt.pow(11);
    EXPECT_EQ(bigInt.to_int64(), -2048);
}

TEST(bignum, TestMultiplyDivideSimple) {
    big_integer val;
    big_integer res;
    big_integer rem;

    val.assign_int64(23225462820950625L);

    // 23225462820 and 950625
    big_integer bi1;
    big_integer bi2;
    val.divide(big_integer(1000000), bi1, bi2);

    // 23225 and 462820
    big_integer bi3;
    big_integer bi4;
    bi1.divide(big_integer(1000000), bi3, bi4);

    EXPECT_EQ(bi2.to_int64(), 950625L);
    EXPECT_EQ(bi3.to_int64(), 23225L);
    EXPECT_EQ(bi4.to_int64(), 462820L);

    big_integer(0).divide(big_integer(1), res, rem);

    EXPECT_EQ(res, big_integer(0));
    EXPECT_EQ(rem, big_integer(0));

    big_integer(0).divide(big_integer(100), res, rem);

    EXPECT_EQ(res, big_integer(0));
    EXPECT_EQ(rem, big_integer(0));

    big_integer(0).divide(big_integer(-1), res, rem);

    EXPECT_EQ(res, big_integer(0));
    EXPECT_EQ(rem, big_integer(0));

    big_integer(0).divide(big_integer(-100), res, rem);

    EXPECT_EQ(res, big_integer(0));
    EXPECT_EQ(rem, big_integer(0));

    big_integer(1).divide(big_integer(1), res, rem);

    EXPECT_EQ(res, big_integer(1));
    EXPECT_EQ(rem, big_integer(0));

    big_integer(10).divide(big_integer(1), res, rem);

    EXPECT_EQ(res, big_integer(10));
    EXPECT_EQ(rem, big_integer(0));

    big_integer(-1).divide(big_integer(1), res, rem);

    EXPECT_EQ(res, big_integer(-1));
    EXPECT_EQ(rem, big_integer(0));

    big_integer(-10).divide(big_integer(1), res, rem);

    EXPECT_EQ(res, big_integer(-10));
    EXPECT_EQ(rem, big_integer(0));

    big_integer(1).divide(big_integer(-1), res, rem);

    EXPECT_EQ(res, big_integer(-1));
    EXPECT_EQ(rem, big_integer(0));

    big_integer(10).divide(big_integer(-1), res, rem);

    EXPECT_EQ(res, big_integer(-10));
    EXPECT_EQ(rem, big_integer(0));

    big_integer(-1).divide(big_integer(-1), res, rem);

    EXPECT_EQ(res, big_integer(1));
    EXPECT_EQ(rem, big_integer(0));

    big_integer(-10).divide(big_integer(-1), res, rem);

    EXPECT_EQ(res, big_integer(10));
    EXPECT_EQ(rem, big_integer(0));

    big_integer(123456789).divide(big_integer(1000), res);
    EXPECT_EQ(res, big_integer(123456));

    val.assign_int64(79823695862);
    val.divide(val, res);

    EXPECT_EQ(res, big_integer(1));

    val.assign_int64(28658345673);
    val.divide(val, val);

    EXPECT_EQ(val, big_integer(1));

    val.assign_int64(-97598673406);
    val.divide(val, res, val);

    EXPECT_EQ(res, big_integer(1));
    EXPECT_EQ(val, big_integer(0));

    val.assign_int64(1);
    val.divide(val, res, val);

    EXPECT_EQ(res, big_integer(1));
    EXPECT_EQ(val, big_integer(0));
}

TEST(bignum, TestAddSimple) {
    big_integer val;
    big_integer res;

    val.assign_int64(0);
    EXPECT_EQ(val.to_int64(), 0L);

    val.add(big_integer(1), res);
    EXPECT_EQ(res.to_int64(), 1L);

    // random
    val.assign_int64(2463541195749558141L);
    val.add(big_integer(3098194482677853036L), res);

    EXPECT_EQ(res.to_int64(), 5561735678427411177L);

    val.assign_int64(0);
    val.add(big_integer(-1), res);

    EXPECT_EQ(res.to_int64(), -1L);

    // random negative
    val.assign_int64(-4032357373991925161L);
    val.add(big_integer(-233378388862503951L), res);

    EXPECT_EQ(res.to_int64(), -4265735762854429112L);
}

TEST(bignum, TestSubtractSimple) {
    big_integer val;
    big_integer res;

    val.assign_int64(0);

    val.subtract(big_integer(1), res);
    EXPECT_EQ(res.to_int64(), -1L);

    // random
    val.assign_int64(5688815843208686889L);
    val.subtract(big_integer(3900474041211631169L), res);

    EXPECT_EQ(res.to_int64(), 1788341801997055720L);

    val.assign_int64(0);
    val.add(big_integer(-1), res);

    EXPECT_EQ(res.to_int64(), -1L);

    // random negative
    val.assign_int64(-3456647618045976636L);
    val.add(big_integer(-4426861903511900512L), res);

    EXPECT_EQ(res.to_int64(), -7883509521557877148L);
}

TEST(bignum, TestDivideBigger) {
    big_integer res;
    big_integer rem;

    big_integer("4790467782742318458842833081").divide(big_integer(1000000000000000000L), res, rem);

    EXPECT_EQ(res.to_int64(), 4790467782L);
    EXPECT_EQ(rem.to_int64(), 742318458842833081L);

    big_integer("4790467782742318458842833081").divide(big_integer("10000000000000000000"), res, rem);

    EXPECT_EQ(res.to_int64(), 479046778L);
    EXPECT_EQ(rem.to_int64(), 2742318458842833081L);

    big_integer(
        "328569986734256745892025106351608546013457217305539845689265945043650274304152384502658961485730864386")
        .divide(big_integer("759823640567289574925305534862590863490856903465"), res, rem);

    EXPECT_EQ(res, big_integer("432429275942170114314334535709873138296890293268042448"));
    EXPECT_EQ(rem, big_integer("725289323707320757244048053769339286218272582066"));

    big_integer("5789420569340865906230645903456092386459628364580763804659834658960883465807263084659832648768603645")
        .divide(big_integer("-29064503640646565660609983646665763458768340596340586"), res, rem);

    EXPECT_EQ(res, big_integer("-199192136253942064949205579447876757418653967046"));
    EXPECT_EQ(rem, big_integer("9693519879390725820633207073869515731754969332274689"));

    big_integer(
        "-107519074510758034695616045096493659264398569023607895679428769875976987594876903458769799098378994985"
        "793874569869348579")
        .divide(
            big_integer("197290846190263940610876503491586943095983984894898998999751636576150263056012501"), res, rem);

    EXPECT_EQ(res, big_integer("-544977512069001243499439196429495600701"));
    EXPECT_EQ(rem, big_integer("-66382358009926062210728796777352226675944219851838448875365359123421443108985378"));

    big_integer("9739565432896546050040656034658762836457836886868678345021405632645902354063045608267340568346582")
        .divide(big_integer("8263050146508634250640862503465899340625908694088569038"), res);

    EXPECT_EQ(res, big_integer("1178688893351540421358600789386475098289416"));
}

TEST(bignum, TestOutputSimpleBigInteger) {
    CheckOutputSimple<big_integer>(0);

    CheckOutputSimple<big_integer>(1);
    CheckOutputSimple<big_integer>(9);
    CheckOutputSimple<big_integer>(10);
    CheckOutputSimple<big_integer>(11);
    CheckOutputSimple<big_integer>(19);
    CheckOutputSimple<big_integer>(123);
    CheckOutputSimple<big_integer>(1234);
    CheckOutputSimple<big_integer>(12345);
    CheckOutputSimple<big_integer>(123456);
    CheckOutputSimple<big_integer>(1234567);
    CheckOutputSimple<big_integer>(12345678);
    CheckOutputSimple<big_integer>(123456789);
    CheckOutputSimple<big_integer>(1234567890);
    CheckOutputSimple<big_integer>(12345678909);
    CheckOutputSimple<big_integer>(123456789098);
    CheckOutputSimple<big_integer>(1234567890987);
    CheckOutputSimple<big_integer>(12345678909876);
    CheckOutputSimple<big_integer>(123456789098765);
    CheckOutputSimple<big_integer>(1234567890987654);
    CheckOutputSimple<big_integer>(12345678909876543);
    CheckOutputSimple<big_integer>(123456789098765432);
    CheckOutputSimple<big_integer>(1234567890987654321);
    CheckOutputSimple<big_integer>(999999999999999999L);
    CheckOutputSimple<big_integer>(999999999099999999L);
    CheckOutputSimple<big_integer>(1000000000000000000L);
    CheckOutputSimple<big_integer>(1000000000000000001L);
    CheckOutputSimple<big_integer>(1000000005000000000L);
    CheckOutputSimple<big_integer>(INT64_MAX);

    CheckOutputSimple<big_integer>(-1);
    CheckOutputSimple<big_integer>(-9);
    CheckOutputSimple<big_integer>(-10);
    CheckOutputSimple<big_integer>(-11);
    CheckOutputSimple<big_integer>(-19);
    CheckOutputSimple<big_integer>(-123);
    CheckOutputSimple<big_integer>(-1234);
    CheckOutputSimple<big_integer>(-12345);
    CheckOutputSimple<big_integer>(-123456);
    CheckOutputSimple<big_integer>(-1234567);
    CheckOutputSimple<big_integer>(-12345678);
    CheckOutputSimple<big_integer>(-123456789);
    CheckOutputSimple<big_integer>(-1234567890);
    CheckOutputSimple<big_integer>(-12345678909);
    CheckOutputSimple<big_integer>(-123456789098);
    CheckOutputSimple<big_integer>(-1234567890987);
    CheckOutputSimple<big_integer>(-12345678909876);
    CheckOutputSimple<big_integer>(-123456789098765);
    CheckOutputSimple<big_integer>(-1234567890987654);
    CheckOutputSimple<big_integer>(-12345678909876543);
    CheckOutputSimple<big_integer>(-123456789098765432);
    CheckOutputSimple<big_integer>(-1234567890987654321);
    CheckOutputSimple<big_integer>(-999999999999999999L);
    CheckOutputSimple<big_integer>(-999999999999999999L);
    CheckOutputSimple<big_integer>(-1000000000000000000L);
    CheckOutputSimple<big_integer>(-1000000000000000001L);
    CheckOutputSimple<big_integer>(-1000000000000000000L);
    CheckOutputSimple<big_integer>(INT64_MIN);
}

TEST(bignum, TestOutputSimpleDecimal) {
    CheckOutputSimple<big_decimal>(0);

    CheckOutputSimple<big_decimal>(1);
    CheckOutputSimple<big_decimal>(9);
    CheckOutputSimple<big_decimal>(10);
    CheckOutputSimple<big_decimal>(11);
    CheckOutputSimple<big_decimal>(19);
    CheckOutputSimple<big_decimal>(123);
    CheckOutputSimple<big_decimal>(1234);
    CheckOutputSimple<big_decimal>(12345);
    CheckOutputSimple<big_decimal>(123456);
    CheckOutputSimple<big_decimal>(1234567);
    CheckOutputSimple<big_decimal>(12345678);
    CheckOutputSimple<big_decimal>(123456789);
    CheckOutputSimple<big_decimal>(1234567890);
    CheckOutputSimple<big_decimal>(12345678909);
    CheckOutputSimple<big_decimal>(123456789098);
    CheckOutputSimple<big_decimal>(1234567890987);
    CheckOutputSimple<big_decimal>(12345678909876);
    CheckOutputSimple<big_decimal>(123456789098765);
    CheckOutputSimple<big_decimal>(1234567890987654);
    CheckOutputSimple<big_decimal>(12345678909876543);
    CheckOutputSimple<big_decimal>(123456789098765432);
    CheckOutputSimple<big_decimal>(1234567890987654321);
    CheckOutputSimple<big_decimal>(999999999999999999L);
    CheckOutputSimple<big_decimal>(999999999099999999L);
    CheckOutputSimple<big_decimal>(1000000000000000000L);
    CheckOutputSimple<big_decimal>(1000000000000000001L);
    CheckOutputSimple<big_decimal>(1000000005000000000L);
    CheckOutputSimple<big_decimal>(INT64_MAX);

    CheckOutputSimple<big_decimal>(-1);
    CheckOutputSimple<big_decimal>(-9);
    CheckOutputSimple<big_decimal>(-10);
    CheckOutputSimple<big_decimal>(-11);
    CheckOutputSimple<big_decimal>(-19);
    CheckOutputSimple<big_decimal>(-123);
    CheckOutputSimple<big_decimal>(-1234);
    CheckOutputSimple<big_decimal>(-12345);
    CheckOutputSimple<big_decimal>(-123456);
    CheckOutputSimple<big_decimal>(-1234567);
    CheckOutputSimple<big_decimal>(-12345678);
    CheckOutputSimple<big_decimal>(-123456789);
    CheckOutputSimple<big_decimal>(-1234567890);
    CheckOutputSimple<big_decimal>(-12345678909);
    CheckOutputSimple<big_decimal>(-123456789098);
    CheckOutputSimple<big_decimal>(-1234567890987);
    CheckOutputSimple<big_decimal>(-12345678909876);
    CheckOutputSimple<big_decimal>(-123456789098765);
    CheckOutputSimple<big_decimal>(-1234567890987654);
    CheckOutputSimple<big_decimal>(-12345678909876543);
    CheckOutputSimple<big_decimal>(-123456789098765432);
    CheckOutputSimple<big_decimal>(-1234567890987654321);
    CheckOutputSimple<big_decimal>(-999999999999999999L);
    CheckOutputSimple<big_decimal>(-999999999099999999L);
    CheckOutputSimple<big_decimal>(-1000000000000000000L);
    CheckOutputSimple<big_decimal>(-1000000000000000001L);
    CheckOutputSimple<big_decimal>(-1000000005000000000L);
    CheckOutputSimple<big_decimal>(INT64_MIN);
}

TEST(bignum, TestInputOutputSimpleBigInteger) {
    CheckInputOutput<big_integer>("0");
    CheckInputOutput<big_integer>("1");
    CheckInputOutput<big_integer>("2");
    CheckInputOutput<big_integer>("9");
    CheckInputOutput<big_integer>("10");
    CheckInputOutput<big_integer>("1123");
    CheckInputOutput<big_integer>("64539472569345602304");
    CheckInputOutput<big_integer>("2376926357280573482539570263854");
    CheckInputOutput<big_integer>(
        "4078460509739485762306457364875609364258763498578235876432589345693645872686453947256"
        "93456023046037490024067294087609279");

    CheckInputOutput<big_integer>("1000000000000");
    CheckInputOutput<big_integer>("1000000000000000000000000000");
    CheckInputOutput<big_integer>("100000000000000000000000000000000000000000000000000000000000");

    CheckInputOutput<big_integer>("99999999999999");
    CheckInputOutput<big_integer>("99999999999999999999999999999999");
    CheckInputOutput<big_integer>(
        "9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999");

    CheckInputOutput<big_integer>("-1");
    CheckInputOutput<big_integer>("-2");
    CheckInputOutput<big_integer>("-9");
    CheckInputOutput<big_integer>("-10");
    CheckInputOutput<big_integer>("-1123");
    CheckInputOutput<big_integer>("-64539472569345602304");
    CheckInputOutput<big_integer>("-2376926357280573482539570263854");
    CheckInputOutput<big_integer>(
        "-407846050973948576230645736487560936425876349857823587643258934569364587268645394725"
        "693456023046037490024067294087609279");

    CheckInputOutput<big_integer>("-1000000000000");
    CheckInputOutput<big_integer>("-1000000000000000000000000000");
    CheckInputOutput<big_integer>("-100000000000000000000000000000000000000000000000000000000000");

    CheckInputOutput<big_integer>("-99999999999999");
    CheckInputOutput<big_integer>("-99999999999999999999999999999999");
    CheckInputOutput<big_integer>(
        "-9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999");
}

TEST(bignum, TestInputOutputSimpleDecimal) {
    CheckInputOutput<big_decimal>("0");
    CheckInputOutput<big_decimal>("1");
    CheckInputOutput<big_decimal>("2");
    CheckInputOutput<big_decimal>("9");
    CheckInputOutput<big_decimal>("10");
    CheckInputOutput<big_decimal>("1123");
    CheckInputOutput<big_decimal>("64539472569345602304");
    CheckInputOutput<big_decimal>("2376926357280573482539570263854");
    CheckInputOutput<big_decimal>(
        "4078460509739485762306457364875609364258763498578235876432589345693645872686453947256"
        "93456023046037490024067294087609279");

    CheckInputOutput<big_decimal>("1000000000000");
    CheckInputOutput<big_decimal>("1000000000000000000000000000");
    CheckInputOutput<big_decimal>("100000000000000000000000000000000000000000000000000000000000");

    CheckInputOutput<big_decimal>("99999999999999");
    CheckInputOutput<big_decimal>("99999999999999999999999999999999");
    CheckInputOutput<big_decimal>(
        "9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999");

    CheckInputOutput<big_decimal>("-1");
    CheckInputOutput<big_decimal>("-2");
    CheckInputOutput<big_decimal>("-9");
    CheckInputOutput<big_decimal>("-10");
    CheckInputOutput<big_decimal>("-1123");
    CheckInputOutput<big_decimal>("-64539472569345602304");
    CheckInputOutput<big_decimal>("-2376926357280573482539570263854");
    CheckInputOutput<big_decimal>(
        "-407846050973948576230645736487560936425876349857823587643258934569364587268645394725"
        "693456023046037490024067294087609279");

    CheckInputOutput<big_decimal>("-1000000000000");
    CheckInputOutput<big_decimal>("-1000000000000000000000000000");
    CheckInputOutput<big_decimal>("-100000000000000000000000000000000000000000000000000000000000");

    CheckInputOutput<big_decimal>("-99999999999999");
    CheckInputOutput<big_decimal>("-99999999999999999999999999999999");
    CheckInputOutput<big_decimal>(
        "-9999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999");

    CheckInputOutput<big_decimal>("0.1");
    CheckInputOutput<big_decimal>("0.2");
    CheckInputOutput<big_decimal>("0.3");
    CheckInputOutput<big_decimal>("0.4");
    CheckInputOutput<big_decimal>("0.5");
    CheckInputOutput<big_decimal>("0.6");
    CheckInputOutput<big_decimal>("0.7");
    CheckInputOutput<big_decimal>("0.8");
    CheckInputOutput<big_decimal>("0.9");
    CheckInputOutput<big_decimal>("0.01");
    CheckInputOutput<big_decimal>("0.001");
    CheckInputOutput<big_decimal>("0.0001");
    CheckInputOutput<big_decimal>("0.00001");
    CheckInputOutput<big_decimal>("0.000001");
    CheckInputOutput<big_decimal>("0.0000001");

    CheckInputOutput<big_decimal>("0.00000000000000000000000000000000001");
    CheckInputOutput<big_decimal>("0.10000000000000000000000000000000001");
    CheckInputOutput<big_decimal>("0.10101010101010101010101010101010101");
    CheckInputOutput<big_decimal>("0.99999999999999999999999999999999999");
    CheckInputOutput<big_decimal>("0.79287502687354897253590684568634528762");

    CheckInputOutput<big_decimal>("0.00000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<big_decimal>("0.10000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<big_decimal>("0.1111111111111111111111111111111111111111111111111111111111");
    CheckInputOutput<big_decimal>("0.9999999999999999999999999999999999999999999999999999999999999999999");
    CheckInputOutput<big_decimal>(
        "0.436589746389567836745873648576289634589763845768268457683762864587684635892768346589629");

    CheckInputOutput<big_decimal>(
        "0."
        "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000001");
    CheckInputOutput<big_decimal>(
        "0."
        "1000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000001");
    CheckInputOutput<big_decimal>(
        "0."
        "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111"
        "11111111111111111111111111111111111111111");
    CheckInputOutput<big_decimal>(
        "0."
        "9999999999999999999999999999999999999999999999999999999999999999999999999999999999999"
        "99999999999999999999999999999999999999999999999999");
    CheckInputOutput<big_decimal>(
        "0."
        "4365897463895678367458736485762896345897638457682684576837628645876846358927683465493"
        "85700256032605603246580726384075680247634627357023645889629");

    CheckInputOutput<big_decimal>("-0.1");
    CheckInputOutput<big_decimal>("-0.2");
    CheckInputOutput<big_decimal>("-0.3");
    CheckInputOutput<big_decimal>("-0.4");
    CheckInputOutput<big_decimal>("-0.5");
    CheckInputOutput<big_decimal>("-0.6");
    CheckInputOutput<big_decimal>("-0.7");
    CheckInputOutput<big_decimal>("-0.8");
    CheckInputOutput<big_decimal>("-0.9");
    CheckInputOutput<big_decimal>("-0.01");
    CheckInputOutput<big_decimal>("-0.001");
    CheckInputOutput<big_decimal>("-0.0001");
    CheckInputOutput<big_decimal>("-0.00001");
    CheckInputOutput<big_decimal>("-0.000001");
    CheckInputOutput<big_decimal>("-0.0000001");

    CheckInputOutput<big_decimal>("-0.00000000000000000000000000000000001");
    CheckInputOutput<big_decimal>("-0.10000000000000000000000000000000001");
    CheckInputOutput<big_decimal>("-0.10101010101010101010101010101010101");
    CheckInputOutput<big_decimal>("-0.99999999999999999999999999999999999");
    CheckInputOutput<big_decimal>("-0.79287502687354897253590684568634528762");

    CheckInputOutput<big_decimal>("-0.00000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<big_decimal>("-0.10000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<big_decimal>("-0.1111111111111111111111111111111111111111111111111111111111");
    CheckInputOutput<big_decimal>("-0.9999999999999999999999999999999999999999999999999999999999999999999");
    CheckInputOutput<big_decimal>(
        "-0.436589746389567836745873648576289634589763845768268457683762864587684635892768346589629");

    CheckInputOutput<big_decimal>(
        "-0."
        "0000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000001");
    CheckInputOutput<big_decimal>(
        "-0."
        "1000000000000000000000000000000000000000000000000000000000000000000000000000000000000"
        "0000000000000000000000000000000000001");
    CheckInputOutput<big_decimal>(
        "-0."
        "1111111111111111111111111111111111111111111111111111111111111111111111111111111111111"
        "11111111111111111111111111111111111111111");
    CheckInputOutput<big_decimal>(
        "-0."
        "9999999999999999999999999999999999999999999999999999999999999999999999999999999999999"
        "99999999999999999999999999999999999999999999999999");
    CheckInputOutput<big_decimal>(
        "-0."
        "4365897463895678367458736485762896345897638457682684576837628645876846358927683465493"
        "85700256032605603246580726384075680247634627357023645889629");

    CheckInputOutput<big_decimal>("1.1");
    CheckInputOutput<big_decimal>("12.21");
    CheckInputOutput<big_decimal>("123.321");
    CheckInputOutput<big_decimal>("1234.4321");
    CheckInputOutput<big_decimal>("12345.54321");
    CheckInputOutput<big_decimal>("123456.654321");
    CheckInputOutput<big_decimal>("1234567.7654321");
    CheckInputOutput<big_decimal>("12345678.87654321");
    CheckInputOutput<big_decimal>("123456789.987654321");
    CheckInputOutput<big_decimal>("1234567890.0987654321");
    CheckInputOutput<big_decimal>("12345678909.90987654321");
    CheckInputOutput<big_decimal>("123456789098.890987654321");
    CheckInputOutput<big_decimal>("1234567890987.7890987654321");
    CheckInputOutput<big_decimal>("12345678909876.67890987654321");
    CheckInputOutput<big_decimal>("123456789098765.567890987654321");
    CheckInputOutput<big_decimal>("1234567890987654.4567890987654321");
    CheckInputOutput<big_decimal>("12345678909876543.34567890987654321");
    CheckInputOutput<big_decimal>("123456789098765432.234567890987654321");
    CheckInputOutput<big_decimal>("1234567890987654321.1234567890987654321");
    CheckInputOutput<big_decimal>("12345678909876543210.01234567890987654321");
    CheckInputOutput<big_decimal>("10000000000000000000000000000000000000000000000000000000000000."
                                  "000000000000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<big_decimal>("111111111111111111111111111111111111111111111111111111111111111111111."
                                  "11111111111111111111111111111111111111111111111111111111111111");
    CheckInputOutput<big_decimal>("99999999999999999999999999999999999999999999999999999999999999999999."
                                  "99999999999999999999999999999999999999999999999999999999999999999999");
    CheckInputOutput<big_decimal>("458796987658934265896483756892638456782376482605002747502306790283640563."
                                  "12017054126750641065780784583204650763485718064875683468568360506340563042567");

    CheckInputOutput<big_decimal>("-1.1");
    CheckInputOutput<big_decimal>("-12.21");
    CheckInputOutput<big_decimal>("-123.321");
    CheckInputOutput<big_decimal>("-1234.4321");
    CheckInputOutput<big_decimal>("-12345.54321");
    CheckInputOutput<big_decimal>("-123456.654321");
    CheckInputOutput<big_decimal>("-1234567.7654321");
    CheckInputOutput<big_decimal>("-12345678.87654321");
    CheckInputOutput<big_decimal>("-123456789.987654321");
    CheckInputOutput<big_decimal>("-1234567890.0987654321");
    CheckInputOutput<big_decimal>("-12345678909.90987654321");
    CheckInputOutput<big_decimal>("-123456789098.890987654321");
    CheckInputOutput<big_decimal>("-1234567890987.7890987654321");
    CheckInputOutput<big_decimal>("-12345678909876.67890987654321");
    CheckInputOutput<big_decimal>("-123456789098765.567890987654321");
    CheckInputOutput<big_decimal>("-1234567890987654.4567890987654321");
    CheckInputOutput<big_decimal>("-12345678909876543.34567890987654321");
    CheckInputOutput<big_decimal>("-123456789098765432.234567890987654321");
    CheckInputOutput<big_decimal>("-1234567890987654321.1234567890987654321");
    CheckInputOutput<big_decimal>("-12345678909876543210.01234567890987654321");
    CheckInputOutput<big_decimal>("-10000000000000000000000000000000000000000000000000000000000000."
                                  "000000000000000000000000000000000000000000000000000000000000001");
    CheckInputOutput<big_decimal>("-111111111111111111111111111111111111111111111111111111111111111111111."
                                  "11111111111111111111111111111111111111111111111111111111111111");
    CheckInputOutput<big_decimal>("-99999999999999999999999999999999999999999999999999999999999999999999."
                                  "99999999999999999999999999999999999999999999999999999999999999999999");
    CheckInputOutput<big_decimal>("-458796987658934265896483756892638456782376482605002747502306790283640563."
                                  "12017054126750641065780784583204650763485718064875683468568360506340563042567");
}

TEST(bignum, TestInputSimpleDecimal) {
    CheckOutputInput(big_decimal(0LL));

    CheckOutputInput(big_decimal(1LL));
    CheckOutputInput(big_decimal(9LL));
    CheckOutputInput(big_decimal(10LL));
    CheckOutputInput(big_decimal(11LL));
    CheckOutputInput(big_decimal(19LL));
    CheckOutputInput(big_decimal(123LL));
    CheckOutputInput(big_decimal(1234LL));
    CheckOutputInput(big_decimal(12345LL));
    CheckOutputInput(big_decimal(123456LL));
    CheckOutputInput(big_decimal(1234567LL));
    CheckOutputInput(big_decimal(12345678LL));
    CheckOutputInput(big_decimal(123456789LL));
    CheckOutputInput(big_decimal(1234567890LL));
    CheckOutputInput(big_decimal(12345678909LL));
    CheckOutputInput(big_decimal(123456789098LL));
    CheckOutputInput(big_decimal(1234567890987LL));
    CheckOutputInput(big_decimal(12345678909876LL));
    CheckOutputInput(big_decimal(123456789098765LL));
    CheckOutputInput(big_decimal(1234567890987654LL));
    CheckOutputInput(big_decimal(12345678909876543LL));
    CheckOutputInput(big_decimal(123456789098765432LL));
    CheckOutputInput(big_decimal(1234567890987654321LL));
    CheckOutputInput(big_decimal(999999999999999999LL));
    CheckOutputInput(big_decimal(999999999099999999LL));
    CheckOutputInput(big_decimal(1000000000000000000LL));
    CheckOutputInput(big_decimal(1000000000000000001LL));
    CheckOutputInput(big_decimal(1000000005000000000LL));
    CheckOutputInput(big_decimal(INT64_MAX));

    CheckOutputInput(big_decimal(-1LL));
    CheckOutputInput(big_decimal(-9LL));
    CheckOutputInput(big_decimal(-10LL));
    CheckOutputInput(big_decimal(-11LL));
    CheckOutputInput(big_decimal(-19LL));
    CheckOutputInput(big_decimal(-123LL));
    CheckOutputInput(big_decimal(-1234LL));
    CheckOutputInput(big_decimal(-12345LL));
    CheckOutputInput(big_decimal(-123456LL));
    CheckOutputInput(big_decimal(-1234567LL));
    CheckOutputInput(big_decimal(-12345678LL));
    CheckOutputInput(big_decimal(-123456789LL));
    CheckOutputInput(big_decimal(-1234567890LL));
    CheckOutputInput(big_decimal(-12345678909LL));
    CheckOutputInput(big_decimal(-123456789098LL));
    CheckOutputInput(big_decimal(-1234567890987LL));
    CheckOutputInput(big_decimal(-12345678909876LL));
    CheckOutputInput(big_decimal(-123456789098765LL));
    CheckOutputInput(big_decimal(-1234567890987654LL));
    CheckOutputInput(big_decimal(-12345678909876543LL));
    CheckOutputInput(big_decimal(-123456789098765432LL));
    CheckOutputInput(big_decimal(-1234567890987654321LL));
    CheckOutputInput(big_decimal(-999999999999999999LL));
    CheckOutputInput(big_decimal(-999999999099999999LL));
    CheckOutputInput(big_decimal(-1000000000000000000LL));
    CheckOutputInput(big_decimal(-1000000000000000001LL));
    CheckOutputInput(big_decimal(-1000000005000000000LL));
    CheckOutputInput(big_decimal(INT64_MIN));
}

TEST(bignum, TestScalingSmall) {
    big_decimal decimal(12345, 2);
    EXPECT_EQ(decimal.get_precision(), 5);

    decimal.set_scale(0, decimal);

    EXPECT_EQ(decimal.get_precision(), 3);

    EXPECT_EQ(decimal.to_int64(), 123);
}

TEST(bignum, TestScalingBig) {
    big_integer bigInt(69213205262741);

    big_decimal decimal;
    EXPECT_EQ(decimal.get_precision(), 1);

    // 4790467782742318458842833081
    bigInt.multiply(bigInt, bigInt);

    decimal = big_decimal(bigInt, 0);
    EXPECT_EQ(decimal.get_precision(), 28);

    // 22948581577492104846692006446607391554788985798427952561
    bigInt.multiply(bigInt, bigInt);

    decimal = big_decimal(bigInt, 0);
    EXPECT_EQ(decimal.get_precision(), 56);

    // 22948581577492104846692006446607391554.788985798427952561
    decimal = big_decimal(bigInt, 18);

    // 22948581577492104846692006446607391554
    decimal.set_scale(0, decimal);

    EXPECT_EQ(decimal.get_precision(), 38);

    // 22948581.577492104846692006446607391554788985798427952561
    decimal = big_decimal(bigInt, 48);

    // 22948581
    decimal.set_scale(0, decimal);

    EXPECT_EQ(decimal.get_precision(), 8);
    EXPECT_EQ(decimal.to_int64(), 22948581);

    // 636471904553811060306806140254026286906087997856914463925431295610150712
    // 436301540552945788827650832722026963914694916372255230793492080431332686
    // 268324254350022490844698008329270553114204362445999680199136593689695140
    // 0874934591063287320666899465891248127072522251998904759858801
    bigInt.pow(5);

    // 63647190455381106.030680614025402628690608799785691446392543129561015071
    // 243630154055294578882765083272202696391469491637225523079349208043133268
    // 626832425435002249084469800832927055311420436244599968019913659368969514
    // 00874934591063287320666899465891248127072522251998904759858801
    decimal = big_decimal(bigInt, 260);
    EXPECT_EQ(decimal.get_precision(), 277);

    // 63647190455381106
    decimal.set_scale(0, decimal);

    EXPECT_EQ(decimal.get_precision(), 17);
    EXPECT_EQ(decimal.to_int64(), 63647190455381106LL);
}

TEST(bignum, TestDecimalSimple) {
    EXPECT_EQ(big_decimal(1000L, 3) + big_decimal(1000L, 3), big_decimal(2000L, 3));
    EXPECT_EQ(big_decimal(1000L, 3) + big_decimal(1000L, 2), big_decimal(11000L, 3));
    EXPECT_EQ(big_decimal(1000L, 3) + big_decimal(1000L, 1), big_decimal(101000L, 3));
    EXPECT_EQ(big_decimal(1000L, 3) + big_decimal(1000L, 0), big_decimal(1001000L, 3));

    EXPECT_EQ(big_decimal(1000L, 3) - big_decimal(1000L, 3), big_decimal(0LL, 3));
    EXPECT_EQ(big_decimal(1000L, 3) - big_decimal(1000L, 2), big_decimal(-9000L, 3));
    EXPECT_EQ(big_decimal(1000L, 3) - big_decimal(1000L, 1), big_decimal(-99000L, 3));
    EXPECT_EQ(big_decimal(1000L, 3) - big_decimal(1000L, 0), big_decimal(-999000L, 3));

    EXPECT_EQ(big_decimal(1000L, 3) * big_decimal(1000L, 3), big_decimal(1000L, 3));
    EXPECT_EQ(big_decimal(1000L, 3) * big_decimal(1000L, 2), big_decimal(10000L, 3));
    EXPECT_EQ(big_decimal(1000L, 3) * big_decimal(1000L, 1), big_decimal(100000L, 3));
    EXPECT_EQ(big_decimal(1000L, 3) * big_decimal(1000L, 0), big_decimal(1000000L, 3));

    EXPECT_EQ(big_decimal(1000L, 3) / big_decimal(1000L, 3), big_decimal(1000L, 3));
    EXPECT_EQ(big_decimal(1000L, 3) / big_decimal(1000L, 2), big_decimal(100L, 3));
    EXPECT_EQ(big_decimal(1000L, 3) / big_decimal(1000L, 1), big_decimal(10L, 3));
    EXPECT_EQ(big_decimal(1000L, 3) / big_decimal(1000L, 0), big_decimal(1L, 3));

    // Generated tests
    // Test with negative numbers
    EXPECT_EQ(big_decimal(-1000L, 3) / big_decimal(1000L, 3), big_decimal(-1000L, 3));
    EXPECT_EQ(big_decimal(1000L, 3) / big_decimal(-1000L, 3), big_decimal(-1000L, 3));
    EXPECT_EQ(big_decimal(-1000L, 3) / big_decimal(-1000L, 3), big_decimal(1000L, 3));

    // Test with zero
    EXPECT_EQ(big_decimal(0LL, 3) / big_decimal(1000L, 3), big_decimal(0LL, 3));

    // Test with a large scale
    EXPECT_EQ(big_decimal(123456789L, 100) / big_decimal(100000000L, 100), big_decimal(123456789L, 8));

    // Test with different scales
    EXPECT_EQ(big_decimal(123456789L, 5) / big_decimal(100000000L, 3), big_decimal(123456789L, 10));
    EXPECT_EQ(big_decimal(123456789L, 3) / big_decimal(100000000L, 5), big_decimal(123456789L, 6));

    // Test scale after divide
    EXPECT_EQ(big_decimal(123456789L, 0) / big_decimal(100000000L, 0), big_decimal(123456789L, 8));
    EXPECT_EQ(big_decimal(123456789L, 0) / big_decimal(100000000L, 1), big_decimal(123456789L, 7));
    EXPECT_EQ(big_decimal(123456789L, 0) / big_decimal(100000000L, 2), big_decimal(123456789L, 6));
    EXPECT_EQ(big_decimal(123456789L, 0) / big_decimal(100000000L, 3), big_decimal(123456789L, 5));
    EXPECT_EQ(big_decimal(123456789L, 0) / big_decimal(100000000L, 4), big_decimal(123456789L, 4));
    EXPECT_EQ(big_decimal(123456789L, 0) / big_decimal(100000000L, 5), big_decimal(123456789L, 3));
    EXPECT_EQ(big_decimal(123456789L, 0) / big_decimal(100000000L, 6), big_decimal(123456789L, 2));
    EXPECT_EQ(big_decimal(123456789L, 0) / big_decimal(100000000L, 7), big_decimal(123456789L, 1));
    EXPECT_EQ(big_decimal(123456789L, 0) / big_decimal(100000000L, 8), big_decimal(123456789L, 0));

    EXPECT_EQ(big_decimal(123456789L, 0) / big_decimal(100000000L, 0), big_decimal(123456789L, 8));
    EXPECT_EQ(big_decimal(123456789L, 1) / big_decimal(100000000L, 0), big_decimal(123456789L, 9));
    EXPECT_EQ(big_decimal(123456789L, 2) / big_decimal(100000000L, 0), big_decimal(123456789L, 10));
    EXPECT_EQ(big_decimal(123456789L, 3) / big_decimal(100000000L, 0), big_decimal(123456789L, 11));
    EXPECT_EQ(big_decimal(123456789L, 4) / big_decimal(100000000L, 0), big_decimal(123456789L, 12));
    EXPECT_EQ(big_decimal(123456789L, 5) / big_decimal(100000000L, 0), big_decimal(123456789L, 13));
    EXPECT_EQ(big_decimal(123456789L, 6) / big_decimal(100000000L, 0), big_decimal(123456789L, 14));
    EXPECT_EQ(big_decimal(123456789L, 7) / big_decimal(100000000L, 0), big_decimal(123456789L, 15));
    EXPECT_EQ(big_decimal(123456789L, 8) / big_decimal(100000000L, 0), big_decimal(123456789L, 16));

    EXPECT_EQ(big_decimal(123456789L, 0) / big_decimal(100000000L, 8), big_decimal(123456789L, 0));
    EXPECT_EQ(big_decimal(123456789L, 1) / big_decimal(100000000L, 7), big_decimal(123456789L, 2));
    EXPECT_EQ(big_decimal(123456789L, 2) / big_decimal(100000000L, 6), big_decimal(123456789L, 4));
    EXPECT_EQ(big_decimal(123456789L, 3) / big_decimal(100000000L, 5), big_decimal(123456789L, 6));
    EXPECT_EQ(big_decimal(123456789L, 4) / big_decimal(100000000L, 4), big_decimal(123456789L, 8));
    EXPECT_EQ(big_decimal(123456789L, 5) / big_decimal(100000000L, 3), big_decimal(123456789L, 10));
    EXPECT_EQ(big_decimal(123456789L, 6) / big_decimal(100000000L, 2), big_decimal(123456789L, 12));
    EXPECT_EQ(big_decimal(123456789L, 7) / big_decimal(100000000L, 1), big_decimal(123456789L, 14));
    EXPECT_EQ(big_decimal(123456789L, 8) / big_decimal(100000000L, 0), big_decimal(123456789L, 16));

    // Random
    EXPECT_EQ(big_decimal(98094819054L, 2) + big_decimal(24689666136L, 1), big_decimal(344991480414L, 2));
    EXPECT_EQ(big_decimal(58628397409L, 6) + big_decimal(45577616757L, 4), big_decimal(4616390073109L, 6));

    EXPECT_EQ(big_decimal(98094819054L, 2) * big_decimal(24689666136L, 1), big_decimal("2421928332114591355.344"));
    EXPECT_EQ(big_decimal(58628397409L, 6) * big_decimal(45577616757L, 4), big_decimal("267214262818.4493782613"));

    EXPECT_EQ(big_decimal(98094819054L, 2) / big_decimal(24689666136L, 1), big_decimal(3973112415277L, 13));
    EXPECT_EQ(big_decimal(58628397409L, 6) / big_decimal(45577616757L, 4), big_decimal(1286341884034L, 14));
}

TEST(bignum, TestPrecisionSimple) {
    big_decimal test(1LL);

    EXPECT_EQ(big_decimal(-9LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(-8LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(-7LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(-6LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(-5LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(-4LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(-3LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(-2LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(-1LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(0LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(1LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(2LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(3LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(4LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(5LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(6LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(7LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(8LL).get_precision(), 1);
    EXPECT_EQ(big_decimal(9LL).get_precision(), 1);

    EXPECT_EQ(big_decimal(2147483648LL).get_precision(), 10); // 2^31:       10 digits
    EXPECT_EQ(big_decimal(-2147483647LL).get_precision(), 10); // -2^31+1:    10 digits
    EXPECT_EQ(big_decimal(98893745455LL).get_precision(), 11); // random:     11 digits
    EXPECT_EQ(big_decimal(3455436789887LL).get_precision(), 13); // random:     13 digits
    EXPECT_EQ(big_decimal(140737488355328LL).get_precision(), 15); // 2^47:       15 digits
    EXPECT_EQ(big_decimal(-140737488355328LL).get_precision(), 15); // -2^47:      15 digits
    EXPECT_EQ(big_decimal(7564232235739573LL).get_precision(), 16); // random:     16 digits
    EXPECT_EQ(big_decimal(25335434990002322LL).get_precision(), 17); // random:     17 digits
    EXPECT_EQ(big_decimal(9223372036854775807LL).get_precision(), 19); // 2^63 - 1:   19 digits
    EXPECT_EQ(big_decimal(-9223372036854775807LL).get_precision(), 19); // -2^63 + 1:  19 digits
}

TEST(bignum, TestPrecisionChange) {
    big_integer bigInt(32421);

    // 75946938183
    bigInt.multiply(big_integer(2342523), bigInt);

    // 4244836901495581620
    bigInt.multiply(big_integer(55892140), bigInt);

    // 1361610054778960404282184020
    bigInt.multiply(big_integer(320768521), bigInt);

    // 1454144449122723409814375680476734820
    bigInt.multiply(big_integer(1067959541), bigInt);

    // 117386322514277938455905731466723946155156640
    bigInt.multiply(big_integer(80725352), bigInt);

    // 1173863225142779384559.05731466723946155156640
    big_decimal decimal(bigInt, 23);

    EXPECT_EQ(decimal.get_precision(), 45);
    EXPECT_EQ(decimal.get_scale(), 23);

    for (std::int16_t i = 0; i < decimal.get_scale(); ++i) {
        decimal.set_scale(i, decimal);

        EXPECT_EQ(decimal.get_precision(), decimal.get_precision() - decimal.get_scale() + i);
    }
}

TEST(bignum, TestDoubleCast) {
    CheckDoubleCast(100000000000000.0);
    CheckDoubleCast(10000000000000.0);
    CheckDoubleCast(1000000000000.0);
    CheckDoubleCast(100000000000.0);
    CheckDoubleCast(10000000000.0);
    CheckDoubleCast(1000000000.0);
    CheckDoubleCast(100000000.0);
    CheckDoubleCast(10000000.0);
    CheckDoubleCast(1000000.0);
    CheckDoubleCast(100000.0);
    CheckDoubleCast(10000.0);
    CheckDoubleCast(1000.0);
    CheckDoubleCast(100.0);
    CheckDoubleCast(10.0);
    CheckDoubleCast(1.0);
    CheckDoubleCast(0.1);
    CheckDoubleCast(0.01);
    CheckDoubleCast(0.001);
    CheckDoubleCast(0.0001);
    CheckDoubleCast(0.00001);
    CheckDoubleCast(0.000001);
    CheckDoubleCast(0.0000001);
    CheckDoubleCast(0.00000001);
    CheckDoubleCast(0.000000001);
    CheckDoubleCast(0.0000000001);
    CheckDoubleCast(0.00000000001);
    CheckDoubleCast(0.000000000001);
    CheckDoubleCast(0.00000000000001);
    CheckDoubleCast(0.000000000000001);

    CheckDoubleCast(2379506093465806.);
    CheckDoubleCast(172650963870256.3);
    CheckDoubleCast(63506206502638.57);
    CheckDoubleCast(8946589364589.763);
    CheckDoubleCast(896258976348.9568);
    CheckDoubleCast(28967349256.39428);
    CheckDoubleCast(2806348972.689369);
    CheckDoubleCast(975962354.2835845);
    CheckDoubleCast(96342568.93542342);
    CheckDoubleCast(6875825.934892387);
    CheckDoubleCast(314969.6543969458);
    CheckDoubleCast(32906.02476506344);
    CheckDoubleCast(9869.643965396453);
    CheckDoubleCast(779.6932689452953);
    CheckDoubleCast(75.93592963492326);
    CheckDoubleCast(8.719654293587452);
    CheckDoubleCast(.90001236587463439);

    CheckDoubleCast(-235235E-100);
    CheckDoubleCast(-235235E-90);
    CheckDoubleCast(-235235E-80);
    CheckDoubleCast(-235235E-70);
    CheckDoubleCast(-235235E-60);
    CheckDoubleCast(-235235E-50);
    CheckDoubleCast(-235235E-40);
    CheckDoubleCast(-235235E-30);
    CheckDoubleCast(-235235E-25);
    CheckDoubleCast(-235235E-20);
    CheckDoubleCast(-235235E-15);
    CheckDoubleCast(-235235E-10);
    CheckDoubleCast(-235235E-9);
    CheckDoubleCast(-235235E-8);
    CheckDoubleCast(-235235E-7);
    CheckDoubleCast(-235235E-6);
    CheckDoubleCast(-235235E-5);
    CheckDoubleCast(-235235E-4);
    CheckDoubleCast(-235235E-2);
    CheckDoubleCast(-235235E-1);
    CheckDoubleCast(-235235);
    CheckDoubleCast(-235235E+1);
    CheckDoubleCast(-235235E+2);
    CheckDoubleCast(-235235E+3);
    CheckDoubleCast(-235235E+4);
    CheckDoubleCast(-235235E+5);
    CheckDoubleCast(-235235E+6);
    CheckDoubleCast(-235235E+7);
    CheckDoubleCast(-235235E+8);
    CheckDoubleCast(-235235E+9);
    CheckDoubleCast(-235235E+10);
    CheckDoubleCast(-235235E+15);
    CheckDoubleCast(-235235E+20);
    CheckDoubleCast(-235235E+25);
    CheckDoubleCast(-235235E+30);
    CheckDoubleCast(-235235E+40);
    CheckDoubleCast(-235235E+50);
    CheckDoubleCast(-235235E+60);
    CheckDoubleCast(-235235E+70);
    CheckDoubleCast(-235235E+80);
    CheckDoubleCast(-235235E+90);
    CheckDoubleCast(-235235E+100);

    CheckDoubleCast(-2379506093465806.);
    CheckDoubleCast(-172650963870256.3);
    CheckDoubleCast(-63506206502638.57);
    CheckDoubleCast(-8946589364589.763);
    CheckDoubleCast(-896258976348.9568);
    CheckDoubleCast(-28967349256.39428);
    CheckDoubleCast(-2806348972.689369);
    CheckDoubleCast(-975962354.2835845);
    CheckDoubleCast(-96342568.93542342);
    CheckDoubleCast(-6875825.934892387);
    CheckDoubleCast(-314969.6543969458);
    CheckDoubleCast(-32906.02476506344);
    CheckDoubleCast(-9869.643965396453);
    CheckDoubleCast(-779.6932689452953);
    CheckDoubleCast(-75.93592963492326);
    CheckDoubleCast(-8.719654293587452);
    CheckDoubleCast(-.90001236587463439);

    CheckDoubleCast(-100000000000000.0);
    CheckDoubleCast(-10000000000000.0);
    CheckDoubleCast(-1000000000000.0);
    CheckDoubleCast(-100000000000.0);
    CheckDoubleCast(-10000000000.0);
    CheckDoubleCast(-1000000000.0);
    CheckDoubleCast(-100000000.0);
    CheckDoubleCast(-10000000.0);
    CheckDoubleCast(-1000000.0);
    CheckDoubleCast(-100000.0);
    CheckDoubleCast(-10000.0);
    CheckDoubleCast(-1000.0);
    CheckDoubleCast(-100.0);
    CheckDoubleCast(-10.0);
    CheckDoubleCast(-1.0);
    CheckDoubleCast(-0.1);
    CheckDoubleCast(-0.01);
    CheckDoubleCast(-0.001);
    CheckDoubleCast(-0.0001);
    CheckDoubleCast(-0.00001);
    CheckDoubleCast(-0.000001);
    CheckDoubleCast(-0.0000001);
    CheckDoubleCast(-0.00000001);
    CheckDoubleCast(-0.000000001);
    CheckDoubleCast(-0.0000000001);
    CheckDoubleCast(-0.00000000001);
    CheckDoubleCast(-0.000000000001);
    CheckDoubleCast(-0.00000000000001);
    CheckDoubleCast(-0.000000000000001);
}
