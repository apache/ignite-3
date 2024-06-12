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

#include "mpi.h"

#include "ignite_error.h"

#include <utility>
#include <vector>

namespace ignite {

namespace {

void check(int code) {
    switch (code) {
        case MBEDTLS_ERR_MPI_ALLOC_FAILED:
            throw ignite_error("mbedtls: alloc failed");
        case MBEDTLS_ERR_MPI_BAD_INPUT_DATA:
            throw ignite_error("mbedtls: bad input data");
        case MBEDTLS_ERR_MPI_BUFFER_TOO_SMALL:
            throw ignite_error("mbedtls: buffer too small");
        case MBEDTLS_ERR_MPI_DIVISION_BY_ZERO:
            throw ignite_error("mbedtls: division by zero");
        case MBEDTLS_ERR_MPI_FILE_IO_ERROR:
            throw ignite_error("mbedtls: file io error");
        case MBEDTLS_ERR_MPI_INVALID_CHARACTER:
            throw ignite_error("mbedtls: invalid characters");
        case MBEDTLS_ERR_MPI_NEGATIVE_VALUE:
            throw ignite_error("mbedtls: negative value");
        case MBEDTLS_ERR_MPI_NOT_ACCEPTABLE:
            throw ignite_error("mbedtls: not acceptable");
        case 0:
            return;
        default:
            throw ignite_error("mbedtls: unspecified error");
    }
}

} // namespace

mpi::mpi() {
    init();
}

mpi::mpi(const mpi::word *mag, unsigned short size, mpi_sign sign) {
    init();

    val.p = const_cast<word *>(mag);
    val.n = size;
    val.s = sign;
}

mpi::mpi(std::int32_t v) {
    init();
    check(mbedtls_mpi_lset(&val, v));
}

mpi::mpi(const char *string) {
    init();
    assign_from_string(string);
}

mpi::~mpi() {
    free();
}

mpi::mpi(const mpi &other) {
    init();

    check(mbedtls_mpi_copy(&val, &other.val));
    check(mbedtls_mpi_shrink(&val, 0));
}

mpi::mpi(mpi &&other) noexcept {
    using std::swap;

    init();

    std::swap(val.s, other.val.s);
    std::swap(val.n, other.val.n);
    std::swap(val.p, other.val.p);
}

mpi &mpi::operator=(const mpi &other) {
    if (this == &other) {
        return *this;
    }

    reinit();

    check(mbedtls_mpi_copy(&val, &other.val));

    return *this;
}

mpi &mpi::operator=(mpi &&other) noexcept {
    using std::swap;

    if (this == &other) {
        return *this;
    }

    std::swap(val.s, other.val.s);
    std::swap(val.n, other.val.n);
    std::swap(val.p, other.val.p);

    return *this;
}

void mpi::init() {
    mbedtls_mpi_init(&val);
}

void mpi::free() {
    mbedtls_mpi_free(&val);
}

void mpi::reinit() {
    free();
    init();
}

bool mpi::is_zero() const noexcept {
    return mbedtls_mpi_cmp_int(&val, 0) == 0;
}

bool mpi::is_positive() const noexcept {
    return val.s > 0 && !is_zero();
}

bool mpi::is_negative() const noexcept {
    return val.s < 0;
}

void mpi::make_positive() noexcept {
    val.s = mpi_sign::POSITIVE;
}

void mpi::make_negative() noexcept {
    val.s = mpi_sign::NEGATIVE;
}

void mpi::negate() noexcept {
    if (!is_zero()) {
        val.s = -val.s;
    }
}

void swap(mpi &lhs, mpi &rhs) {
    using std::swap;

    std::swap(lhs.val.s, rhs.val.s);
    std::swap(lhs.val.n, rhs.val.n);
    std::swap(lhs.val.p, rhs.val.p);
}

mpi mpi::operator+(const mpi &addendum) const {
    mpi result;

    check(mbedtls_mpi_add_mpi(&result.val, &val, &addendum.val));
    result.shrink();

    return result;
}

mpi mpi::operator-(const mpi &subtrahend) const {
    mpi result;

    check(mbedtls_mpi_sub_mpi(&result.val, &val, &subtrahend.val));
    result.shrink();

    return result;
}

mpi mpi::operator*(const mpi &factor) const {
    mpi result;

    check(mbedtls_mpi_mul_mpi(&result.val, &val, &factor.val));
    result.shrink();

    return result;
}

mpi mpi::operator/(const mpi &divisor) const {
    mpi result;

    check(mbedtls_mpi_div_mpi(&result.val, nullptr, &val, &divisor.val));
    result.shrink();

    return result;
}

mpi mpi::operator%(const mpi &divisor) const {
    mpi remainder;

    check(mbedtls_mpi_div_mpi(nullptr, &remainder.val, &val, &divisor.val));
    remainder.shrink();

    return remainder;
}

void mpi::add(const mpi &addendum) {
    check(mbedtls_mpi_add_mpi(&val, &val, &addendum.val));
    shrink();
}

void mpi::subtract(const mpi &subtrahend) {
    check(mbedtls_mpi_sub_mpi(&val, &val, &subtrahend.val));
    shrink();
}

void mpi::multiply(const mpi &factor) {
    check(mbedtls_mpi_mul_mpi(&val, &val, &factor.val));
    shrink();
}

void mpi::divide(const mpi &divisor) {
    check(mbedtls_mpi_div_mpi(&val, nullptr, &val, &divisor.val));
    shrink();
}

void mpi::modulo(const mpi &divisor) {
    check(mbedtls_mpi_div_mpi(nullptr, &val, &val, &divisor.val));
    shrink();
}

void mpi::shrink(size_t limbs) {
    check(mbedtls_mpi_shrink(&val, limbs));
}

void mpi::grow(size_t limbs) {
    check(mbedtls_mpi_grow(&val, limbs));
}

mpi mpi::div_and_mod(const mpi &divisor, mpi &remainder) const {
    mpi result;

    check(mbedtls_mpi_div_mpi(&result.val, &remainder.val, &val, &divisor.val));

    result.shrink();
    remainder.shrink();

    return result;
}

void mpi::assign_from_string(const char *string) {
    reinit();
    check(mbedtls_mpi_read_string(&val, 10, string));
}

std::string mpi::to_string() const {
    std::size_t required_size = 0;
    auto code = mbedtls_mpi_write_string(&val, 10, nullptr, 0, &required_size); // get required buffer size
    if (code == MBEDTLS_ERR_MPI_BUFFER_TOO_SMALL) {
        std::string buffer(required_size, 0);

        check(mbedtls_mpi_write_string(&val, 10, buffer.data(), required_size, &required_size));

        buffer.resize(required_size - 1); // -1 for \0. We don't need it for std::string.

        return buffer;
    }

    check(code);

    return {};
}

bool mpi::operator==(const mpi &other) const {
    return compare(other);
}

int mpi::compare(const mpi &other, bool ignore_sign) const noexcept {
    return ignore_sign ? mbedtls_mpi_cmp_abs(&val, &other.val) : mbedtls_mpi_cmp_mpi(&val, &other.val);
}

std::size_t mpi::magnitude_bit_length() const noexcept {
    return mbedtls_mpi_bitlen(&val);
}

} // namespace ignite
