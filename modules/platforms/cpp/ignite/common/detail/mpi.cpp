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

#include <mbedtls/build_info.h>
#include <mbedtls/private/bignum.h>

#include <utility>
#include <vector>

static_assert(std::is_same_v<mbedtls_mpi_uint, std::uint32_t>, "MbedTLS word should be std::uint32_t.");

namespace ignite::detail {

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

mpi::mpi(std::int32_t v) {
    init();
    check(mbedtls_mpi_lset(val, v));
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

    check(mbedtls_mpi_copy(val, other.val));
}

mpi::mpi(mpi &&other) noexcept {
    using std::swap;

    init();

    std::swap(val->s, other.val->s);
    std::swap(val->n, other.val->n);
    std::swap(val->p, other.val->p);
}

mpi &mpi::operator=(const mpi &other) {
    if (this == &other) {
        return *this;
    }

    reinit();

    check(mbedtls_mpi_copy(val, other.val));

    return *this;
}

mpi &mpi::operator=(mpi &&other) noexcept {
    using std::swap;

    if (this == &other) {
        return *this;
    }

    std::swap(val, other.val);

    return *this;
}

void mpi::init() {
    val = new mbedtls_mpi;
    mbedtls_mpi_init(val);
}

void mpi::free() {
    mbedtls_mpi_free(val);
    delete val;
}

void mpi::reinit() {
    free();
    init();
}

mpi_sign mpi::sign() const noexcept {
    return static_cast<mpi_sign>(val->s);
}

mpi::word *mpi::pointer() const noexcept {
    return val->p;
}

unsigned short mpi::length() const noexcept {
    return val->n;
}

mpi::mag_view mpi::magnitude() const noexcept {
    return {val->p, val->n, mbedtls_mpi_size(val)};
}

bool mpi::is_zero() const noexcept {
    return mbedtls_mpi_cmp_int(val, 0) == 0;
}

bool mpi::is_positive() const noexcept {
    return val->s > 0 && !is_zero();
}

bool mpi::is_negative() const noexcept {
    return val->s < 0;
}

void mpi::set_sign(mpi_sign sign) {
    val->s = sign;
}

void mpi::make_positive() noexcept {
    val->s = mpi_sign::POSITIVE;
}

void mpi::make_negative() noexcept {
    val->s = mpi_sign::NEGATIVE;
}

void mpi::negate() noexcept {
    if (!is_zero()) {
        val->s = -val->s;
    }
}

void swap(mpi &lhs, mpi &rhs) {
    using std::swap;

    std::swap(lhs.val->s, rhs.val->s);
    std::swap(lhs.val->n, rhs.val->n);
    std::swap(lhs.val->p, rhs.val->p);
}

mpi mpi::operator+(const mpi &addendum) const {
    mpi result;

    check(mbedtls_mpi_add_mpi(result.val, val, addendum.val));

    return result;
}

mpi mpi::operator-(const mpi &subtrahend) const {
    mpi result;

    check(mbedtls_mpi_sub_mpi(result.val, val, subtrahend.val));

    return result;
}

mpi mpi::operator*(const mpi &factor) const {
    mpi result;

    check(mbedtls_mpi_mul_mpi(result.val, val, factor.val));

    return result;
}

mpi mpi::operator/(const mpi &divisor) const {
    mpi result;

    check(mbedtls_mpi_div_mpi(result.val, nullptr, val, divisor.val));

    return result;
}

mpi mpi::operator%(const mpi &divisor) const {
    mpi remainder;

    check(mbedtls_mpi_div_mpi(nullptr, remainder.val, val, divisor.val));

    return remainder;
}

void mpi::add(const mpi &addendum) {
    check(mbedtls_mpi_add_mpi(val, val, addendum.val));
}

void mpi::subtract(const mpi &subtrahend) {
    check(mbedtls_mpi_sub_mpi(val, val, subtrahend.val));
}

void mpi::multiply(const mpi &factor) {
    check(mbedtls_mpi_mul_mpi(val, val, factor.val));
}

void mpi::divide(const mpi &divisor) {
    check(mbedtls_mpi_div_mpi(val, nullptr, val, divisor.val));
}

void mpi::modulo(const mpi &divisor) {
    check(mbedtls_mpi_div_mpi(nullptr, val, val, divisor.val));
}

void mpi::shrink(size_t limbs) {
    check(mbedtls_mpi_shrink(val, limbs));
}

void mpi::grow(size_t limbs) {
    check(mbedtls_mpi_grow(val, limbs));
}

mpi mpi::div_and_mod(const mpi &divisor, mpi &remainder) const {
    mpi result;

    check(mbedtls_mpi_div_mpi(result.val, remainder.val, val, divisor.val));

    return result;
}

void mpi::assign_from_string(const char *string) {
    reinit();
    check(mbedtls_mpi_read_string(val, 10, string));
}

std::string mpi::to_string() const {
    std::size_t required_size = 0;
    auto code = mbedtls_mpi_write_string(val, 10, nullptr, 0, &required_size); // get required buffer size
    if (code == MBEDTLS_ERR_MPI_BUFFER_TOO_SMALL) {
        std::string buffer(required_size, 0);

        check(mbedtls_mpi_write_string(val, 10, buffer.data(), required_size, &required_size));

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
    return ignore_sign ? mbedtls_mpi_cmp_abs(val, other.val) : mbedtls_mpi_cmp_mpi(val, other.val);
}

std::size_t mpi::magnitude_bit_length() const noexcept {
    return mbedtls_mpi_bitlen(val);
}

bool mpi::write(std::uint8_t *data, std::size_t size, bool big_endian) {
    if (big_endian) {
        return mbedtls_mpi_write_binary(val, data, size) == 0;
    }
    return mbedtls_mpi_write_binary_le(val, data, size) == 0;
}

bool mpi::read(const std::uint8_t *data, std::size_t size, bool big_endian) {
    if (big_endian) {
        return mbedtls_mpi_read_binary(val, data, size) == 0;
    }
    return mbedtls_mpi_read_binary_le(val, data, size) == 0;
}

} // namespace ignite::detail
