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

#pragma once

#include <cstddef>
#include <vector>

namespace ignite::network
{

/**
 * SSL/TLS connection state machine backed by a pair of memory BIOs.
 *
 * Owns an SSL instance and two memory BIOs:
 *   - input BIO  (bio_in):  feed received ciphertext in via feed_input()
 *   - output BIO (bio_out): drain ciphertext to send via drain_output()
 *
 * This class performs no network I/O. The caller is responsible for
 * moving bytes between the BIOs and the actual transport.
 */
class ssl_connection
{
public:
    /**
     * Constructor. Creates the SSL instance and attaches memory BIOs.
     *
     * @param ctx SSL_CTX* cast to void*.
     * @param hostname SNI hostname for TLS Server Name Indication. May be nullptr to skip SNI.
     */
    ssl_connection(void* ctx, const char* hostname);

    /**
     * Destructor. Frees the SSL instance (which also frees the attached BIOs).
     */
    ~ssl_connection();

    // Non-copyable, non-movable.
    ssl_connection(const ssl_connection&) = delete;
    ssl_connection& operator=(const ssl_connection&) = delete;

    /**
     * Perform one TLS handshake step (SSL_connect).
     *
     * The caller must call @c drain_output() after each invocation to flush any
     * generated handshake bytes to the peer, and @c feed_input() before retrying
     * when this returns @c false.
     *
     * @return @c true if the handshake completed successfully.
     * @throws ignite_error on a fatal SSL error.
     */
    bool do_handshake();

    /**
     * Encrypt plaintext. On success the resulting ciphertext is available via drain_output().
     *
     * @param data Plaintext bytes.
     * @param len  Number of bytes to encrypt.
     * @return Number of bytes consumed on success (> 0), or <= 0 on error/retry.
     */
    int write(const std::byte* data, int len);

    /**
     * Decrypt data from the input BIO into the caller-supplied buffer.
     *
     * @param buf Buffer to receive plaintext.
     * @param len Buffer size in bytes.
     * @return Number of plaintext bytes written to buf (> 0), or <= 0 on error/retry.
     */
    int read(std::byte* buf, int len);

    /**
     * Number of decrypted plaintext bytes currently buffered inside SSL
     * and available for an immediate read() call.
     */
    [[nodiscard]] int pending_decrypted() const;

    /**
     * Feed ciphertext received from the network into the input BIO.
     *
     * @param data Ciphertext pointer.
     * @param len  Number of bytes.
     * @throws ignite_error on BIO write failure.
     */
    void feed_input(const void* data, int len);

    /**
     * Drain all pending ciphertext from the output BIO.
     *
     * @return Bytes to send to the network. Empty if nothing is pending.
     */
    [[nodiscard]] std::vector<std::byte> drain_output();

    /**
     * TLS protocol version string (e.g. "TLSv1.3").
     */
    [[nodiscard]] const char* version() const;

    /**
     * Returns @c true if the given SSL operation result represents a fatal
     * error, @c false if it is a transient condition (@c WANT_READ, @c WANT_WRITE, etc.)
     * that can be resolved by feeding more input or retrying.
     *
     * @param ssl_op_result Return value of @c write() or @c read().
     */
    [[nodiscard]] bool is_fatal_error(int ssl_op_result) const;

    /**
     * Returns @c true if SSL is requesting more ciphertext input before it
     * can proceed (SSL_WANT_READ). Use after write() or read() returns <= 0.
     */
    [[nodiscard]] bool wants_read_input() const;

    /**
     * Verify the peer's certificate after the handshake completes.
     * Checks that the remote host provided a certificate and that the
     * certificate chain verification result is X509_V_OK.
     *
     * @throws ignite_error if verification fails.
     */
    void verify_peer() const;

private:
    /** SSL instance. */
    void* m_ssl { nullptr };

    /** Input memory BIO: ciphertext flows from network -> bio_in -> SSL. */
    void* m_bio_in { nullptr };

    /** Output memory BIO: ciphertext flows from SSL -> bio_out -> network. */
    void* m_bio_out { nullptr };
};

} // namespace ignite::network
