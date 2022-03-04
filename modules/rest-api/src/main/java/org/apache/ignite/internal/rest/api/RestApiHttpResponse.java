/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.rest.api;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

/**
 * Simple wrapper of HTTP response with some helper methods for filling it with headers and content.
 */
public class RestApiHttpResponse {
    private static final ObjectMapper objectMapper = new ObjectMapper()
            .setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

    /** Response. */
    private final HttpResponse res;

    /** Content. */
    private byte[] content;

    /**
     * Creates a new HTTP response with the given message body.
     *
     * @param res     Response.
     * @param content Content.
     */
    public RestApiHttpResponse(HttpResponse res, byte[] content) {
        this.res = res;
        this.content = content;
    }

    /**
     * Creates a new HTTP response with the given headers and status.
     *
     * @param res Response.
     */
    public RestApiHttpResponse(HttpResponse res) {
        this.res = res;
    }

    /**
     * Set raw bytes as response body.
     *
     * @param content Content data.
     * @return Updated response.
     */
    public RestApiHttpResponse content(byte[] content) {
        this.content = content;
        return this;
    }

    /**
     * Returns content in a form of raw bytes.
     *
     * @return Content.
     */
    public byte[] content() {
        return content;
    }

    /**
     * Set JSON representation of input object as response body.
     *
     * @param content Content object.
     * @return Updated response.
     */
    public RestApiHttpResponse json(Object content) {
        try {
            this.content = objectMapper.writeValueAsBytes(content);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Unable to serialize JSON content", e);
        }

        headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON.toString());

        return this;
    }

    /**
     * Returns HTTP status of this response.
     *
     * @return HTTP Status.
     */
    public HttpResponseStatus status() {
        return res.status();
    }

    /**
     * Sets HTTP status.
     *
     * @param status Status.
     * @return Updated response.
     */
    public RestApiHttpResponse status(HttpResponseStatus status) {
        res.setStatus(status);
        return this;
    }

    /**
     * Returns protocol version.
     *
     * @return Protocol version
     */
    public HttpVersion protocolVersion() {
        return res.protocolVersion();
    }

    /**
     * Sets protocol version.
     *
     * @param httpVer HTTP version.
     * @return Updated response.
     */
    public RestApiHttpResponse protocolVersion(HttpVersion httpVer) {
        res.setProtocolVersion(httpVer);
        return this;
    }

    /**
     * Returns mutable response headers.
     *
     * @return Mutable response headers.
     */
    public HttpHeaders headers() {
        return res.headers();
    }
}
