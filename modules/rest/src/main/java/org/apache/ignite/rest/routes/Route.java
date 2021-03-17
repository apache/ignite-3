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

package org.apache.ignite.rest.routes;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import org.apache.ignite.rest.netty.RestApiHttpRequest;
import org.apache.ignite.rest.netty.RestApiHttpResponse;

/**
 * URI route with appropriate handler for request.
 */
public class Route {

    /** Route. */
    private final String route;

    /** Method. */
    private final HttpMethod method;

    /** Accept type. */
    private final String acceptType;

    /** Handler. */
    private final BiConsumer<RestApiHttpRequest, RestApiHttpResponse> handler;

    /**
     * @param route Route.
     * @param method Method.
     * @param acceptType Accept type.
     * @param handler Request handler.
     */
    public Route(String route, HttpMethod method, String acceptType,
        BiConsumer<RestApiHttpRequest, RestApiHttpResponse> handler) {
        this.route = route;
        this.method = method;
        this.acceptType = acceptType;
        this.handler = handler;
    }

    /**
     * Handle the query by populating the received response object.
     *
     * @param req Request.
     * @param resp Response.
     */
    public void handle(FullHttpRequest req, RestApiHttpResponse resp) {
        handler.accept(new RestApiHttpRequest(req, paramsDecode(req.uri())),
            resp);
    }

    /**
     * Check if the current route matches the request.
     *
     * @param req Request.
     * @return true if route matches the request, else otherwise.
     */
    public boolean match(HttpRequest req) {
        return req.method().equals(method) &&
            matchUri(req.uri()) &&
            matchContentType(req.headers().get(HttpHeaderNames.CONTENT_TYPE));
    }

    /**
     * @param s Content type.
     * @return true if route matches the request, else otherwise.
     */
    private boolean matchContentType(String s) {
        return (acceptType == null) || (acceptType.equals(s));
    }

    /**
     * Check the current route matches input uri.
     * REST API like URIs "/user/:user" is also supported.
     *
     * @param uri Input URI
     * @return true if route matches the request, else otherwise.
     */
    private boolean matchUri(String uri) {
        var receivedParts = new ArrayDeque<>(Arrays.asList(uri.split("/")));
        var realparts = new ArrayDeque<>(Arrays.asList(route.split("/")));

        String part;
        while ((part = realparts.pollFirst()) != null) {
            String receivedPart = receivedParts.pollFirst();
            if (receivedPart == null)
                return false;

            if (part.startsWith(":"))
                continue;

            if (!part.equals(receivedPart))
                return false;
        }
        return receivedParts.isEmpty();
    }

    /**
     * Decode params from REST like URIs "/user/:user".
     *
     * @param uri Input URI.
     * @return Map of decoded params.
     */
    private Map<String, String> paramsDecode(String uri) {

        var receivedParts = new ArrayDeque<>(Arrays.asList(uri.split("/")));
        var realparts = new ArrayDeque<>(Arrays.asList(route.split("/")));

         Map<String, String> res = new HashMap<>();

        String part;
        while ((part = realparts.pollFirst()) != null) {
            String receivedPart = receivedParts.pollFirst();
            if (receivedPart == null)
                throw new IllegalArgumentException("URI is incorrect");

            if (part.startsWith(":")) {
                res.put(part.substring(1), receivedPart);
                continue;
            }

            if (!part.equals(receivedPart))
                throw new IllegalArgumentException("URI is incorrect");
        }
        return res;
    }

}
