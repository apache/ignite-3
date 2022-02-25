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

package org.apache.ignite.rest;

import io.netty.handler.codec.http.HttpMethod;
import io.netty.util.AsciiString;

/**
 * Allows to configure REST handlers vs routes.
 */
public interface Routes {
    /**
     * Adds the route to router chain.
     *
     * @param route Route
     */
    void addRoute(Route route);

    /**
     * GET query helper.
     *
     * @param route      Route.
     * @param acceptType Accept type.
     * @param hnd        Actual handler of the request.
     * @return Router
     */
    default Routes get(String route, AsciiString acceptType, RequestHandler hnd) {
        addRoute(new Route(route, HttpMethod.GET, acceptType.toString(), hnd));
        return this;
    }

    /**
     * GET query helper.
     *
     * @param route Route.
     * @param hnd   Actual handler of the request.
     * @return Router
     */
    default Routes get(String route, RequestHandler hnd) {
        addRoute(new Route(route, HttpMethod.GET, null, hnd));
        return this;
    }

    /**
     * PUT query helper.
     *
     * @param route      Route.
     * @param acceptType Accept type.
     * @param hnd        Actual handler of the request.
     * @return Router
     */
    default Routes put(String route, AsciiString acceptType, RequestHandler hnd) {
        addRoute(new Route(route, HttpMethod.PUT, acceptType.toString(), hnd));
        return this;
    }

    /**
     * Defines a PATCH route.
     *
     * @param route      Route.
     * @param acceptType Accept type.
     * @param hnd        Actual handler of the request.
     * @return Router
     */
    default Routes patch(String route, AsciiString acceptType, RequestHandler hnd) {
        addRoute(new Route(route, HttpMethod.PATCH, acceptType.toString(), hnd));
        return this;
    }
}
