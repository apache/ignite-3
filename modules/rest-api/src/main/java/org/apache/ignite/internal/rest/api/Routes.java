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

import io.netty.handler.codec.http.HttpMethod;

/**
 * Allows to configure routes to REST handlers mapping.
 */
public interface Routes {
    /**
     * Adds the route to the router chain.
     *
     * @param route Route
     */
    void addRoute(Route route);

    /**
     * Adds a GET handler.
     *
     * @param route      Route.
     * @param acceptType Accept type.
     * @param hnd        Actual handler of the request.
     * @return Router
     */
    default Routes get(String route, String acceptType, RequestHandler hnd) {
        addRoute(new Route(route, HttpMethod.GET, acceptType, hnd));
        return this;
    }

    /**
     * Adds a GET handler.
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
     * Adds a PUT handler.
     *
     * @param route      Route.
     * @param acceptType Accept type.
     * @param hnd        Actual handler of the request.
     * @return Router
     */
    default Routes put(String route, String acceptType, RequestHandler hnd) {
        addRoute(new Route(route, HttpMethod.PUT, acceptType, hnd));
        return this;
    }

    /**
     * Adds a PATCH handler.
     *
     * @param route      Route.
     * @param acceptType Accept type.
     * @param hnd        Actual handler of the request.
     * @return Router
     */
    default Routes patch(String route, String acceptType, RequestHandler hnd) {
        addRoute(new Route(route, HttpMethod.PATCH, acceptType, hnd));
        return this;
    }

    /**
     * Adds a POST handler.
     *
     * @param route      Route.
     * @param acceptType Accept type.
     * @param hnd        Actual handler of the request.
     * @return Router
     */
    default Routes post(String route, String acceptType, RequestHandler hnd) {
        addRoute(new Route(route, HttpMethod.POST, acceptType, hnd));
        return this;
    }
}
