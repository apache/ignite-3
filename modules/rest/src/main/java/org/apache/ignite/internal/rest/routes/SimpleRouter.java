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

package org.apache.ignite.internal.rest.routes;

import io.netty.handler.codec.http.HttpRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.ignite.internal.rest.api.Route;
import org.apache.ignite.internal.rest.api.Routes;

/**
 * Dispatcher of http requests.
 *
 * <p>Example:
 * <pre>
 * {@code
 * var router = new SimpleRouter();
 * router.get("/user", (req, resp) -> {
 *     resp.status(HttpResponseStatus.OK);
 * });
 * }
 * </pre>
 */
public class SimpleRouter implements Router, Routes {
    /** Routes. */
    private final List<Route> routes;

    /**
     * Creates a new empty router.
     */
    public SimpleRouter() {
        routes = new ArrayList<>();
    }

    /**
     * Adds the route to router chain.
     *
     * @param route Route
     */
    @Override
    public void addRoute(Route route) {
        routes.add(route);
    }

    /**
     * Finds the route by request.
     *
     * @param req Request.
     * @return Route if founded.
     */
    @Override
    public Optional<Route> route(HttpRequest req) {
        return routes.stream().filter(r -> r.match(req)).findFirst();
    }
}
