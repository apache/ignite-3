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

package org.apache.ignite.internal.configuration.validation;

import java.net.MalformedURLException;
import java.net.URL;
import org.apache.ignite.configuration.validation.Endpoint;
import org.apache.ignite.configuration.validation.ValidationContext;
import org.apache.ignite.configuration.validation.ValidationIssue;
import org.apache.ignite.configuration.validation.Validator;

/** {@link Validator} implementation for the {@link Endpoint} annotation. */
public class EndpointValidator implements Validator<Endpoint, String> {
    @Override
    public void validate(Endpoint annotation, ValidationContext<String> ctx) {
        String endpoint = ctx.getNewValue();

        try {
            URL endpointUrl = new URL(endpoint);

            if (!"http".equalsIgnoreCase(endpointUrl.getProtocol()) && !"https".equalsIgnoreCase(endpointUrl.getProtocol())) {
                ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Endpoint scheme must be http or https: " + endpointUrl.getProtocol()));
            }

            if (endpointUrl.getQuery() != null) {
                ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Endpoint must not have a query string: " + endpointUrl.getQuery()));
            }

            if (endpointUrl.getRef() != null) {
                ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Endpoint must not have a fragment: " + endpointUrl.getRef()));
            }
        } catch (MalformedURLException e) {
            ctx.addIssue(new ValidationIssue(ctx.currentKey(), "Endpoint must be a valid URL: " + endpoint));
        }
    }
}
