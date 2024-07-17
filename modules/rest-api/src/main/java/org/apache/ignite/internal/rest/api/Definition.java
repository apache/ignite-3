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

package org.apache.ignite.internal.rest.api;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.enums.SecuritySchemeType;
import io.swagger.v3.oas.annotations.info.Contact;
import io.swagger.v3.oas.annotations.info.Info;
import io.swagger.v3.oas.annotations.info.License;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.security.SecurityScheme;
import io.swagger.v3.oas.annotations.servers.Server;

/**
 * Dummy class used to generate OpenAPI specification.
 */
@OpenAPIDefinition(
        servers = @Server(url = "http://localhost:10300"),
        info = @Info(
                title = "Ignite REST module",
                version = "3.0.0-SNAPSHOT",
                license = @License(name = "Apache 2.0", url = "https://ignite.apache.org"),
                contact = @Contact(email = "user@ignite.apache.org")))
@SecurityRequirement(name = "basicAuth")
@SecurityScheme(name = "basicAuth", scheme = "basic", type = SecuritySchemeType.HTTP)
public class Definition {
}
