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

import com.google.gson.JsonSyntaxException;
import io.javalin.Javalin;
import java.io.Reader;
import org.apache.ignite.configuration.Configurator;
import org.apache.ignite.configuration.SystemConfiguration;
import org.apache.ignite.configuration.internal.selector.SelectorNotFoundException;
import org.apache.ignite.configuration.presentation.FormatConverter;
import org.apache.ignite.configuration.presentation.json.JsonConverter;
import org.apache.ignite.configuration.storage.ConfigurationStorage;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.rest.configuration.ChangeRest;
import org.apache.ignite.rest.configuration.InitRest;
import org.apache.ignite.rest.configuration.RestConfigurationImpl;
import org.apache.ignite.rest.configuration.Selectors;
import org.slf4j.Logger;

/** */
public class RestModule {
    /** */
    private static final int DFLT_PORT = 8080;

    /** */
    private static final String CONF_URL = "/management/v1/configuration/";

    /** */
    private static final String PATH_PARAM = "selector";

    /** */
    private SystemConfiguration sysConf;

    /** */
    private final Logger log;

    /** */
    public RestModule(Logger log) {
        this.log = log;
    }

    /** */
    public void prepareStart(SystemConfiguration sysConfig, Reader moduleConfReader, ConfigurationStorage storage) {
        sysConf = sysConfig;

        FormatConverter converter = new JsonConverter();

        Configurator<RestConfigurationImpl> restConf = Configurator.create(storage, RestConfigurationImpl::new,
            converter.convertFrom(moduleConfReader, "rest", InitRest.class));

        sysConfig.registerConfigurator(restConf,
            s -> {
                ChangeRest chRest = converter.convertFrom(s, "rest", ChangeRest.class);

                restConf.set(Selectors.REST, chRest);

                return null;
            } );
    }

    /** */
    public void start() {
        Javalin app = startRestEndpoint();

        FormatConverter converter = new JsonConverter();

        app.get(CONF_URL, ctx -> {
            ctx.result(sysConf.presentation().present());
        });

        app.get(CONF_URL + ":" + PATH_PARAM, ctx -> {
            String configPath = ctx.pathParam(PATH_PARAM);

            try {
                ctx.result(sysConf.presentation().presentByPath(configPath));
            }
            catch (SelectorNotFoundException | IllegalArgumentException pathE) {
                ErrorResult eRes = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", pathE.getMessage());

                ctx.status(400).result(converter.convertTo("error", eRes));
            }
        });

        app.post(CONF_URL, ctx -> {
            String root = converter.rootName(ctx.body());

            if (root != null) {
                try {
                    sysConf.updateConfigurationProperty(root, ctx.body());
                }
                catch (SelectorNotFoundException selectorE) {
                    ErrorResult eRes = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", selectorE.getMessage());

                    ctx.status(400).result(converter.convertTo("error", eRes));
                }
                catch (ConfigurationValidationException validationE) {
                    ErrorResult eRes = new ErrorResult("APPLICATION_EXCEPTION", validationE.getMessage());

                    ctx.status(400).result(converter.convertTo("error", eRes));
                }
                catch (JsonSyntaxException e) {
                    String msg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();

                    ErrorResult eRes = new ErrorResult("VALIDATION_EXCEPTION", msg);

                    ctx.status(400).result(converter.convertTo("error", eRes));
                }
                catch (Exception e) {
                    ErrorResult eRes = new ErrorResult("VALIDATION_EXCEPTION", e.getMessage());

                    ctx.status(400).result(converter.convertTo("error", eRes));
                }
            }
            else
                ctx.status(400).result(
                    converter.convertTo("error",
                        new ErrorResult("CONFIG_PATH_UNRECOGNIZED", "Malformed input")
                    )
                );
        });
    }

    /** */
    private Javalin startRestEndpoint() {
        Configurator<RestConfigurationImpl> restConf = sysConf.getConfigurator("rest");

        Integer port = restConf.getPublic(Selectors.REST_PORT);
        Integer portRange = restConf.getPublic(Selectors.REST_PORT_RANGE);

        Javalin app = null;

        if (portRange == null || portRange == 0) {
            try {
                app = Javalin.create().start(port != null ? port : DFLT_PORT);
            }
            catch (RuntimeException e) {
                log.warn("Failed to start REST endpoint: ", e);

                throw e;
            }
        }
        else {
            int startPort = port;

            for (int portCandidate = startPort; portCandidate < startPort + portRange; portCandidate++) {
                try {
                    app = Javalin.create().start(portCandidate);
                }
                catch (RuntimeException ignored) {
                    // No-op.
                }

                if (app != null)
                    break;
            }

            if (app == null) {
                String msg = "Cannot start REST endpoint. " +
                    "All ports in range [" + startPort + ", " + (startPort + portRange) + ") are in use.";

                log.warn(msg);

                throw new RuntimeException(msg);
            }
        }

        log.info("REST protocol started successfully on port " + app.port());

        return app;
    }

    /** */
    public String configRootKey() {
        return "rest";
    }
}
