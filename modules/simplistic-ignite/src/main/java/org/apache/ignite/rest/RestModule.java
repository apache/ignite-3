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

import java.util.Arrays;

import com.google.gson.JsonSyntaxException;
import io.javalin.Javalin;
import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.configuration.extended.ChangeLocal;
import org.apache.ignite.configuration.extended.Local;
import org.apache.ignite.configuration.extended.LocalConfiguration;
import org.apache.ignite.configuration.extended.Selectors;
import org.apache.ignite.configuration.internal.Configurator;
import org.apache.ignite.configuration.internal.selector.SelectorNotFoundException;
import org.apache.ignite.configuration.internal.validation.ConfigurationValidationException;
import org.apache.ignite.configuration.presentation.FormatConverter;
import org.apache.ignite.configuration.presentation.json.JsonConverter;

/** */
public class RestModule {
    /** */
    private static final int DFLT_PORT = 8080;

    /** */
    private static final String CONF_URL = "/management/v1/configuration/";

    /** */
    private static final String PATH_PARAM = "selector";

    /** */
    private final ConfigurationModule confModule;

    /** */
    public RestModule(ConfigurationModule confModule) {
        this.confModule = confModule;
    }

    /** */
    public void start() {
        Configurator<LocalConfiguration> configurator = confModule.localConfigurator();

        Integer port = configurator.getPublic(Selectors.LOCAL_REST_PORT);

        Javalin app = Javalin.create().start(port != null ? port : DFLT_PORT);

        FormatConverter converter = new JsonConverter();

        app.get(CONF_URL, ctx -> {
            Local local = configurator.getRoot().toView();

            ctx.result(converter.convertTo(local));
        });

        app.get(CONF_URL + ":" + PATH_PARAM, ctx -> {
            try {
                Object subTree = configurator.getPublic(Selectors.find(ctx.pathParam(PATH_PARAM)));

                String res = converter.convertTo(subTree);

                ctx.result(res);
            }
            catch (SelectorNotFoundException selectorE) {
                ErrorResult eRes = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", selectorE.getMessage());

                ctx.status(400).result(converter.convertTo(new ResponseWrapper(eRes)));
            }
        });

        app.post(CONF_URL, ctx -> {
            try {
                ChangeLocalWrapper local = converter.convertFrom(ctx.body(), ChangeLocalWrapper.class);

                configurator.set(Selectors.LOCAL, local.local);
            }
            catch (SelectorNotFoundException selectorE) {
                ErrorResult eRes = new ErrorResult("CONFIG_PATH_UNRECOGNIZED", selectorE.getMessage());

                ctx.status(400).result(converter.convertTo(new ResponseWrapper(eRes)));
            }
            catch (ConfigurationValidationException validationE) {
                ErrorResult eRes = new ErrorResult("APPLICATION_EXCEPTION", validationE.getMessage());

                ctx.status(400).result(converter.convertTo(new ResponseWrapper(eRes)));
            }
            catch (JsonSyntaxException e) {
                String msg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();

                ErrorResult eRes = new ErrorResult("VALIDATION_EXCEPTION", msg);

                ctx.status(400).result(converter.convertTo(new ResponseWrapper(eRes)));
            }
            catch (Exception e) {
                ErrorResult eRes = new ErrorResult("VALIDATION_EXCEPTION", e.getMessage());

                ctx.status(400).result(converter.convertTo(new ResponseWrapper(eRes)));
            }
        });
    }

    /** */
    private static class ResponseWrapper {
        /** */
        private final ErrorResult error;

        /** */
        private ResponseWrapper(ErrorResult error) {
            this.error = error;
        }
    }

    /** */
    private static class ChangeLocalWrapper {
        /** */
        private ChangeLocal local;
    }
}
