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

package org.apache.ignite.app;

import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.ignite.configuration.ConfigurationModule;
import org.apache.ignite.rest.RestModule;

/**
 *
 */
public class SimplisticIgnite {
    /** */
    private static final String CONF_PARAM_NAME = "--config";

    /** */
    private static final String DFLT_CONF_FILE_NAME = "bootstrap-config.json";

    /**
     *
     * @param args
     */
    public static void main(String[] args) throws IOException {
        ConfigurationModule confModule = new ConfigurationModule();

        Reader confReader = null;

        try {
            if (args != null) {
                for (int i = 0; i < args.length; i++) {
                    if (CONF_PARAM_NAME.equals(args[i]) && i + 1 < args.length) {
                        confReader = new FileReader(args[i + 1]);

                        break;
                    }
                }
            }

            if (confReader == null) {
                confReader = new InputStreamReader(
                    SimplisticIgnite.class.getClassLoader().getResourceAsStream(DFLT_CONF_FILE_NAME));
            }

            confModule.bootstrap(confReader);
        }
        finally {
            if (confReader != null)
                confReader.close();
        }

        RestModule rest = new RestModule(confModule);

        rest.start();
    }
}
