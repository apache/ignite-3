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

package org.apache.ignite.configuration.internal;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.Serializable;
import java.io.StringReader;
import java.util.Scanner;
import java.util.function.Consumer;
import org.apache.ignite.configuration.internal.selector.Selector;

public class CLIExample {

    //{local:{baseline:{auto_adjust:{enabled:false}}}}
    //{local:{baseline:{auto_adjust:{timeout:10000}}}}
    public static void main(String[] args) throws FileNotFoundException {
        // Remove selectors map
        final ConfigurationStorage storage = new ConfigurationStorage() {

            @Override
            public <T extends Serializable> void save(String propertyName, T object) {

            }

            @Override
            public <T extends Serializable> T get(String propertyName) {
                return null;
            }

            @Override
            public <T extends Serializable> void listen(String key, Consumer<T> listener) {

            }
        };

        Selectors.CLUSTER_BASELINE_NODES_CONSISTENT_ID("");

        final Configurator<LocalConfiguration> configurator = new Configurator<>(storage, LocalConfiguration::new);

        final String filePath = args[0];
        FileReader reader = new FileReader(filePath);

        final Config config = ConfigFactory.parseReader(reader);
        config.resolve();

        applyConfig(configurator, config);

        System.out.print("$ ");
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String line = scanner.nextLine();
            try {
                if (line.startsWith("set ")) {
                    line = line.substring(4);

                    final Config setConfig = ConfigFactory.parseReader(new StringReader(line));
                    setConfig.resolve();

                    applyConfig(configurator, setConfig);
                } else if (line.startsWith("get ")) {
                    line = line.substring(4);
                    final Selector selector = Selectors.find(line);
                    final Object view = configurator.getPublic(selector);
                    System.out.println(view);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.print("$ ");
        }
    }

    public static void applyConfig(Configurator<LocalConfiguration> configurator, Config config) {
        config.entrySet().forEach(entry -> {
            final String key = entry.getKey();
            final Object value = entry.getValue().unwrapped();
            final Selector selector = Selectors.find(key);
            if (selector != null)
                configurator.set(selector, value);
        });
    }

}
