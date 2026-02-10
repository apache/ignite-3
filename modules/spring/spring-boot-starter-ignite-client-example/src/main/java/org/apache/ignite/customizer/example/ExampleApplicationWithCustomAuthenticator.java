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

package org.apache.ignite.customizer.example;

import org.apache.ignite.IgniteClientPropertiesCustomizer;
import org.apache.ignite.client.BasicAuthenticator;
import org.apache.ignite.client.IgniteClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

/**
 * Example application that uses IgniteClientPropertiesCustomizer to set custom authenticator.
 */
@SpringBootApplication
public class ExampleApplicationWithCustomAuthenticator {
    public static void main(String[] args) {
        SpringApplication.run(ExampleApplicationWithCustomAuthenticator.class, args);
    }

    /**
     * This method can be used to set or override properties.
     */
    @Bean
    public IgniteClientPropertiesCustomizer customizeClient() {
        return cfg ->
                cfg.setAuthenticator(
                        BasicAuthenticator.builder().username("ignite").password("ignite").build()
                );
    }

    @Bean
    ApplicationRunner runner() {
        return new ApplicationRunner() {

            @Autowired
            private IgniteClient client;

            @Override public void run(ApplicationArguments args) throws Exception {
                client.sql().execute("CREATE TABLE IF NOT EXISTS Person (id INT PRIMARY KEY, name VARCHAR)");
                client.sql().execute("INSERT INTO Person (id, name) values (1, 'John')");

            }
        };
    }
}
