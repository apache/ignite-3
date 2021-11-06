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

package org.apache.ignite.cli.ui;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class SpinnerTest {
    /**
     *
     */
    private PrintWriter out;

    /**
     *
     */
    private ByteArrayOutputStream outputStream;

    /**
     *
     */
    @BeforeEach
    public void setUp() {
        outputStream = new ByteArrayOutputStream();
        out = new PrintWriter(outputStream);
    }

    /**
     *
     */
    @AfterEach
    public void tearDown() throws IOException {
        out.close();
        outputStream.close();
    }

    /**
     *
     */
    @Test
    public void testSpinner() {
        var spinner = new Spinner(out, "Waiting");

        spinner.spin();
        assertEquals("\rWaiting.  ", outputStream.toString());

        spinner.spin();
        assertEquals("\rWaiting.  \rWaiting.. ", outputStream.toString());

        spinner.spin();
        assertEquals("\rWaiting.  \rWaiting.. \rWaiting...", outputStream.toString());

        spinner.spin();
        assertEquals("\rWaiting.  \rWaiting.. \rWaiting...\rWaiting.  ", outputStream.toString());
    }

    /**
     *
     */
    @Test
    public void testSpinnerClose() {
        var spinner = new Spinner(out, "Waiting");

        spinner.spin();
        spinner.close();

        assertEquals("\rWaiting.  \rWaiting..." + System.lineSeparator(), outputStream.toString());
    }
}
