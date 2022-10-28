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

package org.apache.ignite.internal.component;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.ignite.lang.ErrorGroups.Common;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Test for {@link RestAddressReporter}. */
class RestAddressReporterTest {
    private static final String REST_ADDRESS_FILENAME = "rest-address";

    @Test
    @DisplayName("REST server network address is reported to file")
    void networkAddressReported(@TempDir Path tmpDir) throws IOException {
        // Given
        RestAddressReporter reporter = new RestAddressReporter(tmpDir);

        // When
        reporter.writeReport(new NetworkAddress("localhost", 9999));

        // Then there is a report
        String restAddress = Files.readString(tmpDir.resolve(REST_ADDRESS_FILENAME));
        assertThat(restAddress, equalTo("localhost:9999"));
    }

    @Test
    @DisplayName("File with network address is removed")
    void reportDeleted(@TempDir Path tmpDir) throws IOException {
        // Given reported address
        RestAddressReporter reporter = new RestAddressReporter(tmpDir);
        reporter.writeReport(new NetworkAddress("localhost", 9999));
        // And file exists
        assertThat(Files.exists(tmpDir.resolve(REST_ADDRESS_FILENAME)), is(true));

        // When
        reporter.removeReport();

        // Then file is removed
        assertThat(Files.exists(tmpDir.resolve(REST_ADDRESS_FILENAME)), is(false));
    }

    @Test
    @DisplayName("If there is not report file for some reason then throw an exception")
    void throwsExceptionWhenThereIsNoFile(@TempDir Path tmpDir) {
        // Given
        Path path = Path.of(tmpDir.toUri() + "/nosuchpath");
        RestAddressReporter reporter = new RestAddressReporter(path);

        // When try to removeReport
        IgniteException thrown = assertThrows(IgniteException.class, reporter::removeReport);

        // Then exception thrown with proper message
        assertThat(thrown.getMessage(), containsString("Unexpected error when trying to remove REST server network address file"));
        // And it has COMMON error group
        assertThat(thrown.groupName(), equalTo(Common.COMMON_ERR_GROUP.name()));
    }

    @Test
    @DisplayName("If there is a file with report then it should be rewritten")
    void rewritesAlreadyExistingFile(@TempDir Path tmpDir) throws IOException {
        // Given reported address to file
        Files.writeString(
                tmpDir.resolve(REST_ADDRESS_FILENAME),
                new NetworkAddress("localhost", 9999).toString()
        );

        // When try to write it again but with another port
        new RestAddressReporter(tmpDir).writeReport(new NetworkAddress("localhost", 4444));

        // Then file rewritten
        String restAddress = Files.readString(tmpDir.resolve(REST_ADDRESS_FILENAME));
        assertThat(restAddress, equalTo("localhost:4444"));
    }
}
