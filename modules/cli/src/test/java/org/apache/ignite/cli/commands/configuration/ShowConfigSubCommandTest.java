package org.apache.ignite.cli.commands.configuration;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.ignite.cli.commands.CliCommandTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ShowConfigSubCommandTest extends CliCommandTestBase {

    @BeforeEach
    void setUp() {
        setUp(ShowConfigSubCommand.class);
    }

    @Disabled(value = "Cluster-url has a default value")
    @Test
    @DisplayName("Cluster-url is mandatory option")
    void mandatoryOptions() {
        // When execute without --cluster-url
        execute();

        // Then
        assertThat(err.toString()).contains("Missing required option: '--cluster-url=<clusterUrl>'");
        // And
        assertThat(out.toString()).isEmpty();
    }
}