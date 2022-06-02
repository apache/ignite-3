package org.apache.ignite.cli.commands.cliconfig;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import org.apache.ignite.cli.config.Config;
import org.junit.jupiter.api.Test;

class ConfigTest {
    @Test
    public void testSaveLoadConfig() throws IOException {
        File tempFile = File.createTempFile("cli", null);
        Config config = new Config(tempFile);
        config.setProperty("ignite.cluster-url", "test");
        config.saveConfig();

        Config configAfterSave = new Config(tempFile);
        assertThat(configAfterSave.getProperty("ignite.cluster-url")).isEqualTo("test");
    }
}
