// SPDX-License-Identifier: CC-BY-NC-SA-4.0
package org.ingestbench.flink;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class SqlRunnerTest {
    @TempDir Path directory;

    @Test
    void readsDynamicConfigurationWithoutChangingSecretReferencesOnDisk() throws Exception {
        Path path = directory.resolve("conf.yaml");
        String original = "parallelism.default: 4\ncustom.secret: '${env:TOKEN}'\n";
        Files.writeString(path, original);
        assertEquals(Map.of("parallelism.default", "4", "custom.secret", "a$1\\b';\nnext"),
                SqlRunner.readConfiguration(path, Map.of("TOKEN", "a$1\\b';\nnext")));
        assertEquals(original, Files.readString(path));
        assertThrows(IllegalArgumentException.class,
                () -> SqlRunner.readConfiguration(path, Map.of()));
        Files.writeString(path, "- not-a-mapping\n");
        assertThrows(IllegalArgumentException.class,
                () -> SqlRunner.readConfiguration(path, Map.of()));
    }

    @Test
    void preservesEmbeddedSaslSemicolonsAndSubstitutesSecretsLiterally() {
        String sql = "CREATE TABLE source WITH ('auth' = 'Module required password=\"${env:TOKEN}\";');\n\n"
                + "INSERT INTO sink SELECT different_column FROM source;\n";
        List<String> statements = SqlRunner.readStatements(sql, Map.of("TOKEN", "a$1\\b"));
        assertEquals(2, statements.size());
        assertTrue(statements.get(0).contains("password=\"a$1\\b\";"));
        assertEquals("INSERT INTO sink SELECT different_column FROM source", statements.get(1));
        assertThrows(IllegalArgumentException.class, () -> SqlRunner.splitStatements(" \n"));
    }

    @Test
    void keepsApostrophesInsideSqlStringLiterals() {
        String sql = "CREATE CATALOG ice WITH ('credential' = '${env:TOKEN}');\n";
        assertEquals(List.of("CREATE CATALOG ice WITH ('credential' = 'svc:pa''ss')"),
                SqlRunner.readStatements(sql, Map.of("TOKEN", "svc:pa'ss")));
    }

    @Test
    void secretDelimitersCannotCreateStatements() {
        String sql = "CREATE CATALOG ice WITH ('credential' = '${env:TOKEN}');\n\n"
                + "INSERT INTO sink SELECT different_column FROM source;\n";
        assertEquals(List.of(
                        "CREATE CATALOG ice WITH ('credential' = 'svc:pa'';\nnext;\r\nline')",
                        "INSERT INTO sink SELECT different_column FROM source"),
                SqlRunner.readStatements(sql, Map.of("TOKEN", "svc:pa';\nnext;\r\nline")));
        assertThrows(IllegalArgumentException.class, () -> SqlRunner.readStatements(sql, Map.of()));
    }

    @Test
    void validatesArguments() {
        var options = SqlRunner.Options.parse(new String[]{"--conf", "conf.yaml", "--wait", "--sql", "job.sql"});
        assertEquals(Path.of("job.sql"), options.sql());
        assertEquals(Path.of("conf.yaml"), options.conf());
        assertTrue(options.waitForCompletion());
        assertThrows(IllegalArgumentException.class, () -> SqlRunner.Options.parse(new String[]{"--sql"}));
        assertThrows(IllegalArgumentException.class, () -> SqlRunner.Options.parse(new String[]{"--unknown"}));
        assertThrows(IllegalArgumentException.class, () -> SqlRunner.Options.parse(new String[]{}));
    }

    @Test
    void appliesConfigurationBeforeSqlAndWaitsOnlyWhenRequested() throws Exception {
        TableEnvironment environment = mock(TableEnvironment.class);
        TableConfig config = TableConfig.getDefault();
        when(environment.getConfig()).thenReturn(config);
        TableResult ddl = mock(TableResult.class);
        TableResult insert = mock(TableResult.class);
        when(environment.executeSql("DDL")).thenAnswer(invocation -> {
            assertEquals("8", config.getConfiguration().getString("parallelism.default", ""));
            return ddl;
        });
        when(environment.executeSql("INSERT")).thenReturn(insert);
        SqlRunner.execute(environment, Map.of("parallelism.default", "8"), List.of("DDL", "INSERT"), false);
        var order = inOrder(environment);
        order.verify(environment).executeSql("DDL");
        order.verify(environment).executeSql("INSERT");
        verify(insert, never()).await();
        SqlRunner.execute(environment, Map.of(), List.of("INSERT"), true);
        verify(insert).await();
        verify(ddl, never()).await();
    }
}
