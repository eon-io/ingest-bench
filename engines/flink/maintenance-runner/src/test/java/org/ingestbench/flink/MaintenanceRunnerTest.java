// SPDX-License-Identifier: CC-BY-NC-SA-4.0
package org.ingestbench.flink;

import org.apache.iceberg.flink.maintenance.api.RewriteDataFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class MaintenanceRunnerTest {
    @TempDir Path directory;

    private static final String DOCUMENT = """
            catalog: ice
            catalog_properties:
              uri: http://iceberg-rest:8181
              warehouse: s3://warehouse/
              s3.secret-access-key: '${env:SECRET}'
            table: ingest_bench.t_run
            uid_suffix: run-20260919T000000Z
            parallelism: 6
            interval_s: 600
            rate_limit_s: 15
            lock_check_delay_s: 10
            max_read_back: 100
            rewrite_options:
              min-input-files: "8"
              rewrite-all: "false"
            """;

    @Test
    void readsTheDocumentAndResolvesReferencesOnlyInMemory() throws Exception {
        Path path = directory.resolve("compactor-job.yaml");
        Files.writeString(path, DOCUMENT);
        MaintenanceRunner.Job job = MaintenanceRunner.Job.read(path, Map.of("SECRET", "p$1\\w"));
        assertEquals("ice", job.catalog());
        assertEquals("p$1\\w", job.catalogProperties().get("s3.secret-access-key"));
        assertEquals("ingest_bench.t_run", job.table());
        assertEquals(6, job.parallelism());
        assertEquals(600, job.intervalS());
        assertEquals(Map.of("min-input-files", "8", "rewrite-all", "false"), job.rewriteOptions());
        assertEquals(DOCUMENT, Files.readString(path));
        assertThrows(IllegalArgumentException.class, () -> MaintenanceRunner.Job.read(path, Map.of()));
    }

    @Test
    void everyFieldIsRequired() throws Exception {
        Path path = directory.resolve("short.yaml");
        Files.writeString(path, DOCUMENT.replace("max_read_back: 100\n", ""));
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> MaintenanceRunner.Job.read(path, Map.of("SECRET", "x")));
        assertEquals("the job document sets no max_read_back", error.getMessage());
        Files.writeString(path, DOCUMENT.replace("parallelism: 6", "parallelism: 0"));
        assertThrows(IllegalArgumentException.class, () -> MaintenanceRunner.Job.read(path, Map.of("SECRET", "x")));
    }

    @Test
    void knownOptionsReachTheBuilderAndUnknownOnesAreRefused() {
        RewriteDataFiles.Builder builder = RewriteDataFiles.builder();
        assertSame(builder, MaintenanceRunner.applyOptions(builder, Map.of(
                "min-input-files", "8", "min-file-size-bytes", "268435456", "max-file-group-size-bytes", "2147483648",
                "rewrite-all", "false", "partial-progress.enabled", "false")));
        assertThrows(IllegalArgumentException.class,
                () -> MaintenanceRunner.applyOptions(RewriteDataFiles.builder(), Map.of("min_input_files", "8")));
        assertThrows(IllegalArgumentException.class,
                () -> MaintenanceRunner.applyOptions(RewriteDataFiles.builder(), Map.of("rewrite-all", "yes")));
        assertThrows(NumberFormatException.class,
                () -> MaintenanceRunner.applyOptions(RewriteDataFiles.builder(), Map.of("min-input-files", "eight")));
    }

    @Test
    void theJobOrderIsCheckedHereAndAppliedThroughTheConfigPath() {
        assertSame(RewriteDataFiles.builder().getClass(),
                MaintenanceRunner.applyOptions(RewriteDataFiles.builder(), Map.of("rewrite-job-order", "bytes-desc")).getClass());
        assertEquals(java.util.Optional.of("bytes-desc"), MaintenanceRunner.jobOrder(Map.of("rewrite-job-order", "bytes-desc")));
        assertEquals(java.util.Optional.empty(), MaintenanceRunner.jobOrder(Map.of("min-input-files", "8")));
        assertThrows(IllegalArgumentException.class,
                () -> MaintenanceRunner.applyOptions(RewriteDataFiles.builder(), Map.of("rewrite-job-order", "largest-first")));
    }

    @Test
    void theOnlyArgumentIsTheJobPath() {
        assertEquals(Path.of("/opt/bench/run/compactor-job.yaml"),
                MaintenanceRunner.jobPath(new String[] {"--maintenance", "/opt/bench/run/compactor-job.yaml"}));
        assertThrows(IllegalArgumentException.class, () -> MaintenanceRunner.jobPath(new String[] {"--job", "x"}));
        assertThrows(IllegalArgumentException.class, () -> MaintenanceRunner.jobPath(new String[] {}));
    }
}
