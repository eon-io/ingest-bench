// SPDX-License-Identifier: CC-BY-NC-SA-4.0
package org.ingestbench.flink;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Executes the SQL and configuration staged for a run; connectors process all records. */
public final class SqlRunner {
    private static final Pattern ENV_PLACEHOLDER = Pattern.compile("\\$\\{env:([A-Za-z_][A-Za-z0-9_]*)}");

    private SqlRunner() {}

    record Options(Path sql, Path conf, boolean waitForCompletion) {
        static Options parse(String[] args) {
            Path sql = null;
            Path conf = null;
            boolean waitForCompletion = false;
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "--sql", "--conf" -> {
                        String option = args[i];
                        if (++i == args.length || args[i].startsWith("--")) {
                            throw new IllegalArgumentException(option + " requires a path");
                        }
                        if (option.equals("--sql")) {
                            sql = Path.of(args[i]);
                        } else {
                            conf = Path.of(args[i]);
                        }
                    }
                    case "--wait" -> waitForCompletion = true;
                    default -> throw new IllegalArgumentException("Unknown argument: " + args[i]);
                }
            }
            if (sql == null || conf == null) {
                throw new IllegalArgumentException("Usage: --sql PATH --conf PATH [--wait]");
            }
            return new Options(sql, conf, waitForCompletion);
        }
    }

    static String substituteEnvironment(String text, Map<String, String> environment) {
        return substituteEnvironment(text, environment, false);
    }

    private static String substituteEnvironment(String text, Map<String, String> environment, boolean sqlLiteral) {
        return ENV_PLACEHOLDER.matcher(text).replaceAll(match -> {
            String name = match.group(1);
            String value = environment.get(name);
            if (value == null) {
                throw new IllegalArgumentException("${env:" + name + "} is not set in the environment");
            }
            return Matcher.quoteReplacement(sqlLiteral ? value.replace("'", "''") : value);
        });
    }

    static Map<String, String> readConfiguration(Path path, Map<String, String> environment) throws IOException {
        Object loaded = new Yaml(new SafeConstructor(new LoaderOptions())).load(Files.readString(path));
        if (!(loaded instanceof Map<?, ?> values)) {
            throw new IllegalArgumentException(path + " must hold a mapping of Flink setting to value");
        }
        Map<String, String> configuration = new LinkedHashMap<>();
        values.forEach((key, value) -> configuration.put(
                String.valueOf(key), substituteEnvironment(String.valueOf(value), environment)));
        return configuration;
    }

    static List<String> splitStatements(String sql) {
        // Match the renderer's line-ending delimiter; SASL options contain embedded semicolons.
        List<String> statements = Arrays.stream(sql.replace("\r\n", "\n").split(";\n"))
                .map(String::strip)
                .filter(statement -> !statement.isEmpty())
                .toList();
        if (statements.isEmpty()) {
            throw new IllegalArgumentException("SQL script holds no statements");
        }
        return statements;
    }

    static List<String> readStatements(String sql, Map<String, String> environment) {
        // Rendered SQL puts references in string literals. Split before substitution so secret
        // contents cannot introduce statements, and escape quotes to keep each value in its literal.
        return splitStatements(sql).stream()
                .map(statement -> substituteEnvironment(statement, environment, true))
                .toList();
    }

    static void execute(TableEnvironment table, Map<String, String> configuration,
                        List<String> statements, boolean waitForCompletion) throws Exception {
        configuration.forEach((key, value) -> table.getConfig().set(key, value));
        for (int i = 0; i < statements.size() - 1; i++) {
            table.executeSql(statements.get(i));
        }
        // The staged script ends with its INSERT. Detached clients must return after submission.
        TableResult result = table.executeSql(statements.get(statements.size() - 1));
        if (waitForCompletion) {
            result.await();
        }
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        Map<String, String> environment = System.getenv();
        Map<String, String> configuration = readConfiguration(options.conf(), environment);
        List<String> statements = readStatements(Files.readString(options.sql()), environment);
        TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        execute(table, configuration, statements, options.waitForCompletion());
    }
}
