package com.rowforge.service;

import net.datafaker.Faker;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.Statements;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.rowforge.model.DatasetGeneration;
import com.rowforge.repository.DatasetGenerationRepository;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class DataGeneratorService {

    private static final Logger logger = LoggerFactory.getLogger(DataGeneratorService.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final DatasetGenerationRepository datasetGenerationRepository;

    public DataGeneratorService(DatasetGenerationRepository datasetGenerationRepository) {
        this.datasetGenerationRepository = datasetGenerationRepository;
    }


    public void logGeneration(String anomId, UUID userId, int rows, int tables) {
        DatasetGeneration record = new DatasetGeneration();
        record.setAnom_id(anomId);
        record.setUser_id(userId);
        record.setRows_generated(rows);
        record.setTables_generated(tables);
        
        datasetGenerationRepository.save(record);
    }

    /**
     * Parses the given SQL CREATE TABLE schema and generates fake rows.
     *
     * @param sql    one or more CREATE TABLE statements
     * @param rows   number of rows per table
     * @param format output format: SQL, CSV, or JSON
     * @return generated data as a String
     */
    public String generate(String sql, int rows, String format) {
        logger.info("Generating {} rows in {} format", rows, format);
        try {
            Statements stmts = CCJSqlParserUtil.parseStatements(sql);
            StringBuilder output = new StringBuilder();
            boolean first = true;

            for (Statement stmt : stmts.getStatements()) {
                if (stmt instanceof CreateTable createTable) {
                    if (!first) {
                        output.append("\n\n");
                    }
                    first = false;
                    output.append(generateForTable(createTable, rows, format));
                }
            }

            return output.toString();
        } catch (Exception e) {
            logger.error("Failed to parse SQL schema: {}", e.getMessage());
            throw new IllegalArgumentException("Invalid SQL schema: " + e.getMessage(), e);
        }
    }

    public String saveTrace() {



        return null;
    }

    private String generateForTable(CreateTable createTable, int rows, String format) {
        Faker faker = new Faker();
        String tableName = createTable.getTable().getName();
        List<ColumnDefinition> columns = createTable.getColumnDefinitions();

        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Table '" + tableName + "' has no column definitions.");
        }

        List<String> columnNames = columns.stream()
                .map(ColumnDefinition::getColumnName)
                .toList();

        List<List<String>> allRows = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            List<String> rowValues = new ArrayList<>();
            for (ColumnDefinition col : columns) {
                rowValues.add(generateValue(faker, col, i + 1));
            }
            allRows.add(rowValues);
        }

        return switch (format.toUpperCase()) {
            case "CSV"  -> buildCsv(columnNames, allRows);
            case "JSON" -> buildJson(tableName, columnNames, allRows);
            default     -> buildSql(tableName, columnNames, allRows);
        };
    }

    private String generateValue(Faker faker, ColumnDefinition col, int rowIndex) {
        String colName = col.getColumnName().toLowerCase();
        String typeName = col.getColDataType().getDataType().toUpperCase();

        // --- Infer by column name for realistic data ---
        if (colName.equals("id") || colName.endsWith("_id")) {
            return String.valueOf(rowIndex);
        }
        if (colName.contains("first_name") || colName.equals("firstname")) {
            return quote(faker.name().firstName());
        }
        if (colName.contains("last_name") || colName.equals("lastname")) {
            return quote(faker.name().lastName());
        }
        if (colName.equals("name") || colName.contains("full_name") || colName.contains("fullname")) {
            return quote(faker.name().fullName());
        }
        if (colName.contains("email")) {
            return quote(faker.internet().emailAddress());
        }
        if (colName.contains("phone") || colName.contains("mobile")) {
            return quote(faker.phoneNumber().phoneNumber());
        }
        if (colName.contains("address") || colName.contains("street")) {
            return quote(faker.address().streetAddress());
        }
        if (colName.contains("city")) {
            return quote(faker.address().city());
        }
        if (colName.contains("country")) {
            return quote(faker.address().country());
        }
        if (colName.contains("zip") || colName.contains("postal")) {
            return quote(faker.address().zipCode());
        }
        if (colName.contains("company") || colName.contains("employer")) {
            return quote(faker.company().name());
        }
        if (colName.contains("job") || colName.contains("title") || colName.contains("position")) {
            return quote(faker.job().title());
        }
        if (colName.contains("username") || colName.contains("user_name")) {
            return quote(faker.internet().username());
        }
        if (colName.contains("password") || colName.contains("pwd")) {
            return quote(faker.internet().password(8, 16));
        }
        if (colName.contains("url") || colName.contains("website")) {
            return quote(faker.internet().url());
        }
        if (colName.contains("description") || colName.contains("bio") || colName.contains("notes")) {
            return quote(faker.lorem().sentence());
        }
        if (colName.contains("birth") || colName.contains("dob")) {
            LocalDate dob = LocalDate.now().minusYears(faker.number().numberBetween(18, 80));
            return quote(dob.format(DATE_FORMATTER));
        }
        if (colName.contains("price") || colName.contains("amount") || colName.contains("salary")) {
            return String.format("%.2f", faker.number().randomDouble(2, 10, 10000));
        }
        if (colName.contains("age")) {
            return String.valueOf(faker.number().numberBetween(18, 80));
        }
        if (colName.contains("status")) {
            String[] statuses = {"active", "inactive", "pending", "suspended"};
            return quote(statuses[faker.number().numberBetween(0, statuses.length)]);
        }

        // --- Fall back to SQL data type ---
        return switch (typeName) {
            case "INT", "INTEGER", "SMALLINT", "TINYINT" ->
                    String.valueOf(faker.number().numberBetween(1, 10000));
            case "BIGINT" ->
                    String.valueOf(faker.number().numberBetween(1L, 1_000_000L));
            case "FLOAT", "REAL" ->
                    String.format("%.2f", faker.number().randomDouble(2, 1, 1000));
            case "DOUBLE", "DECIMAL", "NUMERIC" ->
                    String.format("%.2f", faker.number().randomDouble(2, 1, 10000));
            case "BOOLEAN", "BOOL", "BIT" ->
                    String.valueOf(faker.bool().bool());
            case "DATE" ->
                    quote(LocalDate.now().minusDays(faker.number().numberBetween(0, 3650))
                            .format(DATE_FORMATTER));
            case "DATETIME", "TIMESTAMP" ->
                    quote(LocalDate.now().minusDays(faker.number().numberBetween(0, 3650))
                            .atStartOfDay().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            case "TEXT", "LONGTEXT", "MEDIUMTEXT", "CLOB" ->
                    quote(faker.lorem().sentence());
            default ->
                    quote(faker.lorem().word()); // VARCHAR, CHAR, etc.
        };
    }

    private String quote(String value) {
        return "'" + value.replace("'", "''") + "'";
    }

    /** Returns true only if the value is a properly SQL-quoted string (starts and ends with '). */
    private boolean isSqlQuoted(String val) {
        return val.length() >= 2 && val.charAt(0) == '\'' && val.charAt(val.length() - 1) == '\'';
    }

    // ── Output builders ──────────────────────────────────────────────────────

    private String buildSql(String tableName, List<String> columnNames, List<List<String>> allRows) {
        StringBuilder sb = new StringBuilder();
        String cols = String.join(", ", columnNames);
        for (List<String> row : allRows) {
            sb.append(String.format("INSERT INTO %s (%s) VALUES (%s);%n",
                    tableName, cols, String.join(", ", row)));
        }
        return sb.toString();
    }

    private String buildCsv(List<String> columnNames, List<List<String>> allRows) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.join(",", columnNames)).append("\n");
        for (List<String> row : allRows) {
            List<String> csvRow = new ArrayList<>();
            for (String val : row) {
                // Strip SQL single-quotes; wrap in CSV double-quotes
                String stripped = isSqlQuoted(val)
                        ? val.substring(1, val.length() - 1).replace("''", "'")
                        : val;
                csvRow.add("\"" + stripped.replace("\"", "\"\"") + "\"");
            }
            sb.append(String.join(",", csvRow)).append("\n");
        }
        return sb.toString();
    }

    private String buildJson(String tableName, List<String> columnNames, List<List<String>> allRows) {
        StringBuilder sb = new StringBuilder();
        sb.append("{\n  \"table\": \"").append(tableName).append("\",\n  \"rows\": [\n");
        for (int i = 0; i < allRows.size(); i++) {
            sb.append("    {");
            List<String> row = allRows.get(i);
            for (int j = 0; j < columnNames.size(); j++) {
                String val = row.get(j);
                String jsonVal;
                if (isSqlQuoted(val)) {
                    String inner = val.substring(1, val.length() - 1).replace("''", "'");
                    jsonVal = "\"" + inner.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
                } else {
                    jsonVal = val; // numeric / boolean
                }
                sb.append("\"").append(columnNames.get(j)).append("\": ").append(jsonVal);
                if (j < columnNames.size() - 1) sb.append(", ");
            }
            sb.append("}");
            if (i < allRows.size() - 1) sb.append(",");
            sb.append("\n");
        }
        sb.append("  ]\n}");
        return sb.toString();
    }
}
