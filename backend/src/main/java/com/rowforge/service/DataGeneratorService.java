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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.ai.chat.client.ChatClient;
import com.fasterxml.jackson.databind.ObjectMapper;

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
    private final String supabaseDbUrl;
    private final ChatClient chatClient;
    private final ObjectMapper objectMapper;

    public DataGeneratorService(
            DatasetGenerationRepository datasetGenerationRepository,
            ChatClient chatClient,
            @Value("${SUPABASE_DB_URL:}") String supabaseDbUrl) {
        this.datasetGenerationRepository = datasetGenerationRepository;
        this.chatClient = chatClient;
        this.supabaseDbUrl = supabaseDbUrl;
        this.objectMapper = new ObjectMapper();
    }


    public void logGeneration(String anonId, UUID userId, int rows, int tables) {
        if (!isDatabaseConfigured()) {
            logger.warn("Skipping generation persistence: SUPABASE_DB_URL is not configured.");
            return;
        }

        DatasetGeneration record = new DatasetGeneration();
        record.setAnon_id(anonId);
        record.setUser_id(userId);
        record.setRows_generated(rows);
        record.setTables_generated(tables);

        try {
            datasetGenerationRepository.save(record);
        } catch (Exception e) {
            logger.warn("Skipping generation persistence: {}", e.getMessage());
        }
    }

    private boolean isDatabaseConfigured() {
        return supabaseDbUrl != null && !supabaseDbUrl.isBlank() && !supabaseDbUrl.contains("${");
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

        // Get AI-determined Faker statements for each column
        Map<String, String> fakerStatements = getAIFakerStatements(tableName, columns);

        List<List<String>> allRows = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            List<String> rowValues = new ArrayList<>();
            for (ColumnDefinition col : columns) {
                String colName = col.getColumnName();
                String fakerStatement = fakerStatements.getOrDefault(colName, "faker.datatype.string()");
                rowValues.add(generateValueFromFaker(faker, col, fakerStatement, i + 1));
            }
            allRows.add(rowValues);
        }

        return switch (format.toUpperCase()) {
            case "CSV"  -> buildCsv(columnNames, allRows);
            case "JSON" -> buildJson(tableName, columnNames, allRows);
            default     -> buildSql(tableName, columnNames, allRows);
        };
    }

    private Map<String, String> getAIFakerStatements(String tableName, List<ColumnDefinition> columns) {
        try {
            StringBuilder columnsInfo = new StringBuilder();
            for (ColumnDefinition col : columns) {
                String dataType = col.getColDataType().getDataType();
                columnsInfo.append(String.format("- %s (%s)\n", col.getColumnName(), dataType));
            }

            String prompt = String.format("""
                    Analyze this database table schema and suggest the BEST datafaker (Java Faker library) method for each column.
                    Return ONLY a JSON object with column names as keys and Faker method calls as values.
                    Use the Java Faker library syntax (e.g., faker.name().firstName(), faker.internet().emailAddress(), etc.)
                    
                    Table: %s
                    Columns:
                    %s
                    
                    IMPORTANT: Return ONLY valid JSON, no markdown, no explanations, no code blocks.
                    Example response:
                    {"id": "faker.number().numberBetween(1, 10000)", "name": "faker.name().fullName()", "email": "faker.internet().emailAddress()}""",
                    tableName, columnsInfo.toString());

            String response = chatClient.prompt()
                    .user(prompt)
                    .call()
                    .content();

            logger.debug("AI Faker suggestions: {}", response);
            return parseAIResponse(response);
        } catch (Exception e) {
            logger.warn("Failed to get AI suggestions, using defaults: {}", e.getMessage());
            return new HashMap<>();
        }
    }

    private Map<String, String> parseAIResponse(String response) {
        try {
            String jsonString = response.trim();
            // Handle markdown code blocks if present
            if (jsonString.contains("```json")) {
                jsonString = jsonString.substring(jsonString.indexOf("```json") + 7, jsonString.lastIndexOf("```"));
            } else if (jsonString.contains("```")) {
                jsonString = jsonString.substring(jsonString.indexOf("```") + 3, jsonString.lastIndexOf("```"));
            }
            jsonString = jsonString.trim();
            
            return objectMapper.readValue(jsonString, Map.class);
        } catch (Exception e) {
            logger.error("Failed to parse AI response: {}", e.getMessage());
            return new HashMap<>();
        }
    }

    private String generateValueFromFaker(Faker faker, ColumnDefinition col, String fakerStatement, int rowIndex) {
        try {
            // Handle special cases
            String colName = col.getColumnName().toLowerCase();
            
            if (colName.equals("id") || colName.endsWith("_id")) {
                return String.valueOf(rowIndex);
            }

            // Execute the Faker statement dynamically
            Object result = executeFakerStatement(faker, fakerStatement);
            
            // Determine if we need to quote this value
            String typeName = col.getColDataType().getDataType().toUpperCase();
            boolean needsQuotes = isStringType(typeName);

            if (result == null) {
                return needsQuotes ? "''" : "NULL";
            }

            String stringResult = result.toString();
            return needsQuotes ? quote(stringResult) : stringResult;
        } catch (Exception e) {
            logger.warn("Failed to execute Faker statement '{}': {}, using fallback", fakerStatement, e.getMessage());
            return quote(faker.lorem().word());
        }
    }

    private Object executeFakerStatement(Faker faker, String statement) {
        try {
            // Dynamic execution: Parse "faker.method1().method2(args)" and execute via reflection
            // Example: "faker.name().firstName()" 
            // Example: "faker.number().numberBetween(1, 100)"
            
            if (statement == null || statement.isBlank()) {
                return faker.lorem().word();
            }

            // Remove "faker." prefix if present
            String chain = statement.replace("faker.", "");
            
            // Split by "()" to get method calls
            String[] methodCalls = chain.split("\\(\\)");
            
            Object current = faker;
            
            for (int i = 0; i < methodCalls.length; i++) {
                String methodCall = methodCalls[i];
                
                // Skip empty strings
                if (methodCall.isBlank()) continue;
                
                // Check if this method call has parameters: method(arg1, arg2)
                if (methodCall.contains("(")) {
                    String methodName = methodCall.substring(0, methodCall.indexOf("("));
                    String argsStr = methodCall.substring(methodCall.indexOf("(") + 1);
                    
                    // Parse parameters
                    Object[] args = parseArguments(argsStr);
                    Class<?>[] paramTypes = getParamTypes(args);
                    
                    // Invoke with parameters
                    var method = current.getClass().getMethod(methodName, paramTypes);
                    current = method.invoke(current, args);
                } else {
                    // Method call without parameters: method()
                    String methodName = methodCall.trim();
                    if (!methodName.isBlank()) {
                        var method = current.getClass().getMethod(methodName);
                        current = method.invoke(current);
                    }
                }
            }
            
            return current;
        } catch (Exception e) {
            logger.debug("Error executing Faker statement '{}': {}", statement, e.getMessage());
            return faker.lorem().word();
        }
    }

    private Object[] parseArguments(String argsStr) {
        if (argsStr == null || argsStr.isBlank()) {
            return new Object[0];
        }
        
        // Split by comma but be aware of nested structures
        String[] argParts = argsStr.split(",");
        Object[] args = new Object[argParts.length];
        
        for (int i = 0; i < argParts.length; i++) {
            String arg = argParts[i].trim();
            
            // Try to parse as integer
            try {
                args[i] = Integer.parseInt(arg);
                continue;
            } catch (NumberFormatException e) {
                // Not an integer
            }
            
            // Try to parse as long
            try {
                args[i] = Long.parseLong(arg);
                continue;
            } catch (NumberFormatException e) {
                // Not a long
            }
            
            // Try to parse as double
            try {
                args[i] = Double.parseDouble(arg);
                continue;
            } catch (NumberFormatException e) {
                // Not a double
            }
            
            // Treat as string
            args[i] = arg;
        }
        
        return args;
    }

    private Class<?>[] getParamTypes(Object[] args) {
        Class<?>[] types = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            if (args[i] instanceof Integer) {
                types[i] = int.class;
            } else if (args[i] instanceof Long) {
                types[i] = long.class;
            } else if (args[i] instanceof Double) {
                types[i] = double.class;
            } else if (args[i] instanceof String) {
                types[i] = String.class;
            } else {
                types[i] = args[i].getClass();
            }
        }
        return types;
    }

    private boolean isStringType(String sqlType) {
        return sqlType.matches("VARCHAR|CHAR|TEXT|STRING|LONGTEXT|MEDIUMTEXT|CLOB|.*TEXT.*");
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
