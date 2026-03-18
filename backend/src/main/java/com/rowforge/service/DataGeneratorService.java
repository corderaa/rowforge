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

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
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
    Analyze this database table schema and suggest the BEST Java Faker method for each column.
    Return ONLY a JSON object with column names as keys and Faker method calls as values.
    The Faker expressions MUST be valid Java Faker syntax only for version v1.0.0+ (current 2.1.0), current better, (net.datafaker) and returned as quoted strings.
    Make sure the Faker type matches the column type (number → numeric method, string → string method, boolean → faker.bool().bool()).
    For DATE/TIMESTAMP columns use ONLY: faker.date().past(365, java.util.concurrent.TimeUnit.DAYS) or faker.date().future(365, java.util.concurrent.TimeUnit.DAYS). The first argument MUST be at least 1. Never pass LocalDate or LocalDateTime as arguments.
    If a column has a limited set of possible values, use faker.options().option(...) with realistic values.

    IMPORTANT: Return ONLY valid JSON, no markdown, no explanations, no code blocks.

    Table: %s
    Columns:
    %s

    Example response:
    {"id": "faker.number().numberBetween(1, 10000)", "name": "faker.name().fullName()", "email": "faker.internet().emailAddress()"}
    """, tableName, columnsInfo.toString());

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

    private record MethodCall(String methodName, String argsStr) {}

    private Object executeFakerStatement(Faker faker, String statement) {
        try {
            if (statement == null || statement.isBlank()) {
                return faker.lorem().word();
            }

            String chain = statement.trim();
            if (chain.startsWith("faker.")) {
                chain = chain.substring("faker.".length());
            }

            List<MethodCall> calls = parseMethodChain(chain, statement);
            if (calls.isEmpty()) {
                throw new IllegalArgumentException("Invalid faker statement syntax: " + statement);
            }

            Object current = faker;
            for (MethodCall call : calls) {
                Object[] args = parseArguments(call.argsStr());
                Method method = findCompatibleMethod(current.getClass(), call.methodName(), args);
                if (method == null) {
                    throw new NoSuchMethodException(current.getClass().getName() + "." + call.methodName());
                }
                Object[] coercedArgs = coerceArgumentsForMethod(method, args);
                current = method.invoke(current, coercedArgs);
            }

            return current;
        } catch (Exception e) {
            logger.debug("Error executing Faker statement '{}': {}", statement, e.getMessage());
            return faker.lorem().word();
        }
    }

    /**
     * Parses a method-call chain (e.g. {@code date().past(365, TimeUnit.DAYS)}) into ordered
     * {@link MethodCall} records using a bracket-depth counter so nested parens in arguments
     * are handled correctly.
     */
    private List<MethodCall> parseMethodChain(String chain, String originalStatement) {
        List<MethodCall> calls = new ArrayList<>();
        int i = 0;
        int len = chain.length();
        while (i < len) {
            if (chain.charAt(i) == '.') i++;
            if (i >= len) break;

            // Read method name
            int nameStart = i;
            while (i < len && (Character.isLetterOrDigit(chain.charAt(i)) || chain.charAt(i) == '_')) i++;
            if (i >= len || chain.charAt(i) != '(') {
                throw new IllegalArgumentException("Invalid faker statement syntax: " + originalStatement);
            }
            String methodName = chain.substring(nameStart, i);

            // Read arguments, tracking bracket depth
            i++; // skip opening '('
            int argsStart = i;
            int depth = 1;
            while (i < len && depth > 0) {
                char c = chain.charAt(i);
                if (c == '(') depth++;
                else if (c == ')') depth--;
                i++;
            }
            if (depth != 0) {
                throw new IllegalArgumentException("Unmatched parentheses in faker statement: " + originalStatement);
            }
            String argsStr = chain.substring(argsStart, i - 1); // exclude closing ')'
            calls.add(new MethodCall(methodName, argsStr));
        }
        return calls;
    }

    private Object[] parseArguments(String argsStr) {
        if (argsStr == null || argsStr.isBlank()) {
            return new Object[0];
        }

        List<String> argParts = splitArguments(argsStr);
        Object[] args = new Object[argParts.size()];
        for (int i = 0; i < argParts.size(); i++) {
            args[i] = parseArgument(argParts.get(i));
        }

        return args;
    }

    private List<String> splitArguments(String argsStr) {
        List<String> parts = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        int depth = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = 0; i < argsStr.length(); i++) {
            char c = argsStr.charAt(i);
            if (c == '\'' && !inDoubleQuote) {
                inSingleQuote = !inSingleQuote;
            } else if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
            } else if (c == '(' && !inSingleQuote && !inDoubleQuote) {
                depth++;
            } else if (c == ')' && !inSingleQuote && !inDoubleQuote && depth > 0) {
                depth--;
            }

            if (c == ',' && depth == 0 && !inSingleQuote && !inDoubleQuote) {
                parts.add(current.toString().trim());
                current.setLength(0);
                continue;
            }

            current.append(c);
        }

        if (!current.isEmpty()) {
            parts.add(current.toString().trim());
        }
        return parts;
    }

    private Object parseArgument(String arg) {
        String trimmed = arg == null ? "" : arg.trim();
        if (trimmed.isEmpty()) {
            return "";
        }

        if ((trimmed.startsWith("\"") && trimmed.endsWith("\""))
                || (trimmed.startsWith("'") && trimmed.endsWith("'"))) {
            return trimmed.substring(1, trimmed.length() - 1);
        }

        if ("true".equalsIgnoreCase(trimmed) || "false".equalsIgnoreCase(trimmed)) {
            return Boolean.parseBoolean(trimmed);
        }

        try {
            return Integer.parseInt(trimmed);
        } catch (NumberFormatException ignored) {
            // Not an integer
        }

        try {
            return Long.parseLong(trimmed);
        } catch (NumberFormatException ignored) {
            // Not a long
        }

        try {
            return Double.parseDouble(trimmed);
        } catch (NumberFormatException ignored) {
            // Not a double
        }

        // Try static method call e.g. java.time.LocalDate.now()
        if (trimmed.contains("(") && trimmed.endsWith(")")) {
            Object staticResult = resolveStaticMethodCall(trimmed);
            if (staticResult != null) {
                return staticResult;
            }
        }

        Object enumOrStaticField = resolveEnumOrStaticField(trimmed);
        if (enumOrStaticField != null) {
            return enumOrStaticField;
        }

        return trimmed;
    }

    private Object resolveStaticMethodCall(String expr) {
        int parenIdx = expr.indexOf('(');
        if (parenIdx <= 0) return null;

        String qualifiedMethod = expr.substring(0, parenIdx);
        String argsStr = expr.substring(parenIdx + 1, expr.length() - 1);

        int lastDot = qualifiedMethod.lastIndexOf('.');
        if (lastDot <= 0) return null;

        String className = qualifiedMethod.substring(0, lastDot);
        String methodName = qualifiedMethod.substring(lastDot + 1);

        try {
            Class<?> clazz = Class.forName(className);
            Object[] args = parseArguments(argsStr);
            Method method = findCompatibleMethod(clazz, methodName, args);
            if (method != null && java.lang.reflect.Modifier.isStatic(method.getModifiers())) {
                Object[] coercedArgs = coerceArgumentsForMethod(method, args);
                return method.invoke(null, coercedArgs);
            }
        } catch (Exception ignored) {
            // Not a static method call
        }
        return null;
    }

    private Object resolveEnumOrStaticField(String value) {
        int lastDot = value.lastIndexOf('.');
        if (lastDot <= 0 || lastDot == value.length() - 1) {
            return null;
        }

        String className = value.substring(0, lastDot);
        String fieldName = value.substring(lastDot + 1);
        try {
            Class<?> clazz = Class.forName(className);
            if (clazz.isEnum()) {
                @SuppressWarnings({"rawtypes", "unchecked"})
                Object enumValue = Enum.valueOf((Class<? extends Enum>) clazz.asSubclass(Enum.class), fieldName);
                return enumValue;
            }

            Field field = clazz.getField(fieldName);
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                return field.get(null);
            }
        } catch (Exception ignored) {
            // Not an enum/static field expression
        }

        return null;
    }

    private Method findCompatibleMethod(Class<?> clazz, String methodName, Object[] args) {
        // First: exact arity match (non-varargs)
        for (Method method : clazz.getMethods()) {
            if (!method.getName().equals(methodName) || method.getParameterCount() != args.length) {
                continue;
            }

            Class<?>[] paramTypes = method.getParameterTypes();
            boolean compatible = true;
            for (int i = 0; i < paramTypes.length; i++) {
                if (!isArgumentCompatible(paramTypes[i], args[i])) {
                    compatible = false;
                    break;
                }
            }

            if (compatible) {
                return method;
            }
        }

        // Second: varargs match
        for (Method method : clazz.getMethods()) {
            if (!method.getName().equals(methodName) || !method.isVarArgs()) {
                continue;
            }
            Class<?>[] paramTypes = method.getParameterTypes();
            int fixedCount = paramTypes.length - 1;
            if (args.length < fixedCount) continue;

            boolean compatible = true;
            for (int i = 0; i < fixedCount; i++) {
                if (!isArgumentCompatible(paramTypes[i], args[i])) {
                    compatible = false;
                    break;
                }
            }
            if (!compatible) continue;

            Class<?> componentType = paramTypes[fixedCount].getComponentType();
            for (int i = fixedCount; i < args.length; i++) {
                if (!isArgumentCompatible(componentType, args[i])) {
                    compatible = false;
                    break;
                }
            }
            if (compatible) {
                return method;
            }
        }
        return null;
    }

    private boolean isArgumentCompatible(Class<?> paramType, Object arg) {
        if (arg == null) {
            return !paramType.isPrimitive();
        }

        Class<?> argType = arg.getClass();
        if (paramType.isAssignableFrom(argType)) {
            return true;
        }

        if (paramType.isPrimitive()) {
            return switch (paramType.getName()) {
                case "int" -> arg instanceof Number;
                case "long" -> arg instanceof Number;
                case "double" -> arg instanceof Number;
                case "float" -> arg instanceof Number;
                case "short" -> arg instanceof Number;
                case "byte" -> arg instanceof Number;
                case "boolean" -> arg instanceof Boolean;
                case "char" -> arg instanceof Character || (arg instanceof String s && s.length() == 1);
                default -> false;
            };
        }

        if (paramType.isEnum() && arg instanceof String) {
            return true;
        }

        return false;
    }

    private Object[] coerceArgumentsForMethod(Method method, Object[] args) {
        Class<?>[] paramTypes = method.getParameterTypes();
        if (!method.isVarArgs()) {
            Object[] coerced = new Object[args.length];
            for (int i = 0; i < args.length; i++) {
                coerced[i] = coerceArgument(paramTypes[i], args[i]);
            }
            return coerced;
        }

        int fixedCount = paramTypes.length - 1;
        Object[] coerced = new Object[paramTypes.length];
        for (int i = 0; i < fixedCount; i++) {
            coerced[i] = coerceArgument(paramTypes[i], args[i]);
        }

        Class<?> componentType = paramTypes[fixedCount].getComponentType();
        Object varargArray = Array.newInstance(componentType, args.length - fixedCount);
        for (int i = fixedCount; i < args.length; i++) {
            Array.set(varargArray, i - fixedCount, coerceArgument(componentType, args[i]));
        }
        coerced[fixedCount] = varargArray;
        return coerced;
    }

    private Object coerceArgument(Class<?> targetType, Object arg) {
        if (arg == null) {
            return null;
        }

        if (targetType.isAssignableFrom(arg.getClass())) {
            return arg;
        }

        if (targetType.isPrimitive()) {
            if (arg instanceof Number number) {
                return switch (targetType.getName()) {
                    case "int" -> number.intValue();
                    case "long" -> number.longValue();
                    case "double" -> number.doubleValue();
                    case "float" -> number.floatValue();
                    case "short" -> number.shortValue();
                    case "byte" -> number.byteValue();
                    default -> arg;
                };
            }
            if (targetType == char.class && arg instanceof String s && s.length() == 1) {
                return s.charAt(0);
            }
            return arg;
        }

        if (targetType.isEnum() && arg instanceof String s) {
            @SuppressWarnings({"rawtypes", "unchecked"})
            Object enumValue = Enum.valueOf((Class<? extends Enum>) targetType.asSubclass(Enum.class), s);
            return enumValue;
        }

        return arg;
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
