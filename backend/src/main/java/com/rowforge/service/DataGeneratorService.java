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
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class DataGeneratorService {

    private static final Logger logger = LoggerFactory.getLogger(DataGeneratorService.class);
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final Pattern DATE_OFFSET_PATTERN = Pattern.compile(
            "(.+?)\\s*\\+\\s*(\\d+)\\s+(days?|hours?|minutes?)", Pattern.CASE_INSENSITIVE);
    private static final Pattern IF_THEN_PATTERN = Pattern.compile(
            "IF\\s+\\{?(\\w+)\\}?\\s*==\\s*'([^']*)'\\s+THEN\\s+(.*?)(?:\\s+ELSE\\s+(.*))?", Pattern.CASE_INSENSITIVE);
    private static final Pattern MULTI_IF_SEPARATOR = Pattern.compile("\\s*\\|\\|\\s*");
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

        // Get AI generation plan: Faker statements for every column + optional
        // correlations
        GenerationPlan plan = getAIGenerationPlan(tableName, columns);

        // Build a lookup map for column definitions by name
        Map<String, ColumnDefinition> columnMap = new LinkedHashMap<>();
        for (ColumnDefinition col : columns) {
            columnMap.put(col.getColumnName(), col);
        }

        List<List<String>> allRows = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            // Step 1: Pre-generate default faker values for ALL columns
            Map<String, String> rowContext = new LinkedHashMap<>();
            for (ColumnDefinition col : columns) {
                String colName = col.getColumnName();
                String colNameLower = colName.toLowerCase();
                if (colNameLower.equals("id") || colNameLower.endsWith("_id")) {
                    rowContext.put(colName, String.valueOf(i + 1));
                } else {
                    String fakerStmt = plan.columns().getOrDefault(colName, "faker.lorem().word()");
                    Object rawVal = executeFakerStatement(faker, fakerStmt);
                    rowContext.put(colName, rawVal != null ? rawVal.toString() : "");
                }
            }

            // Step 2: Apply correlations — override derived values with evaluated expressions
            for (CorrelationGroup corr : plan.correlations()) {
                for (Map.Entry<String, String> derived : corr.derived().entrySet()) {
                    String colName = derived.getKey();
                    if (columnMap.containsKey(colName)) {
                        String derivedVal = applyTemplate(derived.getValue(), rowContext);
                        derivedVal = evaluateDerivedExpression(derivedVal);
                        rowContext.put(colName, derivedVal);
                    }
                }
            }

            // Step 3: Build the full row in column order with proper quoting
            List<String> rowValues = new ArrayList<>();
            for (ColumnDefinition col : columns) {
                String colName = col.getColumnName();
                String val = rowContext.get(colName);
                if (val == null || "NULL".equalsIgnoreCase(val)) {
                    rowValues.add("NULL");
                } else if (isSqlFunctionExpression(val)) {
                    rowValues.add(val);
                } else {
                    String typeName = col.getColDataType().getDataType().toUpperCase();
                    boolean needsQuotes = isStringType(typeName) || isDateType(typeName);
                    rowValues.add(needsQuotes ? quote(val) : val);
                }
            }
            allRows.add(rowValues);
        }

        return switch (format.toUpperCase()) {
            case "CSV" -> buildCsv(columnNames, allRows);
            case "JSON" -> buildJson(tableName, columnNames, allRows);
            default -> buildSql(tableName, columnNames, allRows);
        };
    }

    private GenerationPlan getAIGenerationPlan(String tableName, List<ColumnDefinition> columns) {
        try {
            StringBuilder columnsInfo = new StringBuilder();
            for (ColumnDefinition col : columns) {
                String dataType = col.getColDataType().toString();
                List<String> specs = col.getColumnSpecs();
                String specsStr = (specs != null && !specs.isEmpty()) ? " " + String.join(" ", specs) : "";
                columnsInfo.append(String.format("- %s (%s%s)\n", col.getColumnName(), dataType, specsStr));
            }

            String prompt = String.format(
                    """
                            You are a data generation engine. Analyze the database table schema below and return a JSON object with exactly two keys: "columns" and "correlations".

                            ===== KEY 1: "columns" =====
                            An object where each key is a column name and each value is a valid net.datafaker v2.1.0 Java method call as a quoted string.
                            Every column in the schema MUST have an entry. No column may be omitted.

                            TYPE MAPPING RULES (strict):
                            - INTEGER/INT/BIGINT -> faker.number().numberBetween(min, max)
                            - DECIMAL/NUMERIC/FLOAT/DOUBLE -> faker.number().randomDouble(decimals, min, max)
                            - VARCHAR/TEXT/CHAR/STRING -> appropriate string-based faker method
                            - BOOLEAN/BOOL -> faker.bool().bool()
                            - DATE/TIMESTAMP/DATETIME -> faker.date().past(365, java.util.concurrent.TimeUnit.DAYS) or faker.date().future(365, java.util.concurrent.TimeUnit.DAYS). First argument MUST be >= 1.

                            VALID FAKER METHOD REFERENCE (use ONLY these, never guess):
                            - Names: faker.name().firstName(), faker.name().lastName(), faker.name().fullName()
                            - Address: faker.address().streetAddress(), faker.address().city(), faker.address().state(), faker.address().zipCode(), faker.address().country()
                            - Internet: faker.internet().emailAddress(), faker.internet().username(), faker.internet().url()
                            - Phone: faker.phoneNumber().cellPhone(), faker.phoneNumber().phoneNumber()
                            - Commerce: faker.commerce().productName(), faker.commerce().price(), faker.commerce().department()
                            - Company: faker.company().name(), faker.company().industry()
                            - Text: faker.lorem().sentence(), faker.lorem().paragraph(), faker.lorem().word()
                            - Numbers: faker.number().numberBetween(min, max), faker.number().randomDouble(decimals, min, max)
                            - Boolean: faker.bool().bool()
                            - Date: faker.date().past(days, java.util.concurrent.TimeUnit.DAYS), faker.date().future(days, java.util.concurrent.TimeUnit.DAYS)
                            - Options: faker.options().option("val1", "val2", ...)
                            - Payment: faker.business().creditCardType(), faker.finance().creditCard()

                            FORBIDDEN METHODS (do NOT use these, they will cause errors):
                            - faker.finance().creditCardType() -> use faker.business().creditCardType() instead
                            - faker.address().fullAddress() -> use faker.address().streetAddress() instead
                            - faker.name().name() -> use faker.name().fullName() instead
                            - faker.random() -> use faker.number() instead
                            - faker.subscription().paymentMethods() -> use faker.options().option("credit_card", "debit_card", "paypal", "bank_transfer") instead
                            If unsure whether a method exists, use faker.options().option(...) with realistic hardcoded values.

                            DATE COLUMN RULES:
                            - Faker date expressions are ONLY for base (anchor) date columns.
                            - If TWO OR MORE date/timestamp columns are logically related (e.g. order_date, shipping_date, delivery_date; start_date, end_date; created_at, updated_at), you MUST:
                              a) Choose exactly ONE as the base date column (the earliest in the logical sequence).
                              b) All other related date columns MUST be derived via correlations.
                              c) Still include them in "columns" with a placeholder expression (e.g. faker.date().past(365, java.util.concurrent.TimeUnit.DAYS)) since the correlation engine will override it.

                            ENUM / CHECK CONSTRAINT RULES:
                            - If the schema defines ENUM values or CHECK constraints with a fixed set of allowed values, you MUST use faker.options().option("val1", "val2", ...) with the EXACT values from the schema.
                            - Do NOT translate, paraphrase, reorder, or invent values. Copy them verbatim.

                            REALISM RULES:
                            - Choose Faker methods that produce semantically appropriate data.
                            - Monetary DECIMAL columns should use faker.number().randomDouble() with ranges matching the column's precision.
                            - For payment method columns, use faker.options().option("credit_card", "debit_card", "paypal", "bank_transfer").
                            - For country columns with a DEFAULT value in the schema, use faker.options().option() weighted toward the default (repeat it).
                            - For postal/zip code columns, use faker.number().numberBetween(10000, 99999) for 5-digit codes.

                            ===== KEY 2: "correlations" =====
                            An array of correlation groups. Each group enforces consistency between related columns within a single row.

                            Each group object has:
                            - "group": a descriptive label (e.g. "person", "address", "order_dates", "order_status")
                            - "base": an array of column names whose values are generated FIRST via their Faker expression
                            - "derived": an object mapping column names to TEMPLATE EXPRESSIONS (not SQL)

                            WHEN CORRELATIONS ARE MANDATORY (not optional):
                            1. Two or more logically related date/timestamp columns exist.
                            2. A status/state column controls the presence or absence of other columns.
                            3. Name-related columns feed into email, username, or display name.
                            4. Address-related columns (city, state, zip, country) appear together.
                            5. Boolean flags depend on status or other columns.
                            If NONE of these conditions apply, set "correlations" to [].

                            ===== DERIVED EXPRESSION SYNTAX (mandatory, no SQL allowed) =====
                            Do NOT use SQL syntax (no CASE, WHEN, SELECT, DATE_ADD, etc.).
                            Use ONLY these template expression formats:

                            1. Simple placeholder: "{column_name}"
                               Replaced with the base column's generated value.

                            2. String template: "{first_name}.{last_name}@example.com"
                               Concatenates base values with literal text.

                            3. Date offset: "{base_date} + N days"
                               Adds N days to the base date. Supported units: days, hours, minutes.
                               N must be >= 1. Example: "{fecha_pedido} + 3 days"

                            4. Conditional (single): "IF {column} == 'value' THEN result ELSE fallback"
                               If column equals value, use result; otherwise use fallback.
                               result and fallback can be: NULL, true, false, a literal, or a date offset.

                            5. Multi-conditional (chained with ||):
                               "IF {col} == 'val1' THEN res1 || IF {col} == 'val2' THEN res2 || IF {col} == 'val3' THEN res3 ELSE default"
                               Evaluated left to right. First matching condition wins.
                               The LAST branch MUST have an ELSE clause as the final fallback.

                            EXAMPLES of derived expressions:
                            - "{fecha_pedido} + 3 days"
                            - "NULL"
                            - "true"
                            - "IF {estado} == 'pendiente' THEN NULL ELSE {fecha_pedido} + 3 days"
                            - "IF {estado} == 'entregado' THEN true || IF {estado} == 'cancelado' THEN false ELSE false"
                            - "IF {estado} == 'pendiente' THEN NULL || IF {estado} == 'enviado' THEN {fecha_pedido} + 3 days || IF {estado} == 'entregado' THEN {fecha_pedido} + 3 days ELSE NULL"
                            - "IF {estado} == 'entregado' THEN {fecha_pedido} + 7 days ELSE NULL"

                            STATUS-DEPENDENT LOGIC RULES:
                            When a status/state column controls other columns, the status column MUST be in "base".
                            Derived fields MUST use conditional expressions. Standard patterns:
                            - Status "pending" or equivalent -> shipping_date: NULL, delivery_date: NULL, paid: false
                            - Status "shipped" or equivalent -> shipping_date: base_date + 1-5 days, delivery_date: NULL, paid: true
                            - Status "delivered" or equivalent -> shipping_date: base_date + 1-5 days, delivery_date: base_date + 5-10 days, paid: true
                            - Status "canceled" or equivalent -> shipping_date: NULL, delivery_date: NULL, paid: false

                            ===== OUTPUT FORMAT =====
                            Return ONLY valid JSON. No markdown, no code fences, no explanations, no comments.

                            Table: %s
                            Columns:
                            %s

                            ===== EXAMPLES =====

                            Example 1 - Person with name-derived fields:
                            {"columns": {"id": "faker.number().numberBetween(1, 10000)", "first_name": "faker.name().firstName()", "last_name": "faker.name().lastName()", "email": "faker.internet().emailAddress()", "username": "faker.internet().username()", "age": "faker.number().numberBetween(18, 80)"}, "correlations": [{"group": "person", "base": ["first_name", "last_name"], "derived": {"email": "{first_name}.{last_name}@example.com", "username": "{first_name}_{last_name}"}}]}

                            Example 2 - Order with status-dependent dates and flags:
                            {"columns": {"id": "faker.number().numberBetween(1, 100000)", "estado": "faker.options().option(\\"pendiente\\", \\"enviado\\", \\"entregado\\", \\"cancelado\\")", "fecha_pedido": "faker.date().past(365, java.util.concurrent.TimeUnit.DAYS)", "fecha_envio": "faker.date().past(365, java.util.concurrent.TimeUnit.DAYS)", "fecha_entrega": "faker.date().past(365, java.util.concurrent.TimeUnit.DAYS)", "pagado": "faker.bool().bool()", "total": "faker.number().randomDouble(2, 10, 5000)"}, "correlations": [{"group": "order_status", "base": ["estado", "fecha_pedido"], "derived": {"fecha_envio": "IF {estado} == 'pendiente' THEN NULL || IF {estado} == 'cancelado' THEN NULL || IF {estado} == 'enviado' THEN {fecha_pedido} + 3 days || IF {estado} == 'entregado' THEN {fecha_pedido} + 3 days ELSE NULL", "fecha_entrega": "IF {estado} == 'entregado' THEN {fecha_pedido} + 7 days ELSE NULL", "pagado": "IF {estado} == 'entregado' THEN true || IF {estado} == 'cancelado' THEN false ELSE false"}}]}

                            Example 3 - No correlations needed:
                            {"columns": {"product_id": "faker.number().numberBetween(1, 10000)", "product_name": "faker.commerce().productName()", "price": "faker.commerce().price()"}, "correlations": []}
                                """,
                    tableName, columnsInfo.toString());

            String response = chatClient.prompt()
                    .user(prompt)
                    .call()
                    .content();

            logger.debug("AI generation plan: {}", response);
            return parseGenerationPlan(response);
        } catch (Exception e) {
            logger.warn("Failed to get AI generation plan, using defaults: {}", e.getMessage());
            return new GenerationPlan(new HashMap<>(), List.of());
        }
    }

    @SuppressWarnings("unchecked")
    GenerationPlan parseGenerationPlan(String response) {
        try {
            String jsonString = response.trim();
            // Handle markdown code blocks if present
            if (jsonString.contains("```json")) {
                jsonString = jsonString.substring(jsonString.indexOf("```json") + 7, jsonString.lastIndexOf("```"));
            } else if (jsonString.contains("```")) {
                jsonString = jsonString.substring(jsonString.indexOf("```") + 3, jsonString.lastIndexOf("```"));
            }
            jsonString = jsonString.trim();

            Map<String, Object> raw = objectMapper.readValue(jsonString, Map.class);

            // Parse columns (required)
            Map<String, String> columns;
            Object columnsObj = raw.get("columns");
            if (columnsObj instanceof Map) {
                columns = new HashMap<>();
                for (Map.Entry<String, Object> e : ((Map<String, Object>) columnsObj).entrySet()) {
                    columns.put(e.getKey(), String.valueOf(e.getValue()));
                }
            } else {
                // Fallback: treat the entire response as a flat column map (backward compat)
                columns = objectMapper.readValue(jsonString, Map.class);
                return new GenerationPlan(columns, List.of());
            }

            // Parse correlations (optional)
            List<CorrelationGroup> correlations = new ArrayList<>();
            Object corrObj = raw.get("correlations");
            if (corrObj instanceof List<?> corrList) {
                for (Object item : corrList) {
                    if (item instanceof Map<?, ?> groupMap) {
                        Object groupLabel = groupMap.get("group");
                        String group = groupLabel != null ? String.valueOf(groupLabel) : "unknown";
                        List<String> base = new ArrayList<>();
                        Object baseObj = groupMap.get("base");
                        if (baseObj instanceof List<?> baseList) {
                            for (Object b : baseList)
                                base.add(String.valueOf(b));
                        }
                        Map<String, String> derived = new HashMap<>();
                        Object derivedObj = groupMap.get("derived");
                        if (derivedObj instanceof Map<?, ?> derivedMap) {
                            for (Map.Entry<?, ?> e : derivedMap.entrySet()) {
                                derived.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
                            }
                        }
                        if (!base.isEmpty()) {
                            correlations.add(new CorrelationGroup(group, base, derived));
                        }
                    }
                }
            }

            return new GenerationPlan(columns, correlations);
        } catch (Exception e) {
            logger.error("Failed to parse AI generation plan: {}", e.getMessage());
            return new GenerationPlan(new HashMap<>(), List.of());
        }
    }

    String applyTemplate(String template, Map<String, String> context) {
        String result = template;
        for (Map.Entry<String, String> entry : context.entrySet()) {
            result = result.replace("{" + entry.getKey() + "}", entry.getValue());
        }
        return result;
    }

    /**
     * Evaluates a derived expression after placeholder substitution.
     * Handles conditionals (IF/THEN/ELSE), date offsets, NULL, true/false, and plain values.
     */
    String evaluateDerivedExpression(String expr) {
        if (expr == null || expr.isBlank()) return "NULL";
        String trimmed = expr.trim();

        if ("NULL".equalsIgnoreCase(trimmed)) return "NULL";
        if ("true".equalsIgnoreCase(trimmed)) return "true";
        if ("false".equalsIgnoreCase(trimmed)) return "false";

        // Multi-conditional: "IF ... THEN ... || IF ... THEN ... ELSE ..."
        if (trimmed.toUpperCase().startsWith("IF ") && trimmed.contains("||")) {
            return evaluateMultiConditional(trimmed);
        }

        // Single conditional: "IF {col} == 'val' THEN result ELSE fallback"
        if (trimmed.toUpperCase().startsWith("IF ")) {
            return evaluateSingleConditional(trimmed);
        }

        // Date offset: "2025-03-20 14:30:00 + 3 days"
        Matcher dateOffsetMatcher = DATE_OFFSET_PATTERN.matcher(trimmed);
        if (dateOffsetMatcher.matches()) {
            return evaluateDateOffset(dateOffsetMatcher.group(1).trim(),
                    Integer.parseInt(dateOffsetMatcher.group(2)),
                    dateOffsetMatcher.group(3).trim());
        }

        return trimmed;
    }

    private String evaluateMultiConditional(String expr) {
        String[] branches = MULTI_IF_SEPARATOR.split(expr);
        for (String branch : branches) {
            branch = branch.trim();
            if (branch.toUpperCase().startsWith("IF ")) {
                String result = evaluateSingleConditional(branch);
                // If the condition matched (didn't fall through to ELSE with no ELSE clause),
                // we check whether this branch had an ELSE. If it did and matched, return it.
                // If it didn't have ELSE and condition was false, result is null -> continue.
                if (result != null) {
                    return result;
                }
            } else {
                // Bare fallback value (shouldn't happen with well-formed expressions)
                return evaluateDerivedExpression(branch);
            }
        }
        return "NULL";
    }

    private String evaluateSingleConditional(String expr) {
        Matcher m = IF_THEN_PATTERN.matcher(expr.trim());
        if (!m.matches()) {
            logger.debug("Could not parse conditional expression: {}", expr);
            return null;
        }

        // Re-parse: after substitution the form is "IF value == 'expected' THEN result [ELSE fallback]"
        String stripped = expr.trim();
        if (stripped.toUpperCase().startsWith("IF ")) stripped = stripped.substring(3).trim();

        // Find == operator
        int eqIdx = stripped.indexOf("==");
        if (eqIdx < 0) return null;

        String leftSide = stripped.substring(0, eqIdx).trim();
        String rest = stripped.substring(eqIdx + 2).trim();

        // Extract expected value (in single quotes)
        if (!rest.startsWith("'")) return null;
        int closeQuote = rest.indexOf("'", 1);
        if (closeQuote < 0) return null;
        String expectedValue = rest.substring(1, closeQuote);

        // Find THEN
        String afterQuote = rest.substring(closeQuote + 1).trim();
        if (!afterQuote.toUpperCase().startsWith("THEN ")) return null;
        String thenPart = afterQuote.substring(5).trim();

        // Split THEN result and optional ELSE
        String thenResult;
        String elseResult = null;
        int elseIdx = findTopLevelElse(thenPart);
        if (elseIdx >= 0) {
            thenResult = thenPart.substring(0, elseIdx).trim();
            elseResult = thenPart.substring(elseIdx + 4).trim(); // skip "ELSE"
        } else {
            thenResult = thenPart;
        }

        boolean conditionMet = leftSide.equals(expectedValue);
        if (conditionMet) {
            return evaluateDerivedExpression(thenResult);
        } else if (elseResult != null) {
            return evaluateDerivedExpression(elseResult);
        }
        return null; // no match, no else -> signal to multi-conditional to try next branch
    }

    private int findTopLevelElse(String s) {
        String upper = s.toUpperCase();
        // Find " ELSE " that is not inside a nested IF
        int depth = 0;
        for (int i = 0; i < upper.length() - 5; i++) {
            if (upper.startsWith("IF ", i)) depth++;
            if (depth == 0 && (i == 0 || s.charAt(i - 1) == ' ') && upper.startsWith("ELSE", i)) {
                // Check it's followed by space or end
                int afterElse = i + 4;
                if (afterElse >= upper.length() || s.charAt(afterElse) == ' ') {
                    return i;
                }
            }
            if (upper.startsWith("THEN ", i) && depth > 0) depth--;
        }
        return -1;
    }

    private String evaluateDateOffset(String dateStr, int amount, String unit) {
        try {
            LocalDateTime base;
            try {
                base = LocalDateTime.parse(dateStr, DATETIME_FORMATTER);
            } catch (Exception e1) {
                try {
                    base = LocalDateTime.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
                } catch (Exception e2) {
                    base = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyy-MM-dd")).atStartOfDay();
                }
            }
            String unitLower = unit.toLowerCase();
            LocalDateTime result = switch (unitLower.replaceAll("s$", "")) {
                case "hour" -> base.plus(amount, ChronoUnit.HOURS);
                case "minute" -> base.plus(amount, ChronoUnit.MINUTES);
                default -> base.plus(amount, ChronoUnit.DAYS);
            };
            return result.format(DATETIME_FORMATTER);
        } catch (Exception e) {
            logger.warn("Failed to evaluate date offset '{}' + {} {}: {}", dateStr, amount, unit, e.getMessage());
            return dateStr;
        }
    }

    private boolean isDateType(String sqlType) {
        return sqlType.matches("DATE|DATETIME|TIMESTAMP|TIME");
    }

    private boolean isSqlFunctionExpression(String val) {
        return false;
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
            boolean needsQuotes = isStringType(typeName) || isDateType(typeName);

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

    private record MethodCall(String methodName, String argsStr) {
    }

    record CorrelationGroup(String group, List<String> base, Map<String, String> derived) {
    }

    record GenerationPlan(Map<String, String> columns, List<CorrelationGroup> correlations) {
    }

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
     * Parses a method-call chain (e.g. {@code date().past(365, TimeUnit.DAYS)})
     * into ordered
     * {@link MethodCall} records using a bracket-depth counter so nested parens in
     * arguments
     * are handled correctly.
     */
    private List<MethodCall> parseMethodChain(String chain, String originalStatement) {
        List<MethodCall> calls = new ArrayList<>();
        int i = 0;
        int len = chain.length();
        while (i < len) {
            if (chain.charAt(i) == '.')
                i++;
            if (i >= len)
                break;

            // Read method name
            int nameStart = i;
            while (i < len && (Character.isLetterOrDigit(chain.charAt(i)) || chain.charAt(i) == '_'))
                i++;
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
                if (c == '(')
                    depth++;
                else if (c == ')')
                    depth--;
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
        if (parenIdx <= 0)
            return null;

        String qualifiedMethod = expr.substring(0, parenIdx);
        String argsStr = expr.substring(parenIdx + 1, expr.length() - 1);

        int lastDot = qualifiedMethod.lastIndexOf('.');
        if (lastDot <= 0)
            return null;

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
                @SuppressWarnings({ "rawtypes", "unchecked" })
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
            if (args.length < fixedCount)
                continue;

            boolean compatible = true;
            for (int i = 0; i < fixedCount; i++) {
                if (!isArgumentCompatible(paramTypes[i], args[i])) {
                    compatible = false;
                    break;
                }
            }
            if (!compatible)
                continue;

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
            @SuppressWarnings({ "rawtypes", "unchecked" })
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

    /**
     * Returns true only if the value is a properly SQL-quoted string (starts and
     * ends with ').
     */
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
                if (j < columnNames.size() - 1)
                    sb.append(", ");
            }
            sb.append("}");
            if (i < allRows.size() - 1)
                sb.append(",");
            sb.append("\n");
        }
        sb.append("  ]\n}");
        return sb.toString();
    }
}
