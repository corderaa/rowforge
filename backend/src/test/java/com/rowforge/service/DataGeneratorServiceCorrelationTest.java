package com.rowforge.service;

import com.rowforge.repository.DatasetGenerationRepository;
import com.rowforge.service.DataGeneratorService.CorrelationGroup;
import com.rowforge.service.DataGeneratorService.GenerationPlan;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class DataGeneratorServiceCorrelationTest {

    private DataGeneratorService service;

    @BeforeEach
    void setUp() {
        // Create service with mocked dependencies (only parsing/template methods are tested)
        service = new DataGeneratorService(
                mock(DatasetGenerationRepository.class),
                null, // ChatClient not needed for unit tests
                "");
    }

    // ── parseGenerationPlan ──────────────────────────────────────────────────

    @Test
    void parseGenerationPlan_withCorrelations() {
        String json = """
            {
              "columns": {
                "id": "faker.number().numberBetween(1, 10000)",
                "first_name": "faker.name().firstName()",
                "last_name": "faker.name().lastName()",
                "email": "faker.internet().emailAddress()",
                "username": "faker.internet().username()",
                "age": "faker.number().numberBetween(18, 80)"
              },
              "correlations": [
                {
                  "group": "person",
                  "base": ["first_name", "last_name"],
                  "derived": {
                    "email": "{first_name}.{last_name}@example.com",
                    "username": "{first_name}_{last_name}"
                  }
                }
              ]
            }
            """;

        GenerationPlan plan = service.parseGenerationPlan(json);

        assertThat(plan.columns()).hasSize(6);
        assertThat(plan.columns().get("first_name")).isEqualTo("faker.name().firstName()");
        assertThat(plan.correlations()).hasSize(1);

        CorrelationGroup corr = plan.correlations().get(0);
        assertThat(corr.group()).isEqualTo("person");
        assertThat(corr.base()).containsExactly("first_name", "last_name");
        assertThat(corr.derived()).containsEntry("email", "{first_name}.{last_name}@example.com");
        assertThat(corr.derived()).containsEntry("username", "{first_name}_{last_name}");
    }

    @Test
    void parseGenerationPlan_withoutCorrelations() {
        String json = """
            {
              "columns": {
                "product_id": "faker.number().numberBetween(1, 10000)",
                "product_name": "faker.commerce().productName()",
                "price": "faker.commerce().price()"
              },
              "correlations": []
            }
            """;

        GenerationPlan plan = service.parseGenerationPlan(json);

        assertThat(plan.columns()).hasSize(3);
        assertThat(plan.correlations()).isEmpty();
    }

    @Test
    void parseGenerationPlan_backwardCompatFlatMap() {
        String json = """
            {"id": "faker.number().numberBetween(1, 10000)", "name": "faker.name().fullName()", "email": "faker.internet().emailAddress()"}
            """;

        GenerationPlan plan = service.parseGenerationPlan(json);

        assertThat(plan.columns()).hasSize(3);
        assertThat(plan.columns().get("name")).isEqualTo("faker.name().fullName()");
        assertThat(plan.correlations()).isEmpty();
    }

    @Test
    void parseGenerationPlan_markdownCodeBlock() {
        String json = """
            ```json
            {"columns": {"id": "faker.number().numberBetween(1, 10000)"}, "correlations": []}
            ```
            """;

        GenerationPlan plan = service.parseGenerationPlan(json);

        assertThat(plan.columns()).containsKey("id");
        assertThat(plan.correlations()).isEmpty();
    }

    @Test
    void parseGenerationPlan_malformedJson() {
        GenerationPlan plan = service.parseGenerationPlan("this is not json at all");

        assertThat(plan.columns()).isEmpty();
        assertThat(plan.correlations()).isEmpty();
    }

    @Test
    void parseGenerationPlan_correlationWithEmptyBase() {
        String json = """
            {
              "columns": {"email": "faker.internet().emailAddress()"},
              "correlations": [{"group": "person", "base": [], "derived": {"email": "test@example.com"}}]
            }
            """;

        GenerationPlan plan = service.parseGenerationPlan(json);

        // Correlation groups with empty base are skipped
        assertThat(plan.correlations()).isEmpty();
    }

    // ── applyTemplate ────────────────────────────────────────────────────────

    @Test
    void applyTemplate_substitutesPlaceholders() {
        Map<String, String> context = Map.of("first_name", "Jane", "last_name", "Doe");
        String result = service.applyTemplate("{first_name}.{last_name}@example.com", context);
        assertThat(result).isEqualTo("Jane.Doe@example.com");
    }

    @Test
    void applyTemplate_username() {
        Map<String, String> context = Map.of("first_name", "Jane", "last_name", "Doe");
        String result = service.applyTemplate("{first_name}_{last_name}", context);
        assertThat(result).isEqualTo("Jane_Doe");
    }

    @Test
    void applyTemplate_missingPlaceholder() {
        Map<String, String> context = Map.of("first_name", "Jane");
        String result = service.applyTemplate("{first_name}.{last_name}@example.com", context);
        // Unresolved placeholder stays as-is
        assertThat(result).isEqualTo("Jane.{last_name}@example.com");
    }

    @Test
    void applyTemplate_noPlaceholders() {
        String result = service.applyTemplate("static@example.com", Map.of());
        assertThat(result).isEqualTo("static@example.com");
    }
}
