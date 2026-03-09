package com.rowforge;

import com.rowforge.service.DataGeneratorService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SpringBootTest
class RowForgeApplicationTests {

    @Autowired
    private DataGeneratorService dataGeneratorService;

    private static final String SAMPLE_SQL =
            "CREATE TABLE users (" +
            "  id INT," +
            "  first_name VARCHAR(50)," +
            "  last_name VARCHAR(50)," +
            "  email VARCHAR(100)," +
            "  age INT" +
            ");";

    @Test
    void contextLoads() {
    }

    @Test
    void generateSqlFormat() {
        String result = dataGeneratorService.generate(SAMPLE_SQL, 3, "SQL");
        assertThat(result).isNotBlank();
        assertThat(result).contains("INSERT INTO users");
        assertThat(result.lines().count()).isEqualTo(3);
    }

    @Test
    void generateCsvFormat() {
        String result = dataGeneratorService.generate(SAMPLE_SQL, 2, "CSV");
        assertThat(result).isNotBlank();
        // First line is the header
        String header = result.lines().findFirst().orElse("");
        assertThat(header).contains("id").contains("first_name").contains("email");
        // header + 2 data rows = 3 lines
        assertThat(result.lines().count()).isEqualTo(3);
    }

    @Test
    void generateJsonFormat() {
        String result = dataGeneratorService.generate(SAMPLE_SQL, 2, "JSON");
        assertThat(result).isNotBlank();
        assertThat(result).contains("\"table\": \"users\"");
        assertThat(result).contains("\"rows\":");
    }

    @Test
    void invalidSqlThrowsException() {
        assertThatThrownBy(() -> dataGeneratorService.generate("THIS IS NOT SQL", 1, "SQL"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid SQL schema");
    }
}
