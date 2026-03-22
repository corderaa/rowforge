package com.rowforge.controller;

import com.rowforge.model.Schema;
import com.rowforge.service.DataGeneratorService;

import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class GenerateController {

    private static final Logger logger = LoggerFactory.getLogger(GenerateController.class);

    private final DataGeneratorService dataGeneratorService;

    public GenerateController(DataGeneratorService dataGeneratorService) {
        this.dataGeneratorService = dataGeneratorService;
    }

    @PostMapping("/generate")
    public ResponseEntity<String> generate(HttpServletRequest request, @RequestBody Schema schema) {
        logger.info("Received generate request: {} rows, format={}", schema.getRows(), schema.getFormat());

        String anonId = request.getHeader("X-Anon-Id");
        if (anonId == null || anonId.isBlank()) {
            anonId = UUID.randomUUID().toString();
        }

        if (schema.getSql() == null || schema.getSql().isBlank()) {
            return ResponseEntity.badRequest().body("SQL schema must not be empty.");
        }
        if (schema.getRows() <= 0 || schema.getRows() > 10000) {
            return ResponseEntity.badRequest().body("Rows must be between 1 and 10000.");
        }
        if (schema.getFormat() == null || schema.getFormat().isBlank()) {
            schema.setFormat("SQL");
        }

        try {
            String result = dataGeneratorService.generate(
                    schema.getSql(), schema.getRows(), schema.getFormat());

            if (null != result) {
                dataGeneratorService.logGeneration(anonId, null, schema.getRows(), schema.getTables());
            }
            return ResponseEntity.ok(result);
        } catch (IllegalArgumentException e) {
            logger.warn("Invalid schema: {}", e.getMessage());
            return ResponseEntity.badRequest()
                    .body("Schema validation failed. Please check your CREATE TABLE syntax.");
        } catch (Exception e) {
            logger.error("Unexpected error during generation", e);
            return ResponseEntity.internalServerError().body("An unexpected error occurred.");
        }
    }
}
