Plan
I’d implement this as an LLM-assisted correlation layer in the backend, scoped to one generated row at a time. Faker should stay the baseline for every column, and the LLM should only add correlation metadata when a field actually needs to be linked, while the backend validates that output and enforces the final row values so names, emails, and usernames stay aligned naturally.

Steps

Keep Faker as the default generator for every column, and ask the LLM only for a compact JSON correlation plan for columns that need linkage.
Add a validation/normalization layer inside backend/src/main/java/com/rowforge/service/DataGeneratorService.java so malformed or unsupported model output falls back to simple heuristics.
Use the current AI-generated Faker suggestions as the baseline for all columns, then overlay correlation metadata only where the LLM says it is needed.
Create one row-scoped profile per generated row and use it to derive all correlated columns in that row, instead of generating each field independently.
Derive email and username from the generated identity profile with a stable template for that row, so values like Jane Doe and Jane.Doe@example.com line up consistently.
Add tests around the backend generator to verify that correlated fields stay in sync within a row, while unrelated columns still behave as before, and that malformed LLM output falls back safely.
Document the behavior in README.md so users know Faker is the default and correlation is added only when needed.
Relevant files

backend/src/main/java/com/rowforge/service/DataGeneratorService.java — main generation flow, LLM prompt, validation, and correlation enforcement.
backend/src/main/java/com/rowforge/model/Schema.java — only needed if you later expose seed or correlation settings.
backend/src/main/java/com/rowforge/controller/GenerateController.java — likely unchanged for the MVP.
frontend/src/app/generate/generate.component.ts — only needed if you decide to expose new user options.
README.md — update the documented behavior after implementation.
Verification

Generate a schema with first_name, last_name, email, and username columns and confirm the fields are internally consistent for each row.
Run the backend tests and confirm SQL, CSV, and JSON output still work.
Check that non-correlated fields continue to use the current AI/Faker pipeline.
Confirm the fallback path still works when the LLM returns invalid or missing correlation output.
Decisions

Scope is limited to same-row identity correlation, not foreign keys or cross-table referential integrity.
The LLM chooses correlation groups, but the backend owns validation and final value derivation.
No API change is required for the MVP.
Deterministic consistency within a dataset is preferred over adding user-facing correlation settings immediately.

Further Considerations

If LLM cost or latency becomes a concern, cache the correlation plan per schema hash and reuse it across requests.
If the model output proves too flexible, narrow the contract to a small enum of supported correlation types instead of free-form rules.