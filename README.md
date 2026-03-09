# RowForge

> **SQL Test Data Generator** — paste a `CREATE TABLE` schema, get realistic fake data instantly.

RowForge is a full-stack developer tool that lets you generate SQL, CSV, or JSON test data from a SQL schema in seconds. It uses [DataFaker](https://datafaker.net/) for realistic values and [JSQLParser](https://github.com/JSQLParser/JSqlParser) to parse your schema.

---

## Tech Stack

| Layer    | Technology                      |
|----------|---------------------------------|
| Backend  | Java 17, Spring Boot 3, Maven   |
| Frontend | Angular 17, TypeScript          |
| Faker    | DataFaker 2.x                   |
| Parser   | JSQLParser 4.x                  |

---

## Project Structure

```
rowforge/
├── backend/                       # Spring Boot application
│   ├── src/main/java/com/rowforge/
│   │   ├── RowForgeApplication.java
│   │   ├── controller/GenerateController.java
│   │   ├── service/DataGeneratorService.java
│   │   └── model/Schema.java
│   └── pom.xml
└── frontend/                      # Angular application
    ├── src/app/
    │   ├── app.module.ts
    │   ├── app.component.ts
    │   └── generate/
    │       ├── generate.component.ts
    │       ├── generate.component.html
    │       └── generate.component.css
    ├── angular.json
    └── package.json
```

---

## Getting Started

### Prerequisites

- Java 17+
- Maven 3.8+
- Node.js 18+ and npm 9+

### 1 – Run the Backend

```bash
cd backend
mvn spring-boot:run
```

The API will be available at `http://localhost:8080`.

### 2 – Run the Frontend

```bash
cd frontend
npm install
npm start
```

Open `http://localhost:4200` in your browser. The dev server proxies `/api/*` requests to the backend automatically.

---

## API

### `POST /api/generate`

**Request body (JSON):**

```json
{
  "sql": "CREATE TABLE users (id INT, first_name VARCHAR(50), email VARCHAR(100));",
  "rows": 100,
  "format": "SQL"
}
```

| Field    | Type   | Description                          |
|----------|--------|--------------------------------------|
| `sql`    | string | One or more `CREATE TABLE` statements |
| `rows`   | int    | Number of rows to generate (1–10000) |
| `format` | string | `SQL`, `CSV`, or `JSON`              |

**Response:** plain text containing the generated data.

---

## Example Schema

```sql
CREATE TABLE users (
  id          INT,
  first_name  VARCHAR(50),
  last_name   VARCHAR(50),
  email       VARCHAR(100),
  phone       VARCHAR(20),
  city        VARCHAR(50),
  created_at  DATETIME
);
```

---

## Running Tests

```bash
cd backend
mvn test
```

---

## License

[MIT](LICENSE) © 2026 Ugaitz Cordero