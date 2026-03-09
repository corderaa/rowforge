import { Component } from '@angular/core';
import { HttpClient } from '@angular/common/http';

@Component({
  selector: 'app-generate',
  templateUrl: './generate.component.html',
  styleUrls: ['./generate.component.css']
})
export class GenerateComponent {
  sql = `CREATE TABLE users (
  id INT,
  first_name VARCHAR(50),
  last_name VARCHAR(50),
  email VARCHAR(100),
  age INT,
  created_at DATETIME
);`;

  rows = 10;
  format = 'SQL';
  result = '';
  loading = false;
  error = '';

  private readonly apiUrl = '/api/generate';

  constructor(private http: HttpClient) {}

  generate(): void {
    if (!this.sql.trim()) {
      this.error = 'Please paste a SQL schema before generating.';
      return;
    }
    this.loading = true;
    this.error = '';
    this.result = '';

    this.http
      .post(this.apiUrl, { sql: this.sql, rows: this.rows, format: this.format }, { responseType: 'text' })
      .subscribe({
        next: (data) => {
          this.result = data;
          this.loading = false;
        },
        error: (err) => {
          this.error = err.error ?? 'An error occurred. Is the backend running on port 8080?';
          this.loading = false;
        }
      });
  }

  get downloadFilename(): string {
    const ext = this.format === 'SQL' ? 'sql' : this.format === 'CSV' ? 'csv' : 'json';
    return `rowforge-data.${ext}`;
  }

  get downloadMimeType(): string {
    if (this.format === 'JSON') return 'application/json';
    if (this.format === 'CSV') return 'text/csv';
    return 'text/plain';
  }

  get downloadUrl(): string {
    const blob = new Blob([this.result], { type: this.downloadMimeType });
    return URL.createObjectURL(blob);
  }
}
