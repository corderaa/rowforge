import { Component, OnDestroy } from '@angular/core';
import { HttpClient } from '@angular/common/http';

@Component({
  selector: 'app-generate',
  templateUrl: './generate.component.html',
  styleUrls: ['./generate.component.css']
})
export class GenerateComponent implements OnDestroy {
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

  private _downloadUrl = '';
  private readonly apiUrl = '/api/generate';
  private readonly anonIdStorageKey = 'rowforgeAnonId';

  constructor(private http: HttpClient) {}

  generate(): void {
    if (!this.sql.trim()) {
      this.error = 'Please paste a SQL schema before generating.';
      return;
    }
    this.loading = true;
    this.error = '';
    this.result = '';
    this.revokeDownloadUrl();

    this.http
      .post(
        this.apiUrl,
        { sql: this.sql, rows: this.rows, format: this.format },
        {
          responseType: 'text',
          headers: { 'X-Anon-Id': this.getOrCreateAnonId() },
        }
      )
      .subscribe({
        next: (data) => {
          this.result = data;
          this.loading = false;
          this.createDownloadUrl();
        },
        error: (err) => {
          this.error = err.error ?? 'An error occurred. Is the backend running on port 8080?';
          this.loading = false;
        },
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
    return this._downloadUrl;
  }

  ngOnDestroy(): void {
    this.revokeDownloadUrl();
  }

  private createDownloadUrl(): void {
    this.revokeDownloadUrl();
    const blob = new Blob([this.result], { type: this.downloadMimeType });
    this._downloadUrl = URL.createObjectURL(blob);
  }

  private revokeDownloadUrl(): void {
    if (this._downloadUrl) {
      URL.revokeObjectURL(this._downloadUrl);
      this._downloadUrl = '';
    }
  }

  private getOrCreateAnonId(): string {
    const existing = localStorage.getItem(this.anonIdStorageKey);
    if (existing && existing.trim()) {
      return existing;
    }

    const created = crypto.randomUUID();
    localStorage.setItem(this.anonIdStorageKey, created);
    return created;
  }
}
