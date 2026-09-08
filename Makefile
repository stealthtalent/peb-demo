.PHONY: db-create db-schema db-drop run test test-short vet tidy

# ─── Database ────────────────────────────────────────────────────────

# Create the peb_demo database in the local Postgres (docker-compose on 8432).
db-create:
	psql -h localhost -U test -d postgres -c "CREATE DATABASE peb_demo;" 2>/dev/null || true

# Apply the demo schema to peb_demo.
db-schema:
	psql -h localhost -U test -d peb_demo -f schemas/demo.sql

# Drop the demo database (for clean rebuilds).
db-drop:
	psql -h localhost -U test -d postgres -c "DROP DATABASE IF EXISTS peb_demo;" 2>/dev/null || true

# ─── Application ─────────────────────────────────────────────────────

# Run the HTMX web server (default port 8080).
run:
	go run ./src/

# ─── Quality ─────────────────────────────────────────────────────────

test:
	go test ./... -race -count=1 -v

test-short:
	go test ./... -short -count=1

vet:
	go vet ./...

tidy:
	go mod tidy
