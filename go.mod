module github.com/stealthtalent/peb-demo

go 1.26

require (
	github.com/jackc/pgx/v5 v5.10.0
	github.com/stealthtalent/postgres-event-bus v0.0.0
)

require (
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	go.uber.org/zap v1.28.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/text v0.40.0 // indirect
)

replace github.com/stealthtalent/postgres-event-bus => ../postgres-event-bus
