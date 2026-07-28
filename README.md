# Aptos Core Processors (SDK version)
Processors that index data from the Aptos Transaction Stream (GRPC). These processors have been (re)-written using the new Indexer SDK.

- **Note: Official releases coming soon!**

## Overview
This tutorial shows you how to run the Aptos core processors in this repo.

If you want to index a custom contract, we recommend using the [Quickstart Guide](https://aptos.dev/en/build/indexer/indexer-sdk/quickstart).

### Prerequisite

- A running PostgreSQL instance, with a valid database. More tutorial can be
  found [here](https://github.com/aptos-labs/aptos-core/tree/main/crates/indexer#postgres)

- [diesel-cli](https://diesel.rs/guides/getting-started)

- A `config.yaml` file. See example [here](./processor/example-config.yaml).

#### `config.yaml` Explanation

- `processor_config`
    - `type`: which processor to run
    - `channel_size`: size of channel in between steps
    - `tables_to_write`: which tables this processor writes. See [Selecting tables to write](#selecting-tables-to-write) below.
    - Some processors require additional configuration. See the full list of configs [here](./processor/src/config/processor_config.rs#L102).

##### Selecting tables to write

`tables_to_write` takes Postgres table names, matched case-insensitively. Unrecognized names are skipped with a warning rather than failing startup, so check the logs if a table is unexpectedly empty.

- **Omitted or empty**: every table the processor supports, except the opt-in-only ones below.
- **Non-empty**: an allowlist -- only the tables you name are written. Anything you leave out is silently *not* written, so list every table you want.

Each processor logs its effective selection at startup (`Writing all default tables for this processor; ...`), which is the quickest way to confirm a config does what you meant.

###### Opt-in-only tables

Three historical tables are **not** written unless you name them explicitly:

| Table | Processor |
|---|---|
| `fungible_asset_balances` | `fungible_asset_processor` |
| `token_datas_v2` | `token_v2_processor` |
| `token_ownerships_v2` | `token_v2_processor` |

They hold one row per write set change rather than one row per entity, so they grow much faster than their `current_` counterparts -- size storage accordingly before enabling them.

Naming an opt-in-only table does **not** turn `tables_to_write` into an allowlist for everything else. This enables the two historical tables while leaving every other `token_v2_processor` table on:

```yaml
processor_config:
  type: token_v2_processor
  tables_to_write:
    - token_datas_v2
    - token_ownerships_v2
```

To restrict to an explicit set instead, list all the tables you want:

```yaml
processor_config:
  type: token_v2_processor
  tables_to_write:
    - current_token_datas_v2
    - current_token_ownerships_v2
    - token_datas_v2
```

Caveats when enabling these:

- **No backfill.** Rows only start appearing from the version the processor is at when you enable the flag. Inserts use `ON CONFLICT ... DO NOTHING`, so replaying earlier versions will not fill in or correct history you missed.
- **`token_datas_v2.is_deleted_v2` is always `NULL`.** Burns are only recorded in `current_token_datas_v2`; no historical row is emitted for them. Do not use `token_datas_v2` alone to determine whether a token still exists.
- **The `legacy_migration_v1` views stay incomplete.** `legacy_migration_v1.coin_balances` reads `fungible_asset_balances` directly, so it will start returning *partial* history -- data covering only the period since you enabled the flag. The token views (`legacy_migration_v1.token_ownerships`, `token_activities`) join `collections_v2`, which no Postgres processor writes, so they continue to return zero rows.
- **Supporting indexes are not created.** The indexes these tables need for the legacy views are commented out in [the migration](./processor/src/db/migrations/2024-05-22-200847_add_v1_migration_views/up.sql) because they must be built `CONCURRENTLY` outside of diesel. Create them before querying at any scale.

- `processor_mode`: The processor can be run in these modes:
    - Default (bootstrap) mode: On first run, the processor will start from `initial_starting_version`. Upon restart, the processor continues from `processor_status.last_success_version` saved in DB. 
        ```
        processor_mode:
            type: default
            initial_starting_version: 0
        ```
    - Backfill mode: Running in backfill mode will track the backfill status in `backfill_processor_status` table. Give your backfill a unique identifier, `backfill_id`. If the backfill restarts, it will continue from `backfill_processor_status.last_success_version`. 
        ```
        processor_mode:
            type: backfill
            backfill_id: bug_fix_101 # Appended to `processor_type` for a unique backfill identifier
            initial_starting_version: 0 # Processor starts here unless there is a greater checkpointed version
            ending_version: 1000 # If no ending_version is set, it will use `processor_status.last_success_version`
            overwrite_checkpoint: false # Overwrite checkpoints if it exists, restarting the backfill from `initial_starting_version`. Defaults to false
        ```
    - Testing mode: This mode is used to replay the processor for specific transaction versions. The processor always starts at `override_starting_version` and does not update the `processor_status` table. If no `ending_version` is set, the processor will run only using `override_starting_version` (1 transaction).
        ```
        processor_mode:
            type: testing
            override_starting_version: 100
            ending_version: 200 # Optional. Defaults to override_starting_version
        ``

- `transaction_stream_config`
    - `indexer_grpc_data_service_address`: Data service non-TLS endpoint address. See [available Transaction Stream endpoints](https://aptos.dev/en/build/indexer/txn-stream/aptos-hosted-txn-stream).
    - `auth_token`: Auth token used for connection. See [instructions on how to get an auth token](https://aptos.dev/en/build/indexer/txn-stream/aptos-hosted-txn-stream).
    - `request_name_header`: request name header to append to the grpc request; name of the processor
    - `additional_headers`: addtional headers to append to the grpc request
    - `indexer_grpc_http2_ping_interval_in_secs`: client-side grpc HTTP2 ping interval.
    - `indexer_grpc_http2_ping_timeout_in_secs`: client-side grpc HTTP2 ping timeout.
    - `indexer_grpc_reconnection_timeout_secs`: grpc reconnection timeout
    - `indexer_grpc_response_item_timeout_secs`: grpc response item timeout
   
- `db_config`
    - `type`: type of storage, `postgres_config` or `parquet_config`
    - `connection_string`: PostgresQL DB connection string


### Use docker image for existing processors (Only for **Unix/Linux**)

- Use the provided `Dockerfile` and `config.yaml` (update accordingly)
    - Build: `cd ecosystem/indexer-grpc/indexer-grpc-parser && docker build . -t indexer-processor`
    - Run: `docker run indexer-processor:latest`

### Use source code for existing parsers

- Use the provided `config.yaml` (update accordingly)
- Run `cd processor && cargo run --release -- -c config.yaml`


### Manually running diesel-cli
- `cd` into the database folder you use under `processor/src/db/`, then run it.

## Processor Specific Notes

### Supported Coin Type Mappings
See mapping in [v2_fungible_asset_balances.rs](https://github.com/aptos-labs/aptos-indexer-processors/blob/main/rust/processor/src/db/common/models/fungible_asset_models/v2_fungible_asset_balances.rs#L40) for a list supported coin type mappings.
