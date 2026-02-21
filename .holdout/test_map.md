# Test Map: crypto-pipeline

Maps every acceptance criterion to its test(s).

## REQ-001: Backfill Historical Candle Data

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ001_backfill.py | test_backfill_single_asset_populates_candles | Backfill --assets bitcoin stores ~720 hourly candle records |
| AC-2 | test_REQ001_backfill.py | test_backfill_default_assets | Backfill with no --assets stores bitcoin and ethereum |
| AC-3 | test_REQ001_backfill.py | test_backfill_candle_fields | Each candle has open, high, low, close, volume, period |
| AC-4 | test_REQ001_backfill.py | test_backfill_no_duplicate_candles | Running backfill twice creates no duplicates |

## REQ-002: Compute Derived Metrics

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ002_metrics.py | test_sma_present_from_correct_candle | SMA-20 from 20th, SMA-50 from 50th candle onwards |
| AC-2 | test_REQ002_metrics.py | test_sma_20_value_correct | SMA-20 equals arithmetic mean of 20 closes |
| AC-3 | test_REQ002_metrics.py | test_sma_50_value_correct | SMA-50 equals arithmetic mean of 50 closes |
| AC-4 | test_REQ002_metrics.py | test_rolling_volatility_correct | Volatility = pop std dev of log returns over 20 periods |
| AC-5 | test_REQ002_metrics.py | test_vwap_correct | VWAP = cumsum(tp*vol)/cumsum(vol), daily reset |
| AC-6 | test_REQ002_metrics.py | test_sma20_and_volatility_null_insufficient_history | SMA-20 and volatility NULL with <20 candles |
| AC-7 | test_REQ002_metrics.py | test_sma50_null_insufficient_history | SMA-50 NULL with <50 candles |
| Property | test_REQ002_property_metrics.py | test_sma_of_constant_prices_equals_constant | SMA of constant series equals the constant |
| Property | test_REQ002_property_metrics.py | test_volatility_of_constant_prices_is_zero | Volatility of constant series is zero |
| Property | test_REQ002_property_metrics.py | test_sma20_bounded_by_window_min_max | SMA-20 bounded by min/max close in window |
| Property | test_REQ002_property_metrics.py | test_vwap_uniform_volume_equals_avg_typical_price | VWAP with uniform volume = avg typical price |

## REQ-003: Stream Real-Time Price Ticks

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ003_stream.py | test_stream_stores_tick_in_database | Tick received -> stored in ticks table |
| AC-2 | test_REQ003_stream.py | test_stream_ctrl_c_exits_cleanly | SIGINT exits with code 0 |
| AC-3 | test_REQ003_stream.py | test_stream_multiple_assets_separate_records | Multiple assets stored as separate records |

## REQ-004: Automatic Reconnection with Exponential Backoff

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ004_reconnection.py | test_backoff_sequence_mathematical_properties | First delay is 1 second |
| AC-2 | test_REQ004_reconnection.py | test_backoff_sequence_mathematical_properties | Delay doubles each time, cap at 60s |
| AC-3,4 | test_REQ004_reconnection.py | test_reconnection_backoff_timing | Reconnection stores ticks and resets backoff |
| Integration | test_REQ004_reconnection.py | test_stream_reconnect_via_subprocess | Stream handles connection loss gracefully |

## REQ-005: Query Latest Prices

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ005_query_latest.py | test_query_latest_single_asset | Shows asset name, latest price, timestamp |
| AC-2 | test_REQ005_query_latest.py | test_query_latest_default_assets | Default assets show latest for bitcoin and ethereum |
| AC-3 | test_REQ005_query_latest.py | test_query_latest_no_data | No data indicates unavailable |
| Extra | test_REQ005_query_latest.py | test_query_latest_returns_most_recent | Returns most recent by timestamp, not insertion order |

## REQ-006: Query Historical Prices and Metrics

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ006_query_history.py | test_query_history_shows_candles_and_metrics | Output contains candles with SMA, VWAP, volatility |
| AC-2 | test_REQ006_query_history.py | test_query_history_respects_date_range | Only records within start/end dates returned |
| AC-3 | test_REQ006_query_history.py | test_query_history_no_data_in_range | No data in window gives appropriate message |
| AC-4 | test_REQ006_query_history.py | test_query_history_null_metrics_display | NULL metrics shown as empty or N/A |

## REQ-007: Asset Selection Flag

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ007_asset_selection.py | test_assets_flag_comma_separated | --assets bitcoin,ethereum,litecoin operates on all three |
| AC-2 | test_REQ007_asset_selection.py | test_default_assets | No --assets defaults to bitcoin and ethereum |
| AC-3 | test_REQ007_asset_selection.py | test_single_asset | Single asset operates on just that asset |
| Extra | test_REQ007_asset_selection.py | test_assets_flag_on_query_latest | --assets works on query latest subcommand |

## REQ-008: Database Path Flag

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ008_db_path.py | test_db_flag_custom_path | --db uses specified file as database |
| AC-2 | test_REQ008_db_path.py | test_db_default_path | Default is crypto_pipeline.db in cwd |
| AC-3 | test_REQ008_db_path.py | test_db_creates_file_automatically | Non-existent file is created automatically |

## REQ-009: Database Schema Initialisation

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ009_schema.py | test_new_database_creates_tables | New DB gets candles, ticks, metrics tables |
| AC-2 | test_REQ009_schema.py | test_existing_tables_not_dropped | Existing tables preserved, not recreated |
| Extra | test_REQ009_schema.py | test_candles_table_has_required_columns | Candles table has OHLCV + period columns |
| Extra | test_REQ009_schema.py | test_metrics_table_has_required_columns | Metrics table has SMA, volatility, VWAP columns |
| Extra | test_REQ009_schema.py | test_ticks_table_has_required_columns | Ticks table has asset, price, timestamp columns |

## REQ-010: ETL Refresh (Incremental Update)

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ010_refresh.py | test_refresh_incremental_fetch | Only fetches candles newer than latest stored |
| AC-2 | test_REQ010_refresh.py | test_refresh_recomputes_metrics | Metrics recomputed after refresh |
| AC-3 | test_REQ010_refresh.py | test_refresh_no_new_data | No new data available -> no new records |

## REQ-011: CLI Entry Point

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ011_cli_entry.py | test_help_lists_subcommands | --help lists backfill, stream, query, refresh |
| AC-2 | test_REQ011_cli_entry.py | test_invalid_subcommand_exits_nonzero | Invalid subcommand exits non-zero |
| AC-3 | test_REQ011_cli_entry.py | test_query_help_lists_subsubcommands | query --help lists latest and history |
| Extra | test_REQ011_cli_entry.py | test_module_importable | Package is importable |
| Extra | test_REQ011_cli_entry.py | test_subcommand_help[backfill/stream/refresh] | Each subcommand accepts --help |
| Structural | test_structure_cli.py | TestCLIStructure | CLI structure: subcommand routing, help text |
| Structural | test_structure_cli.py | TestFlagAvailability | --assets and --db in help for all subcommands |

## REQ-012: Asset Metadata Storage

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ012_asset_metadata.py | test_backfill_stores_asset_metadata | Assets table has name, symbol, price_usd |
| AC-2 | test_REQ012_asset_metadata.py | test_metadata_upserted_not_duplicated | Metadata upserted, not duplicated |
| Extra | test_REQ012_asset_metadata.py | test_multiple_assets_metadata | Multiple assets stored correctly |

## REQ-013: API Error Handling

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ013_api_errors.py | test_backfill_rate_limited | HTTP 429 -> stderr + non-zero exit |
| AC-2 | test_REQ013_api_errors.py | test_backfill_api_unreachable | Connection error -> stderr + non-zero exit |
| Extra | test_REQ013_api_errors.py | test_backfill_server_error | HTTP 500 handled gracefully |
| Extra | test_REQ013_api_errors.py | test_refresh_api_error | Refresh also handles API errors |

## REQ-014: Numeric Validation

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ014_numeric_validation.py | test_nan_price_skipped | NaN price skipped, warning logged |
| AC-2 | test_REQ014_numeric_validation.py | test_inf_volume_skipped | Inf volume skipped, warning logged |
| AC-3 | test_REQ014_numeric_validation.py | test_valid_numerics_stored | Valid numerics stored normally |
| Extra | test_REQ014_numeric_validation.py | test_negative_inf_skipped | Negative infinity rejected |
| Extra | test_REQ014_numeric_validation.py | test_no_inf_nan_in_database | No inf/NaN values in DB after backfill |
| Property | test_REQ014_property_numeric.py | test_finite_values_accepted | Any finite positive float is accepted |
| Property | test_REQ014_property_numeric.py | test_nan_in_any_field_rejected | NaN in any field causes record skip |
| Property | test_REQ014_property_numeric.py | test_inf_in_any_field_rejected | Inf in any field causes record skip |
| Property | test_REQ014_property_numeric.py | test_mixed_valid_invalid_stores_only_valid | Mix of valid/invalid stores only valid |

## REQ-015: Connection Pooling

| AC | Test File | Test Function | Description |
|---|---|---|---|
| AC-1 | test_REQ015_connection_pooling.py | test_backfill_multiple_assets_reuses_session | Multiple API calls succeed with consistent headers |
| Extra | test_REQ015_connection_pooling.py | test_session_headers_consistent | All requests use consistent User-Agent header |

## Summary

| Metric | Count |
|---|---|
| Requirements covered | 15 / 15 |
| Acceptance criteria covered | 41 / 41 |
| Test files | 16 |
| Test functions (approx) | 55 |
| Property-based test files | 2 |
| Structural test files | 1 |
