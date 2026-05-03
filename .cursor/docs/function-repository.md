# Function Repository

A complete list of every Python function in the app, organised by module and step. Each description is written in plain English so anyone can understand what the function does.

---

## Module 1 — DataStitcher (`DataConsolidationAppV7/backend/module-1/`)

### Upload + Settings

| Function | File | What it does |
|----------|------|--------------|
| `upload` | `routes/data_loading_routes.py` | Receives the uploaded files from the browser, loads them into the session database, and streams real-time progress events (SSE) back to the browser so the user can see which file is being processed. |
| `delete_table_route` | `routes/data_loading_routes.py` | Removes a table from the session when the user clicks delete. |
| `set_header_row` | `routes/data_loading_routes.py` | Changes which row is used as the column header for a table. |
| `_rebuild_meta` | `routes/data_loading_routes.py` | Refreshes the session's summary information (like column counts and row stats) after something changes. |
| `_file_name_from_key` | `data_loading/file_loader.py` | Turns an internal table identifier into a human-readable file name for display. |
| `_clean_header` | `data_loading/file_loader.py` | Tidies up a header cell — trims spaces, converts to text — so column names are consistent. |
| `_to_str` | `data_loading/file_loader.py` | Converts a value to a string without cleaning. Returns None for blank values. |
| `bulk_clean_table` | `data_loading/file_loader.py` | Applies UPPER and TRIM to every column in a table using a single fast SQL statement instead of cleaning each cell one by one in Python. |
| `_sql_all_null_condition` | `data_loading/file_loader.py` | Builds a SQL fragment that checks whether all columns in a row are empty — used to skip blank rows when building data tables from raw tables. |
| `_sql_literal` | `data_loading/file_loader.py` | Safely escapes a string for use as a value in a SQL query. |
| `_is_empty_row` | `data_loading/file_loader.py` | Checks whether a row is completely blank or has no meaningful data. |
| `_iter_csv_rows` | `data_loading/file_loader.py` | Reads a CSV file and produces rows one at a time for processing. |
| `_pad_row` | `data_loading/file_loader.py` | Makes a row the right width by adding empty cells or trimming extras so all rows match. |
| `_load_excel_sheet` | `data_loading/file_loader.py` | Reads one sheet from an Excel file using DuckDB native ingestion for the raw table, then builds the data table from the raw table via SQL — no Python row iteration. |
| `_load_csv` | `data_loading/file_loader.py` | Reads a CSV file using native DataFrame ingestion + SQL. Reads CSV once into memory, ingests natively, then builds the data table via SQL — no Python row loops. |
| `load_zip_to_session` | `data_loading/file_loader.py` | Opens a ZIP file, finds all spreadsheets (.xlsx, .xlsm, .xlsb, .xltx, .xltm), CSVs, and nested ZIP files inside, and loads each one into the session database. Nested ZIPs are extracted one level deep. Accepts an optional progress callback that fires after each file is loaded so the upload route can stream per-file progress to the browser. |
| `_build_columns_from_header` | `data_loading/file_loader.py` | Takes the chosen header row and builds a list of column names and their metadata. |
| `rebuild_table_from_raw_table` | `data_loading/file_loader.py` | Rebuilds the working data table from scratch after the user changes the header row. |
| `infer_file_type` | `data_loading/service.py` | Guesses whether a table came from a CSV, Excel (.xlsx, .xlsm, .xlsb, .xltx, .xltm), or other file type based on its name. |
| `file_display_name` | `data_loading/service.py` | Creates a nice display label for a file (e.g. "Sales_Data.xlsx — Sheet1"). |
| `_bulk_table_meta` | `data_loading/service.py` | Fetches row counts and column lists for many tables at once using two SQL queries instead of querying each table one by one. |
| `build_inventory_from_db` | `data_loading/service.py` | Builds the list of all uploaded tables with their row counts and column info for the sidebar. |
| `build_files_payload_from_db` | `data_loading/service.py` | Builds the data package the browser needs to show file paths, sheet names, and table keys. |
| `build_previews_from_db` | `data_loading/service.py` | Builds quick column-and-header previews for every table to show in the UI. |
| `build_single_preview` | `data_loading/service.py` | Builds a preview (first 50 rows) for one specific table. Used for lazy-loading previews when the user expands a table. |
| `_df_row_to_list` | `data_loading/file_loader.py` | Converts a single DataFrame row into a plain Python list, replacing NaN with None for downstream compatibility. |

### Data Preview

| Function | File | What it does |
|----------|------|--------------|
| `get_preview` | `routes/data_loading_routes.py` | Returns the first 50 rows of a single table as a preview. Called on-demand when the user expands a table card. |
| `get_raw_preview` | `routes/data_loading_routes.py` | Sends back the raw grid of a table (before any header is chosen) so the user can pick the right header row. |
| `get_raw_array_from_table` | `data_loading/file_loader.py` | Reads a chunk of raw rows from the database for the preview screen. |
| `pick_best_rows` | `shared/db/table_ops.py` | Takes a list of row dicts and returns the ones with the most filled-in columns, so previews show the most informative data instead of sparse rows. |
| `pick_best_raw_rows` | `shared/db/table_ops.py` | Same as pick_best_rows but works on raw list-of-lists data (used for the raw grid preview before headers are chosen). |

### Append Strategy

| Function | File | What it does |
|----------|------|--------------|
| `append_plan_route` | `routes/appending_routes.py` | Receives the request when the user clicks "Generate Append Plan" and runs the AI grouping logic. |
| `save_append_groups_route` | `routes/appending_routes.py` | Saves the user's edited group assignments (after moving or renaming groups) to the session. |
| `append_mapping_route` | `routes/appending_routes.py` | Runs the column mapping step that figures out how columns from different tables in a group align. |
| `append_execute_route` | `routes/appending_routes.py` | Runs the actual stacking — combines rows from all tables in each group into one table per group. |
| `_trim_payload` | `appending/service.py` | Shrinks large data structures before sending them to the AI so the request stays within size limits. |
| `run_append_plan` | `appending/service.py` | The core logic that proposes how to group tables together, using AI when available. |
| `save_append_groups` | `appending/service.py` | Writes the group definitions into the session database so they persist. |
| `run_append_mapping` | `appending/service.py` | Figures out which columns across different tables in a group should be treated as the same field. |
| `run_append_execute` | `appending/service.py` | Actually stacks the tables: creates new combined tables and registers them in the session. |
| `_resolve_appended_map` | `summary/insights/service.py` | Looks up the database table name for each appended group so downstream steps can find it. |
| `resolve_group_table` | `summary/insights/service.py` | Figures out which physical database table backs a given append group (the stacked version or the original). |

### Header Normalisation

| Function | File | What it does |
|----------|------|--------------|
| `header_norm_run` | `routes/header_normalisation_routes.py` | Receives the request when the user clicks "Run Header Normalisation" and triggers the AI mapping. |
| `header_norm_apply` | `routes/header_normalisation_routes.py` | Applies the user's approved mappings — renames columns in the actual data tables. |
| `header_norm_preview` | `routes/header_normalisation_routes.py` | Returns a preview of what a single table would look like after applying the mappings. |
| `header_norm_group_preview` | `routes/header_normalisation_routes.py` | Returns a preview for an entire append group after applying mappings. |
| `header_norm_download_excel` | `routes/header_normalisation_routes.py` | Creates and sends an Excel workbook containing the current mapping decisions for offline editing. |
| `header_norm_download_summary` | `routes/header_normalisation_routes.py` | Creates a concise Excel file summarising the column renames — one sheet per group, with two columns: original name and mapped name. |
| `header_norm_upload_excel` | `routes/header_normalisation_routes.py` | Reads an uploaded Excel mapping file and applies those decisions back into the session. |
| `run_header_norm` | `header-normalisation/service.py` | Runs the full header normalisation pipeline for each table — deterministic matching first, then AI for the rest. |
| `_get_data_rows` | `header-normalisation/service.py` | Fetches a sample of data rows from a table to give the AI context about what each column contains. |
| `_to_ui_decision` | `header-normalisation/service.py` | Converts the internal matching result into the format the browser expects for display. |
| `apply_header_norm` | `header-normalisation/service.py` | Renames columns in the database and updates all related metadata after the user confirms mappings. |
| `_resolve_tbl` | `header-normalisation/service.py` | Looks up the real database table name for a logical table key. |
| `_rebuild_meta` | `header-normalisation/service.py` | Refreshes session metadata after normalisation is applied. |
| `get_table_preview` | `header-normalisation/service.py` | Builds a preview showing the table's headers and sample rows after normalisation. |
| `_resolve_knowledge_base_dir` | `header-normalisation/alias_store.py` | Finds the folder on disk where learned alias mappings are stored. |
| `_load_learned_aliases` | `header-normalisation/alias_store.py` | Loads previously learned header-to-field mappings from a file on disk. |
| `_save_learned_aliases` | `header-normalisation/alias_store.py` | Saves newly learned mappings to disk so they're available next time. |
| `alias_add` | `header-normalisation/alias_store.py` | Adds a new alias — records that a particular raw header name maps to a standard field. |
| `get_alias_store_stats` | `header-normalisation/alias_store.py` | Returns counts of how many aliases are stored for each standard field. |
| `merge_into_lookup` | `header-normalisation/alias_store.py` | Combines learned aliases into the main lookup table used during matching. |
| `merge_learned_aliases` | `header-normalisation/alias_store.py` | Loads learned aliases from disk and merges them into the active lookup in one step. |
| `save_snapshot` | `header-normalisation/alias_store.py` | Saves a record of the original headers vs. what they were mapped to, for future learning. |
| `load_snapshot` | `header-normalisation/alias_store.py` | Loads a previously saved mapping snapshot for review or reuse. |
| `_sample_value_hint` | `header-normalisation/matching_engine.py` | Creates a short hint from sample values to help the matching engine understand what a column contains. |
| `_fuzzy_score` | `header-normalisation/matching_engine.py` | Calculates how similar a column name is to a standard field name using fuzzy text comparison. |
| `map_single_header` | `header-normalisation/matching_engine.py` | The main matching function for one column — tries 8 different matching methods from exact to fuzzy, and flags columns that need AI help. |
| `profile_table_columns` | `header-normalisation/profiler.py` | Analyses each column in a table to determine its data type, sample values, and other stats that help matching. |
| `_neighbours` | `header-normalisation/profiler.py` | Returns the names of columns next to a given column, which can help figure out what it contains. |
| `_get_sample_values` | `header-normalisation/ai_mapper.py` | Pulls a few example values from a column to include in the AI prompt. |
| `_call_ai_json` | `header-normalisation/ai_mapper.py` | Sends a request to the AI and gets back a structured JSON answer for column mapping. |
| `ai_map_unmapped` | `header-normalisation/ai_mapper.py` | Sends all the columns that couldn't be matched automatically to the AI in batches for suggestions. |
| `ai_validate_mapped` | `header-normalisation/ai_mapper.py` | Asks the AI to double-check columns that were already matched automatically, to catch mistakes. |
| `_tokenize` | `header-normalisation/deterministic_matcher.py` | Breaks a column name into individual words for comparison (e.g. "invoice_date" becomes ["invoice", "date"]). |
| `_jaccard` | `header-normalisation/deterministic_matcher.py` | Measures how much two sets of words overlap — used to compare column names without AI. |
| `_summarise_score` | `header-normalisation/deterministic_matcher.py` | Turns a numeric similarity score into a human-readable label like "strong match" or "weak match". |
| `score_deterministic` | `header-normalisation/deterministic_matcher.py` | Scores how well a column name matches a standard field using rules only (no AI). |
| `_norm` | `header-normalisation/aliases.py` | Cleans up a header name (lowercase, trim) so it can be looked up in the alias dictionary. |
| `split_camel` | `header-normalisation/aliases.py` | Splits names written in CamelCase into separate words (e.g. "InvoiceDate" becomes "Invoice Date"). |
| `expand_abbrevs` | `header-normalisation/aliases.py` | Expands common procurement abbreviations in column names (e.g. "amt" becomes "amount"). |
| `infer_value_type` | `header-normalisation/aliases.py` | Looks at sample values and guesses whether the column contains dates, numbers, currencies, etc. |
| `semantic_hints` | `header-normalisation/aliases.py` | Produces keyword tags from sample values to help identify what a column is about. |

### Data Cleaning

| Function | File | What it does |
|----------|------|--------------|
| `standard_field_dtypes` | `routes/inventory_routes.py` | Returns the expected data types for each standard procurement field (e.g. "Total Spend" should be a number). |
| `clean_table` | `routes/inventory_routes.py` | Receives the request when the user clicks "Apply Cleaning" for a single table and runs all selected cleaning steps. |
| `clean_group` | `routes/inventory_routes.py` | Runs the same cleaning steps across all tables in a group at once. |
| `delete_rows` | `routes/inventory_routes.py` | Deletes specific rows the user selected from a table. |
| `dedup_preview` | `routes/inventory_routes.py` | Shows how many duplicate rows would be removed if deduplication is applied with the chosen key columns. |
| `dedup_apply` | `routes/inventory_routes.py` | Actually removes the duplicate rows from the table(s) after the user confirms. |
| `analyze_column_format_route` | `routes/inventory_routes.py` | Analyses the formats and patterns in a column (e.g. "80% of dates are dd/mm/yyyy, 20% are mm-dd-yyyy"). |
| `apply_column_standardize_route` | `routes/inventory_routes.py` | Applies a chosen standardization rule to a column (e.g. convert all dates to one format). |
| `concat_columns_apply_route` | `routes/inventory_routes.py` | Combines two or more columns into a single new column with a separator between values. |
| `delete_concat_column_route` | `routes/inventory_routes.py` | Removes a column that was previously created by concatenation. |
| `remove_columns_route` | `routes/inventory_routes.py` | Drops selected columns from a table (keeps a backup so they can be restored). |
| `restore_columns_route` | `routes/inventory_routes.py` | Brings back columns that were previously removed, using the backup copy. |
| `_shadow_name` | `inventory/service.py` | Generates an internal working-copy table name so the original table isn't modified directly. |
| `_work_name` | `inventory/service.py` | Figures out which database table is the current "working" version of a logical table. |
| `_rebuild_from_select` | `inventory/service.py` | Rebuilds a table by running a query and saving the result as a new table (used during cleaning). |
| `_delete_null_or_empty_rows` | `inventory/service.py` | Removes rows where the chosen columns are blank or empty. |
| `_apply_case_and_trim` | `inventory/service.py` | Applies text transformations: trim whitespace, convert to uppercase or lowercase. |
| `_apply_column_types` | `inventory/service.py` | Tries to convert columns to their expected types (e.g. text that looks like numbers gets stored as numbers). |
| `_deduplicate_rows` | `inventory/service.py` | Removes duplicate rows, keeping only the first occurrence based on key columns. |
| `dedup_preview_stats` | `inventory/service.py` | Calculates how many duplicates exist for each combination of key columns, for the preview screen. |
| `dedup_apply_group` | `inventory/service.py` | Removes duplicates across all tables in a group in one operation. |
| `delete_rows_sql` | `inventory/service.py` | Deletes rows from a table, optionally only those matching certain conditions. |
| `clean_table_sql` | `inventory/service.py` | Runs the full cleaning pipeline on one table: remove empties, trim, standardise case, fix types. |
| `clean_group_sql` | `inventory/service.py` | Runs the full cleaning pipeline on every table in a group, using the same settings. |
| `analyze_column_format` | `inventory/service.py` | Looks at all values in a column and reports what formats are present (dates, numbers, patterns). |
| `apply_column_standardize` | `inventory/service.py` | Applies a chosen standardization to a column (rebuilds the table with the transformed values). |
| `concat_columns_apply` | `inventory/service.py` | Creates a new column by joining the values of several columns with a separator. |
| `delete_concat_column` | `inventory/service.py` | Removes a concatenated column and rebuilds the table without it. |
| `remove_columns` | `inventory/service.py` | Removes columns from a table while saving a backup of the original so they can be restored. |
| `restore_columns` | `inventory/service.py` | Restores previously removed columns from the backup. |
| `array_to_objects` | `data_loading/file_loader.py` | Converts a grid of rows into a list of row-objects (one per row, with column names as keys). |
| `clean_rows_sql` | `data_loading/file_loader.py` | Cleans up row data (fixes types, trims text) before saving it to the database. |

### Merge

| Function | File | What it does |
|----------|------|--------------|
| `recommend_base` | `routes/merging_routes.py` | Receives the request to recommend which table should be the "base" for merging and returns the AI's suggestion. |
| `common_columns` | `routes/merging_routes.py` | Returns a list of columns that appear in both the base and source tables, so the user can pick join keys. |
| `simulate` | `routes/merging_routes.py` | Runs a trial join without saving anything — returns match rates and warnings so the user can decide. |
| `execute` | `routes/merging_routes.py` | Runs the actual merge and streams progress updates to the browser in real time. |
| `finalize` | `routes/merging_routes.py` | Wraps up the merge — saves final metadata and marks the merge as complete. |
| `skip` | `routes/merging_routes.py` | Records that the user chose to skip merging for this group. |
| `redo_clear_cache` | `routes/merging_routes.py` | Clears all cached merge results so the user can start the merge step fresh. |
| `delete_output` | `routes/merging_routes.py` | Deletes a saved merge output from the session. |
| `register_merged_group` | `routes/merging_routes.py` | Records metadata about a merged group after it's been transferred from another step. |
| `download_csv` | `routes/merging_routes.py` | Sends the merged data to the browser as a CSV file download. |
| `download_step_xlsx` | `routes/merging_routes.py` | Sends a specific merge-step table as an Excel file download. |
| `download_step_csv` | `routes/merging_routes.py` | Sends a specific merge-step table as a CSV file download. |
| `download_xlsx` | `routes/merging_routes.py` | Sends the main merged output as an Excel file download. |
| `download_all` | `routes/merging_routes.py` | Bundles all merge outputs into one Excel download. |
| `download_all_csv` | `routes/merging_routes.py` | Bundles all merge outputs as multiple CSV files for download. |
| `merge_history_route` | `routes/merging_routes.py` | Returns the list of all past merge versions so the user can browse history. |
| `table_preview` | `routes/merging_routes.py` | Returns a paginated preview of any merge-related table for the UI. |
| `group_preview_route` | `routes/merging_routes.py` | Returns a preview of a merge input group's data. |
| `transfer_to_normalizer` | `routes/merging_routes.py` | Sends the merged data to the Normalizer module (Module 2). |
| `transfer_to_analyzer` | `routes/merging_routes.py` | Sends the merged data to the Summarizer module (Module 3). |
| `_strip_file_ext` | `merging/guided_merge_service.py` | Removes the file extension from a name for cleaner display and comparison. |
| `_normalize_col` | `merging/guided_merge_service.py` | Cleans up a column name so it can be compared to other column names reliably. |
| `_norm_key_expr` | `merging/guided_merge_service.py` | Builds a database expression that normalises a join key for more accurate matching. |
| `recommend_base_file` | `merging/guided_merge_service.py` | Scores each table to decide which one makes the best "base" for joining. Returns a graceful session_expired indicator instead of crashing when the session data is missing (e.g. after a server restart). |
| `find_common_columns` | `merging/guided_merge_service.py` | Finds columns that exist in both tables and returns details about each for the join-key picker. |
| `classify_single_column` | `merging/guided_merge_service.py` | Uses rules to classify what kind of data a column holds (identifier, category, amount, date, etc.). |
| `classify_all_columns` | `merging/guided_merge_service.py` | Runs the column classifier on every column in a table at once. |
| `classify_columns` | `merging/guided_merge_service.py` | Classifies columns using rules first, then optionally asks the AI for harder cases. |
| `simulate_join` | `merging/guided_merge_service.py` | Runs the join in simulation mode — calculates match rates, duplicate risks, and unmatched rows without saving. |
| `execute_merge` | `merging/guided_merge_service.py` | Runs the actual database join, producing the merged result table. |
| `generate_validation_report` | `merging/guided_merge_service.py` | Creates a quality report after merging, showing match rates and potential issues. |
| `finalize_merge` | `merging/guided_merge_service.py` | Saves the final merged table, registers it in the session, and updates all metadata. |
| `skip_merge` | `merging/guided_merge_service.py` | Records that the user skipped merging and clears any partial merge results. |
| `persist_merge_output` | `merging/guided_merge_service.py` | Saves merge results permanently and stores bookkeeping information about what was merged. |
| `delete_merge_output` | `merging/guided_merge_service.py` | Removes merge results from the session database. |
| `fuzzy_match_column` | `merging/column_metadata.py` | Tries to match a raw column name to a known standard procurement field using fuzzy text comparison. |
| `get_color_for_eligibility` | `merging/column_metadata.py` | Returns a colour code for the UI based on how suitable a column is for joining. |
| `match_keys_distinct_sql` | `db/join_ops.py` | Creates a database query to check how many distinct key values overlap between two tables. |
| `compute_composite_match_rate_sql` | `db/join_ops.py` | Creates a query to estimate the match rate when using multiple columns as a combined join key. |
| `left_join_sql` | `db/join_ops.py` | Creates the database query that joins two tables together based on matching columns. |
| `check_dim_uniqueness` | `db/join_ops.py` | Checks whether a column has unique values (important for deciding if it's safe to join on). |
| `profile_all_columns` | `db/join_ops.py` | Calculates stats for every column in a table — how many unique values, min, max, fill rate. |
| `classify_dim_columns` | `db/join_ops.py` | Identifies which columns act as categories/dimensions vs. which are numeric measures. |
| `detect_format_pattern` | `db/join_ops.py` | Looks at sample values and detects formatting patterns (e.g. "all values start with 'PO-' followed by numbers"). |
| `build_adaptive_normalization` | `db/join_ops.py` | Creates database expressions that clean up join keys on the fly (trim, lowercase, remove prefixes). |

### Data Quality

| Function | File | What it does |
|----------|------|--------------|
| `dqa_date` | `routes/data_quality_routes.py` | Receives the date quality request. Runs SQL under the session lock, then calls AI outside the lock so other panels aren't blocked. |
| `dqa_currency` | `routes/data_quality_routes.py` | Receives the currency quality request. Runs SQL under the session lock, then calls AI outside the lock. |
| `dqa_payment_terms` | `routes/data_quality_routes.py` | Receives the payment terms quality request. Runs SQL under the session lock, then calls AI outside the lock. |
| `dqa_country_region` | `routes/data_quality_routes.py` | Receives the country/region quality request. Runs SQL under the session lock, then calls AI outside the lock. |
| `dqa_supplier` | `routes/data_quality_routes.py` | Receives the supplier quality request. Runs SQL under the session lock, then calls AI outside the lock. |
| `_session_lock` | `routes/data_quality_routes.py` | Prevents two quality assessments from running at the same time for the same session. |
| `_parse_request_fields` | `routes/data_quality_routes.py` | Reads the standard fields (session ID, table name, API key) from every quality assessment request without touching the database. |
| `_resolve_table_under_lock` | `routes/data_quality_routes.py` | Gets the database connection and resolves the table name, all under the session lock so there are no race conditions. |
| `TableMissingError` | `data_quality_assessment/service.py` | A custom error type raised when a table is missing from the session but the session itself is still alive. |
| `_validate_table` | `data_quality_assessment/service.py` | Checks that the requested table actually exists before running an assessment. Raises TableMissingError if the table is gone but the session is alive. |
| `run_dqa_date_sql` | `data_quality_assessment/service.py` | Runs the database queries for the date quality check — must be called under the session lock. |
| `run_dqa_date_ai` | `data_quality_assessment/service.py` | Generates the AI insight for the date panel — safe to call without any lock. |
| `run_dqa_currency_sql` | `data_quality_assessment/service.py` | Runs the database queries for the currency quality check — must be called under the session lock. |
| `run_dqa_currency_ai` | `data_quality_assessment/service.py` | Generates the AI insight for the currency panel — safe to call without any lock. |
| `run_dqa_payment_terms_sql` | `data_quality_assessment/service.py` | Runs the database queries for the payment terms quality check — must be called under the session lock. |
| `run_dqa_payment_terms_ai` | `data_quality_assessment/service.py` | Generates the AI insight for the payment terms panel — safe to call without any lock. |
| `run_dqa_country_region_sql` | `data_quality_assessment/service.py` | Runs the database queries for the country/region quality check — must be called under the session lock. |
| `run_dqa_country_region_ai` | `data_quality_assessment/service.py` | Generates the AI insight for the country/region panel — safe to call without any lock. |
| `run_dqa_supplier_sql` | `data_quality_assessment/service.py` | Runs the database queries for the supplier quality check — must be called under the session lock. |
| `run_dqa_supplier_ai` | `data_quality_assessment/service.py` | Generates the AI insight for the supplier panel — safe to call without any lock. |
| `run_supplier_analysis_sql` | `data_quality_assessment/supplier_analysis.py` | Queries the database for top suppliers by spend and unique supplier count. |
| `run_supplier_analysis_ai` | `data_quality_assessment/supplier_analysis.py` | Sends the supplier list to AI for normalisation assessment. |
| `run_payment_terms_analysis_sql` | `data_quality_assessment/payment_terms_analysis.py` | Queries the database for payment terms breakdown by spend. |
| `run_payment_terms_analysis_ai` | `data_quality_assessment/payment_terms_analysis.py` | Sends the payment terms data to AI for insight generation. |
| `_pick_spend_column` | `data_quality_assessment/date_analysis.py` | Picks the best spend column for the date panel — tries reporting currency first, then local. |
| `_build_date_extract_sql` | `data_quality_assessment/date_analysis.py` | Creates database queries that pull apart dates into year, month, etc. for analysis. |
| `_build_format_table` | `data_quality_assessment/date_analysis.py` | Builds a breakdown table showing how many dates are in each format. |
| `_build_reporting_pivot` | `data_quality_assessment/date_analysis.py` | Creates a pivot showing spend by reporting period (e.g. monthly or quarterly). |
| `_build_currency_crosstab_pivot` | `data_quality_assessment/date_analysis.py` | Creates a cross-tab of currencies vs. dates for mixed-currency analysis. |
| `run_date_analysis_sql` | `data_quality_assessment/date_analysis.py` | Runs the database queries for date format detection, spend pivot, and total spend summary. |
| `run_date_analysis_ai` | `data_quality_assessment/date_analysis.py` | Sends the date data to AI for insight generation. |
| `run_currency_analysis_sql` | `data_quality_assessment/currency_analysis.py` | Queries the database for currency metrics and per-currency breakdown. |
| `run_currency_analysis_ai` | `data_quality_assessment/currency_analysis.py` | Sends the currency data to AI for insight generation. |
| `_unique_values` | `data_quality_assessment/country_region_analysis.py` | Gets the list of unique values in a country or region column. |
| `run_country_region_analysis_sql` | `data_quality_assessment/country_region_analysis.py` | Queries the database for unique country and region values. |
| `run_country_region_analysis_ai` | `data_quality_assessment/country_region_analysis.py` | Sends the country/region data to AI for insight generation. |
| `generate_financial_insights` | `data_quality_assessment/ai_prompts.py` | Sends date, currency, and payment terms data to AI in one call and gets back structured 3-point insights for each panel. |
| `generate_entity_insights` | `data_quality_assessment/ai_prompts.py` | Sends country/region and supplier data to AI in one call and gets back structured 3-point insights for each panel. |
| `generate_date_insight` | `data_quality_assessment/ai_prompts.py` | Asks the AI to write a structured 3-point summary of the date quality findings. Uses the consolidated financial prompt internally. |
| `generate_currency_insight` | `data_quality_assessment/ai_prompts.py` | Asks the AI to write a structured 3-point summary of the currency quality findings. Uses the consolidated financial prompt internally. |
| `generate_payment_terms_insight` | `data_quality_assessment/ai_prompts.py` | Asks the AI to write a structured 3-point summary of the payment terms findings. Uses the consolidated financial prompt internally. |
| `generate_country_region_insight` | `data_quality_assessment/ai_prompts.py` | Asks the AI to write a structured 3-point summary of the country/region findings. Uses the consolidated entity prompt internally. |
| `generate_supplier_insight` | `data_quality_assessment/ai_prompts.py` | Asks the AI to write a structured 3-point summary of the supplier quality findings. Uses the consolidated entity prompt internally. |
| `_normalise_insight` | `data_quality_assessment/ai_prompts.py` | Makes sure AI insight output is always a list of strings, handling both old string-based and new array-based formats. |
| `resolve_column` | `data_quality_assessment/column_resolver.py` | Finds the best matching column for a given role (like "date" or "vendor_name") from the columns in the table. Tries exact match first, then known aliases, then fuzzy matching. |
| `resolve_all_columns` | `data_quality_assessment/column_resolver.py` | Finds all columns in the table that match a given role, not just the first one. Returns them ordered by match quality. |
| `find_date_columns` | `data_quality_assessment/column_resolver.py` | Finds all date-related columns using the resolver, plus any column with "date" in the name as a fallback. |
| `find_country_columns` | `data_quality_assessment/column_resolver.py` | Finds all country-type columns, plus any column with "country" in the name as a fallback. |
| `find_currency_columns` | `data_quality_assessment/column_resolver.py` | Finds all currency-type columns using the resolver, plus any column with "curr" in the name as a fallback. |
| `find_payment_terms_columns` | `data_quality_assessment/column_resolver.py` | Finds all payment-terms-type columns, plus any column with "payment" or "term" in the name as a fallback. |
| `find_supplier_columns` | `data_quality_assessment/column_resolver.py` | Finds all supplier/vendor-type columns, plus any column with "vendor" or "supplier" in the name as a fallback. |
| `suggest_columns_ai` | `data_quality_assessment/column_resolver_ai.py` | Sends column names and sample values to the AI and asks it to pick the top 3 most likely columns for each analysis role (date, currency, payment terms, country, vendor). |
| `collect_column_samples` | `data_quality_assessment/service.py` | Reads column names and a few non-null sample values for each column from a table, for use in AI column suggestions. |
| `run_dqa_suggest_columns` | `data_quality_assessment/service.py` | Calls the AI to suggest the best columns for each analysis role, using pre-collected column samples. |
| `dqa_suggest_columns` | `routes/data_quality_routes.py` | Receives the column suggestion request. Reads samples under the session lock, then calls AI outside the lock. |
| `find_column` | `data_quality_assessment/metrics.py` | Given a list of possible column names and the set of columns actually in the table, finds the first match ignoring upper/lowercase differences and extra spaces. Legacy function kept for backward compatibility. |
| `numeric_spend_expr` | `data_quality_assessment/metrics.py` | Creates a database formula that safely converts a spend column to a number, handling commas and spaces in values like "1,234.56". Returns 0 for non-numeric values. |
| `raw_numeric_expr` | `data_quality_assessment/metrics.py` | Similar to numeric_spend_expr but keeps negative numbers and returns blank instead of 0 for non-numeric values. Used for spend bifurcation. |
| `_safe_pct` | `data_quality_assessment/metrics.py` | Calculates a percentage safely, returning zero instead of crashing when the total is zero. |
| `_non_null_condition` | `data_quality_assessment/metrics.py` | Creates a database condition that checks whether a cell has actual content (not blank or null). |
| `run_dqa_financial_ai` | `data_quality_assessment/service.py` | Sends date, currency, and payment terms SQL results to a single AI call for consolidated insights. |
| `run_dqa_entity_ai` | `data_quality_assessment/service.py` | Sends country/region and supplier SQL results to a single AI call for consolidated insights. |
| `run_dqa_all_sql` | `data_quality_assessment/service.py` | Runs all seven panels' SQL queries in a single lock acquisition so the database is accessed just once. |
| `dqa_all` | `routes/data_quality_routes.py` | Receives a request to run all panels at once. Runs all SQL under one lock, then makes 2 consolidated AI calls, and returns everything together. |
| `compute_fill_rates` | `data_quality_assessment/metrics.py` | Calculates what percentage of each column is filled in (vs. empty). |
| `compute_date_metrics` | `data_quality_assessment/metrics.py` | Measures how many dates are valid, parseable, and in a recognisable format. |
| `compute_spend_metrics` | `data_quality_assessment/metrics.py` | Checks whether spend values are proper numbers and flags outliers. |
| `compute_supplier_metrics` | `data_quality_assessment/metrics.py` | Counts unique suppliers and calculates top-supplier concentration. |
| `compute_description_metrics` | `data_quality_assessment/metrics.py` | Measures the quality of text descriptions — length, word count, and whether they contain real information. |
| `compute_non_procurable_spend` | `data_quality_assessment/metrics.py` | Estimates how much spend is tagged to non-procurable or suspicious categories. |
| `compute_alphanumeric_spend` | `data_quality_assessment/metrics.py` | Flags spend values that are stored as text instead of numbers. |
| `compute_currency_metrics` | `data_quality_assessment/metrics.py` | Checks currency code quality — how many distinct codes, consistency across rows. |
| `compute_currency_quality_analysis` | `data_quality_assessment/metrics.py` | A deeper currency check comparing currency codes against actual spend values. |
| `compute_fill_rate_summary` | `data_quality_assessment/metrics.py` | Summarises fill rates across all columns into a quick overview for the dashboard. |
| `run_fill_rate_analysis` | `data_quality_assessment/fill_rate_analysis.py` | Computes per-column fill rate (percentage of non-empty rows) and spend coverage. If reporting currency spend exists, shows a single spend percentage per column. If only local spend, shows per-currency spend percentages. |
| `run_spend_bifurcation` | `data_quality_assessment/fill_rate_analysis.py` | Splits total spend into positive and negative amounts. If reporting currency exists, returns a single pair. If only local spend, groups by currency code. |
| `dqa_fill_rate` | `routes/data_quality_routes.py` | Receives the request for the fill rate summary and returns per-column fill rates with spend coverage. No AI key required. |
| `dqa_spend_bifurcation` | `routes/data_quality_routes.py` | Receives the request for positive vs negative spend bifurcation. No AI key required. |
| `run_dqa_fill_rate` | `data_quality_assessment/service.py` | Validates the table and runs the fill rate analysis. |
| `run_dqa_spend_bifurcation` | `data_quality_assessment/service.py` | Validates the table and runs the spend bifurcation analysis. |

### Pre-Merge Insights & Analysis (runs between Append and Merge)

| Function | File | What it does |
|----------|------|--------------|
| `pre_merge_analysis` | `routes/insights_routes.py` | Receives the request to analyse how well groups would merge before the user runs the actual merge. |
| `group_insights` | `routes/insights_routes.py` | Receives the request to generate AI insights for the data in each append group. |
| `_stats_for_prompt` | `summary/insights/service.py` | Formats column statistics into a clean structure the AI can understand. |
| `_execute_slices` | `summary/insights/service.py` | Runs summary queries (group-by aggregations) on a table for insight generation. |
| `_run_ai_pipeline_for_group` | `summary/insights/service.py` | Runs the full AI analysis chain for one group: profile the data, audit quality, and suggest strategies. |
| `run_insights` | `summary/insights/service.py` | Runs insights for every group plus a cross-group narrative about how they relate. |
| `run_pre_merge_analysis` | `summary/insights/service.py` | Analyses overlap between groups and produces AI recommendations about the best merge strategy. |
| `compute_deep_column_stats` | `summary/insights/stats/column_stats_computer.py` | Calculates detailed stats for each column — top values, patterns, numeric ranges, and data types. |
| `estimate_duplicate_rows` | `summary/insights/stats/column_stats_computer.py` | Estimates how many rows are duplicates across the table. |
| `compute_cross_table_consistency` | `summary/insights/stats/column_stats_computer.py` | Compares columns across tables to see if they contain similar values and structures. |
| `analyze_cross_group_sql` | `summary/insights/stats/column_stats_computer.py` | Measures schema and value overlap between groups to help decide if merging makes sense. |

### Post-Merge Analysis (runs after merge on the final table)

| Function | File | What it does |
|----------|------|--------------|
| `run_analysis` | `summary/analysis/service.py` | Runs a full analysis on the merged table — column stats, AI profiling, and quality auditing. |
| `_enrich_column_stats` | `summary/analysis/service.py` | Adds extra information to column stats, like what percentage of values are actually numeric. |
| `_detect_date_columns` | `summary/analysis/service.py` | Guesses which columns contain dates by looking at their names and sample values. |
| `_detect_currency_columns` | `summary/analysis/service.py` | Guesses which columns contain currency codes by looking at their names. |
| `_detect_spend_columns` | `summary/analysis/service.py` | Guesses which columns contain spend/amount values by looking at their names. |
| `run_procurement_analysis` | `summary/analysis/service.py` | Runs a procurement-specific analysis on the merged data — date ranges, currencies, spend breakdowns, plus AI narrative. |

### Orchestration & Execution Engine

| Function | File | What it does |
|----------|------|--------------|
| `execution_state` | `routes/insights_routes.py` | Returns the current state of the pipeline — which steps are done, what can run next, and what artifacts exist. |
| `execution_run` | `routes/insights_routes.py` | Runs a batch of requested operations (like "run append then run norm") in sequence. |
| `_execute_operation` | `routes/insights_routes.py` | Runs one specific operation in the pipeline (e.g. just the append step or just the merge step). |
| `_build_state_patch` | `routes/insights_routes.py` | Creates a status update showing what changed after an operation ran. |
| `_build_artifact_summary` | `routes/insights_routes.py` | Summarises which output tables and results exist in the session right now. |
| `_operation_is_ready` | `routes/insights_routes.py` | Checks whether a requested operation has all its prerequisites met (required tables exist, API key set, etc.). |
| `_missing_requirements` | `routes/insights_routes.py` | Lists what's missing before an operation can run (e.g. "needs API key" or "no merged table found"). |
| `invalidate_downstream` | `routes/insights_routes.py` | When the user goes back and changes something, clears all the results from later steps that are now outdated. |
| `chat` | `routes/insights_routes.py` | Handles the procurement assistant chat — sends context and the user's question to the AI and streams the reply. |
| `run_chat` | `summary/insights/service.py` | Runs the AI chat completion with session context and returns the response (streaming or all at once). |

### Shared / Utility

| Function | File | What it does |
|----------|------|--------------|
| `create_app` | `app.py` | Creates and configures the Flask web application — sets up all routes, middleware, and background tasks. |
| `health` | `app.py` | A simple endpoint that returns "ok" to confirm the server is running. Used by the launcher to verify the service is fully ready before opening the browser. |
| `_nan_to_none` | `app.py` | Cleans up data for JSON responses by replacing special "not a number" values with proper empty values. |
| `_session_cleanup_loop` | `app.py` | A background process that periodically deletes old session files that haven't been used recently. |
| `chunk_list` | `shared/utils/helpers.py` | Splits a list into smaller batches of a given size (used when processing data in chunks). |
| `resolve_input_table` | `shared/utils/helpers.py` | Figures out which actual database table a user's table reference points to. |
| `validate_step_inputs` | `shared/utils/helpers.py` | Checks that all the tables a step needs are present before letting it run. |
| `json_safe` | `shared/utils/helpers.py` | Makes any Python data structure safe to convert to JSON (handles dates, sets, and other tricky types). |
| `make_unique` | `shared/utils/helpers.py` | Ensures all names in a list are unique by adding suffixes to duplicates. |
| `find_column` | `shared/utils/helpers.py` | Finds a column in a list by name, ignoring case and extra spaces. |
| `get_session_db` | `shared/db/session_db.py` | Opens (or retrieves from cache) the database connection for a user's session. |
| `close_session_db` | `shared/db/session_db.py` | Closes the database connection for a session and removes it from cache. |
| `delete_session_db` | `shared/db/session_db.py` | Deletes the session's database file from disk entirely. |
| `safe_table_name` | `shared/db/session_db.py` | Creates a safe database table name from user-provided text (removes special characters). |
| `register_table` | `shared/db/session_db.py` | Records that a logical table name maps to a specific database table. |
| `unregister_table` | `shared/db/session_db.py` | Removes the mapping for a logical table name. |
| `lookup_sql_name` | `shared/db/session_db.py` | Looks up which database table a logical name points to. |
| `all_registered_tables` | `shared/db/session_db.py` | Lists every table registered in the current session. |
| `cleanup_stale_sessions` | `shared/db/session_db.py` | Deletes session files that are older than a certain time limit. |
| `quote_id` | `shared/db/table_ops.py` | Wraps a table or column name in quotes so it's safe to use in database queries. |
| `store_table` | `shared/db/table_ops.py` | Saves a batch of rows into a database table in one transaction. |
| `store_table_streaming` | `shared/db/table_ops.py` | Saves rows into a database table in smaller chunks, for very large datasets that won't fit in memory at once. |
| `store_df_native` | `shared/db/table_ops.py` | Saves a pandas DataFrame into a database table using DuckDB's zero-copy path — no Python row iteration, orders of magnitude faster for large datasets. |
| `read_table` | `shared/db/table_ops.py` | Reads rows from a database table and returns them as a list of row-objects. |
| `read_table_columns` | `shared/db/table_ops.py` | Returns the ordered list of column names for a table. |
| `table_exists` | `shared/db/table_ops.py` | Checks whether a specific table exists in the database. |
| `drop_table` | `shared/db/table_ops.py` | Deletes a table from the database if it exists. |
| `table_row_count` | `shared/db/table_ops.py` | Returns how many rows a table has. |
| `iterate_table` | `shared/db/table_ops.py` | Reads rows from a table one at a time (memory-efficient for very large tables). |
| `column_stats` | `shared/db/stats_ops.py` | Calculates basic stats for each column — how many nulls, how many unique values, fill rate. |
| `column_distinct_values` | `shared/db/stats_ops.py` | Returns the most common unique values in a column. |
| `column_distinct_count` | `shared/db/stats_ops.py` | Returns how many unique values a column has. |
| `column_null_count` | `shared/db/stats_ops.py` | Returns how many empty/null values a column has. |
| `compute_overlap` | `shared/db/stats_ops.py` | Measures how many values two columns share in common (useful for join planning). |
| `get_meta` | `shared/db/meta_ops.py` | Reads a stored setting or piece of metadata from the session. |
| `set_meta` | `shared/db/meta_ops.py` | Saves a setting or piece of metadata to the session. |
| `delete_meta` | `shared/db/meta_ops.py` | Removes a stored setting from the session. |
| `get_all_meta_keys` | `shared/db/meta_ops.py` | Lists all the settings currently stored in the session. |
| `duckdb_connect` | `shared/db/duckdb_compat.py` | Opens a DuckDB database file and returns a connection that works just like the old database connections. |
| `DuckDBConnection` | `shared/db/duckdb_compat.py` | A wrapper around DuckDB that makes it behave the same way as the old database — so all existing code works without changes. |
| `DictRow` | `shared/db/duckdb_compat.py` | Makes database rows work like dictionaries — you can access values by column name (e.g. row["name"]). |
| `DuckCursorWrapper` | `shared/db/duckdb_compat.py` | Wraps query results so they return dictionary-style rows and support familiar methods like fetchone() and fetchall(). |
| `normalize_for_match` | `shared/db/table_ops.py` | Creates a formula that cleans up values for matching — trims spaces, ignores case, and normalises numbers so "123" and "123.0" match. |
| `get_client` | `shared/ai/client.py` | Creates the AI client connection using the user's API key. |
| `get_model` | `shared/ai/client.py` | Returns the name of the AI model being used. |
| `call_ai_json` | `shared/ai/client.py` | Sends a prompt to the AI and gets back a structured JSON answer (with caching and retries). |
| `call_ai_json_validated` | `shared/ai/client.py` | Same as above, but also checks that the AI's answer matches the expected format. |
| `batch_ai_mapping` | `shared/ai/batch_runner.py` | Runs many AI mapping requests in parallel batches for speed. |
| `reframe_procurement` | `ai-core/procurement_reframer.py` | Asks the AI to reword text using proper procurement terminology. |

---

## Module 2 — Normalizer (`ProcIP_Module2-main/backend/`)

### Upload Data

| Function | File | What it does |
|----------|------|--------------|
| `upload_file` | `app.py` | Receives the uploaded file (ZIP, Excel, or CSV), reads all sheets, and stores them in the session's SQLite database. |
| `import_from_stitcher` | `app.py` | Receives data sent from the DataStitcher module, loads it as a single table, and stores it in a new session database. |

### Data Preview

| Function | File | What it does |
|----------|------|--------------|
| `get_raw_preview` | `app.py` | Returns the raw grid of a table (before headers are chosen) so the user can pick the correct header row. |
| `get_preview` | `app.py` | Returns the column names and first 50 rows of a table for the preview screen. |
| `get_current_preview` | `app.py` | Returns a preview of the currently selected table that's being worked on. |
| `suggest_columns` | `app.py` | Uses rules to suggest which columns are most likely the date, currency, and spend columns. |
| `_suggest_columns` | `app.py` | The internal logic that examines column names and sample values to guess date, currency, and spend columns. |
| `set_header_row` | `app.py` | Changes which row is used as the header, applies any custom renames, and rebuilds the table. |
| `delete_rows` | `app.py` | Removes selected rows from a table. |
| `delete_table` | `app.py` | Removes a table from the session entirely. |
| `select_table` | `app.py` | Locks in one table as the active working table and prepares it for normalization. |
| `current_inventory` | `app.py` | Returns the list of all tables in the session with their row and column counts. |
| `pick_best_rows` | `db/bridge.py` | Takes a list of row dicts and returns the ones with the most filled-in columns, so previews show the most informative data. |
| `pick_best_df_rows` | `db/bridge.py` | Takes a DataFrame and returns the rows with the most filled-in columns, so previews show the most informative data. |

### Normalization — Supplier Names

| Function | File | What it does |
|----------|------|--------------|
| `normalize_supplier_name_agent` | `agents/normalization.py` | Cleans up supplier names — removes legal suffixes (Inc, Ltd), trims spaces, and merges near-duplicate names using fuzzy matching. Adds a new "Normalised Supplier Name" column. |
| `_clean_supplier_name` | `agents/normalization.py` | Strips legal suffixes, website domains, and extra spaces from a single supplier name. |
| `_fuzzy_dedup` | `agents/normalization.py` | Finds supplier names that are almost identical and merges them into one canonical name. |

### Normalization — Supplier Country

| Function | File | What it does |
|----------|------|--------------|
| `normalize_supplier_country_agent` | `agents/normalization.py` | Standardises country names using a built-in lookup, with AI as a fallback for ambiguous values. Adds a normalised country column and tracks which method was used for each value. |
| `_lookup_country` | `agents/normalization.py` | Tries to match a country abbreviation or alias to its full English name using a built-in dictionary. |
| `assess_supplier_country` | `agents/normalization.py` | Checks how well-populated a country column is and flags potential issues before normalisation starts. |
| `assess_supplier_country_api` | `app.py` | Receives the "Assess" button request from the browser and runs the country assessment. |

### Normalization — Dates

| Function | File | What it does |
|----------|------|--------------|
| `date_normalization_agent` | `agents/normalization.py` | Finds all date-like columns and converts them to the user's chosen format. Handles Excel serial numbers, partial dates, and various international formats. Adds normalised date columns next to the originals. |
| `_excel_serial` | `agents/normalization.py` | Converts an Excel serial number (like 45678) into a proper date. |
| `_date_preprocess` | `agents/normalization.py` | Cleans up a raw date string — removes time parts, ordinal suffixes ("1st"), and standardises separators. |
| `_parse_partial_date` | `agents/normalization.py` | Handles dates that are incomplete, like just a year ("2024") or month-year ("Jan 2024"). |
| `_try_date_masks` | `agents/normalization.py` | Tries a list of known date formats one by one until one works for a given date string. |
| `_profile_date_series` | `agents/normalization.py` | Looks at a sample of dates to guess whether the data uses day-first (13/04/2025) or month-first (04/13/2025) format. |
| `_parse_one_date` | `agents/normalization.py` | The full date parser for a single value — tries every method (serial numbers, known formats, partial dates, pandas fallback). |
| `_normalize_date_series` | `agents/normalization.py` | Converts an entire column of dates to the target format and reports what date ordering was detected. |
| `_detect_file_column` | `agents/normalization.py` | Finds the column that contains source file names (used to group dates by file when formats differ per file). |
| `_diverse_date_samples` | `agents/normalization.py` | Picks a diverse set of date strings to send to the AI for format detection. |

### Normalization — Payment Terms

| Function | File | What it does |
|----------|------|--------------|
| `payment_terms_agent` | `agents/normalization.py` | Standardises payment terms: first asks the AI to produce clean English descriptions, then uses pattern matching to extract number of days, discount percentage, and a doubt flag. Adds new columns for each. |
| `_parse_payment_term` | `agents/normalization.py` | Uses pattern matching to pull out the number of days, discount rate, and doubt indicator from a payment term string. |

### Normalization — Regions

| Function | File | What it does |
|----------|------|--------------|
| `normalize_region_agent` | `agents/normalization.py` | Assigns each row a standard region (NA, EMEA, APAC, or LATAM) based on country, using built-in rules first and AI for edge cases. Adds a normalised region column. |
| `_lookup_region` | `agents/normalization.py` | Maps a standard country name to its region code using a built-in country-to-region dictionary. |
| `assess_region` | `agents/normalization.py` | Checks how well-populated a region or country column is before running normalisation. |
| `assess_region_api` | `app.py` | Receives the "Assess" button request from the browser and runs the region assessment. |

### Normalization — Plant / Site

| Function | File | What it does |
|----------|------|--------------|
| `normalize_plant_agent` | `agents/normalization.py` | Cleans up all plant, site, and location columns using rules and AI. Adds normalised columns next to the originals. |

### Normalization — Currency Conversion

| Function | File | What it does |
|----------|------|--------------|
| `normalize_spend_agent` | `agents/normalization.py` | Loops through each spend column, loads the exchange rate table, converts values to USD, and reports success metrics. |
| `_coerce_spend_columns` | `agents/normalization.py` | Tidies up the list of spend column names the user provided, removing blanks and duplicates. |
| `assess_currency_conversion` | `agents/normalization.py` | Pre-checks before running conversion — verifies the FX table can be loaded, flags unsupported currencies, and shows column population stats. |
| `assess_currency_conversion_api` | `app.py` | Receives the "Assess" button request from the browser and runs the currency conversion assessment. |
| `load_fx_table` | `agents/fx_rates.py` | Loads exchange rates from a built-in dictionary (the default) or from an Excel file if you pass a file path. Builds lookup tables for monthly and yearly rates, and caches the result so it only runs once. |
| `_build_derived_structures` | `agents/fx_rates.py` | Takes the raw rate entries and the list of currencies, then figures out the newest available rate for each currency, calculates yearly averages, and counts how many months each average is based on. |
| `_load_from_excel` | `agents/fx_rates.py` | Reads an FX rate Excel workbook, finds the header row, and extracts all the monthly rates into a dictionary. Used as a fallback when someone provides an explicit file path to load_fx_table. |
| `run_conversion` | `agents/fx_rates.py` | Converts every spend value in a column to a chosen target currency using the matching exchange rate. Supports all currencies in the FX table. For non-USD targets, bridges through USD (source -> USD -> target). Reports detailed per-row results. |
| `resolve_fx_reference_path` | `agents/fx_rates.py` | Finds the exchange rate Excel file — checks an explicit path, environment variable, and standard locations. Used only when an explicit override path is provided. |
| `supported_currencies_api` | `app.py` | Returns the list of all currency codes available in the FX rates table, so the frontend can populate the target currency dropdown. |

### Normalization — Other

| Function | File | What it does |
|----------|------|--------------|
| `add_record_id_agent` | `agents/normalization.py` | Adds a sequential "Record ID" column (1, 2, 3, ...) if the table doesn't already have one. |
| `_upsert_adjacent_column` | `agents/normalization.py` | Inserts or replaces a new column right next to a source column in the table. |
| `_upsert_adjacent_columns` | `agents/normalization.py` | Same as above but for multiple new columns at once, keeping them in order. |

### Shared / Utility

| Function | File | What it does |
|----------|------|--------------|
| `_nan_to_none` | `app.py` | Cleans up data for JSON responses by replacing "not a number" values with proper empty values. |
| `_get_session_id` | `app.py` | Reads the session ID from the request (JSON body, form data, query param, or header). |
| `_clean_preview_rows` | `app.py` | Replaces NaN-like string values with empty strings for display in the browser. |
| `get_api_key` | `app.py` | Reads the API key from the request body or from an environment variable. |
| `health_check` | `app.py` | A simple endpoint (available at both /api/status and /api/health) that returns "ok" to confirm the server is running. Used by the launcher to verify the service is fully ready before opening the browser. |
| `run_normalization` | `app.py` | Dispatches the correct normalisation agent based on which operation the user selected, runs it, and returns results. |
| `transfer_to_analyzer` | `app.py` | Sends the normalised data to the Summarizer module (Module 3). |
| `reset_normalization` | `app.py` | Reloads the original data from the session database, discarding all normalisation changes. |
| `reset_state` | `app.py` | Deletes the entire session database for a completely fresh start. |
| `download` | `app.py` | Sends the normalised dataset to the browser as a CSV download. |
| `get_session_db` | `db/session_db.py` | Opens (or retrieves from cache) the SQLite database connection for a session. |
| `get_session_lock` | `db/session_db.py` | Returns a per-session lock that route handlers grab before touching the database, so two requests for the same session don't collide. |
| `close_session_db` | `db/session_db.py` | Closes the database connection for a session and removes it from cache. |
| `delete_session_db` | `db/session_db.py` | Deletes the session's database file and its lock from memory. |
| `safe_table_name` | `db/session_db.py` | Creates a safe database table name from user-provided text. |
| `register_table` | `db/session_db.py` | Records that a logical table name maps to a specific database table. |
| `unregister_table` | `db/session_db.py` | Removes the mapping for a logical table name. |
| `lookup_sql_name` | `db/session_db.py` | Looks up which database table a logical name points to. |
| `all_registered_tables` | `db/session_db.py` | Lists every table registered in the current session. |
| `cleanup_stale_sessions` | `db/session_db.py` | Deletes session files that are older than a certain time limit. |
| `cleanup_all_sessions` | `db/session_db.py` | Closes all connections and deletes all session database files. |
| `store_table` | `db/table_ops.py` | Saves a batch of rows into a database table in one go. |
| `store_table_streaming` | `db/table_ops.py` | Saves rows into a database table in smaller chunks, for large datasets. |
| `store_df_native` | `db/table_ops.py` | Saves a pandas DataFrame into a database table using DuckDB's zero-copy path — no Python row iteration. |
| `read_table` | `db/table_ops.py` | Reads rows from a database table and returns them as a list of row-objects. |
| `read_table_columns` | `db/table_ops.py` | Returns the ordered list of column names for a table. |
| `table_exists` | `db/table_ops.py` | Checks whether a specific table exists in the database. |
| `drop_table` | `db/table_ops.py` | Deletes a table from the database if it exists. |
| `table_row_count` | `db/table_ops.py` | Returns how many rows a table has. |
| `iterate_table` | `db/table_ops.py` | Reads rows from a table one at a time for memory efficiency. |
| `get_meta` | `db/meta_ops.py` | Reads a stored setting or piece of metadata from the session. |
| `set_meta` | `db/meta_ops.py` | Saves a setting or piece of metadata to the session. |
| `delete_meta` | `db/meta_ops.py` | Removes a stored setting from the session. |
| `sqlite_to_df` | `db/bridge.py` | Loads a SQLite table into a pandas DataFrame for agent processing. |
| `df_to_sqlite` | `db/bridge.py` | Saves a pandas DataFrame to a DuckDB table using native zero-copy ingestion. All values are cast to text. |
| `get_client` | `agents/helpers.py` | Creates the Portkey AI client connection using the API key and server settings (same pattern as Modules 1 and 3). |
| `get_model` | `agents/helpers.py` | Returns the Portkey model name to use for AI calls. |
| `_batch_ai_mapping` | `agents/helpers.py` | Runs many AI mapping requests in parallel batches for speed. |
| `_find_column` | `agents/helpers.py` | Searches column headers by keyword to find a specific column, with optional AI help for tricky cases. |
| `identify_header_row` | `agents/helpers.py` | Scans the first few rows to guess which one is the actual header (by checking which row has the most non-empty cells). |
| `make_unique` | `agents/helpers.py` | Ensures all column names are unique by adding suffixes to duplicates. |

---

## Module 3 — Spend Summarizer (`SummarizationModule/backend/`)

### Upload

| Function | File | What it does |
|----------|------|--------------|
| `upload` | `routes/upload_routes.py` | Receives uploaded files, creates a new session, and loads all tables into the database. Returns inventory and column info (previews are loaded lazily). |
| `get_preview` | `routes/upload_routes.py` | Returns the first 50 rows of a single table as a preview. Called on-demand when the user expands a table card. |
| `import_from_module` | `routes/upload_routes.py` | Receives data sent from the DataStitcher or Normalizer, creates a session, and loads it. |
| `delete_table` | `routes/upload_routes.py` | Removes a table from the session and refreshes the inventory and column metadata. |
| `_safe_sql_name` | `services/upload/file_loader.py` | Cleans up a name so it's safe to use as a database table or column identifier. |
| `_clean_header` | `services/upload/file_loader.py` | Tidies up a single header cell — trims spaces, removes odd characters. |
| `_dedupe_headers` | `services/upload/file_loader.py` | Makes sure all column names are unique by adding suffixes when duplicates appear. |
| `_ensure_registry` | `services/upload/file_loader.py` | Creates the internal tracking table that keeps a record of all uploaded data tables. |
| `_register_table` | `services/upload/file_loader.py` | Adds a new entry to the tracking table for a freshly loaded data table. |
| `_get_registry` | `services/upload/file_loader.py` | Reads all entries from the tracking table to see which tables are in the session. |
| `_unregister_table` | `services/upload/file_loader.py` | Removes a table's entry from the tracking table (used when deleting a table). |
| `_parse_csv_bytes` | `services/upload/file_loader.py` | Reads raw CSV file contents into a DataFrame and extracts headers. Returns a DataFrame for native DuckDB ingestion. |
| `_parse_excel_bytes` | `services/upload/file_loader.py` | Reads raw Excel file contents and returns DataFrames per sheet (not Python grids) for native DuckDB ingestion. |
| `_store_df_native` | `services/upload/file_loader.py` | Saves a pandas DataFrame into a database table using DuckDB's zero-copy path — no Python row iteration. |
| `_store_raw_table` | `services/upload/file_loader.py` | Saves the raw DataFrame into a database table using native ingestion (kept as a backup for re-picking headers). |
| `_store_data_table_from_raw` | `services/upload/file_loader.py` | Builds the working data table from the raw table via SQL, skipping the header row and empty rows — no Python iteration. |
| `_build_table_key` | `services/upload/file_loader.py` | Creates a stable identifier for a file-and-sheet combination (e.g. "Sales.xlsx__Sheet1"). |
| `_set_bulk_pragmas` | `services/upload/file_loader.py` | Speeds up the database during large bulk loads by temporarily relaxing safety settings. |
| `_restore_pragmas` | `services/upload/file_loader.py` | Restores normal database safety settings after a bulk load finishes. |
| `load_zip_to_session` | `services/upload/file_loader.py` | Opens a ZIP file, finds all spreadsheets and CSVs inside, and loads each one into the session database. |
| `load_single_file` | `services/upload/file_loader.py` | Loads one CSV or Excel file into the session database. |
| `delete_table_from_session` | `services/upload/file_loader.py` | Drops a table's raw and data copies from the database and removes its tracking entry. |

### Data Preview

| Function | File | What it does |
|----------|------|--------------|
| `raw_preview` | `routes/upload_routes.py` | Returns the raw grid (before headers) for a table so the user can choose the right header row. |
| `set_header_row` | `routes/upload_routes.py` | Changes which row is the header, rebuilds the table, and refreshes all related metadata. |
| `delete_rows` | `routes/upload_routes.py` | Deletes selected rows from a table and returns an updated preview. |
| `_safe_value` | `services/upload/file_loader.py` | Cleans up a cell value so it's safe for JSON (e.g. replaces NaN with null). |
| `build_preview` | `services/upload/file_loader.py` | Builds a limited-row preview of every table in the session for the UI. |
| `build_single_preview` | `services/upload/file_loader.py` | Builds a preview (first 50 rows) for one specific table. Used for lazy-loading previews when the user expands a table. |
| `get_raw_preview` | `services/upload/file_loader.py` | Reads a window of raw rows from the backup table for the header-selection screen. |
| `set_header_row_for_table` | `services/upload/file_loader.py` | Rebuilds a table from the raw table using SQL (only fetches the header row into Python). Skips the header row and empty rows entirely in SQL. |
| `delete_rows_from_table` | `services/upload/file_loader.py` | Deletes rows from a table by their row IDs and returns how many were removed. |
| `collect_column_info` | `services/upload/file_loader.py` | Gathers column metadata (names, types, sample values) across all tables for the mapping step. |
| `build_inventory` | `services/upload/file_loader.py` | Returns the list of all tables with their keys, row counts, and sheet names. |
| `pick_best_rows` | `services/upload/file_loader.py` | Takes a list of row dicts and returns the ones with the most filled-in columns, so previews show the most informative data. |
| `pick_best_raw_rows` | `services/upload/file_loader.py` | Same as pick_best_rows but works on raw list-of-lists data (used for the raw grid preview). |

### Map Columns

| Function | File | What it does |
|----------|------|--------------|
| `map_columns` | `routes/mapping_routes.py` | Receives the "Detect Columns" request, runs deterministic matching then a single-batch AI call for unmatched fields, and returns suggestions. |
| `confirm_mapping` | `routes/mapping_routes.py` | Saves the user's confirmed field-to-column mapping, builds the typed analysis table, precomputes feasibility, and returns a cast report. |
| `deterministic_match` | `services/mapping/column_mapper.py` | Matches upload columns to the 32 standard procurement fields using exact name and alias comparison (case-insensitive). |
| `ai_map_columns` | `services/mapping/column_mapper.py` | Sends all unmatched fields and unmatched columns to the AI in a single batch call, validates type compatibility, resolves duplicate claims, and returns mapping results. |
| `_cast_numeric` | `services/mapping/column_mapper.py` | Converts a column to numbers, stripping currency symbols and commas. Reports which values couldn't be parsed. |
| `_cast_datetime` | `services/mapping/column_mapper.py` | Converts a column to dates using the date_parser module. Reports which values couldn't be parsed. |
| `_cast_string` | `services/mapping/column_mapper.py` | Cleans a column to trimmed strings, replacing null-like values with empty strings. |
| `build_typed_table` | `services/mapping/column_mapper.py` | Creates the unified "analysis_data" table by pulling mapped columns from all source tables, converting types, and reporting what worked. |
| `_excel_serial` | `services/mapping/date_parser.py` | Converts an Excel serial number into a proper date. |
| `_date_preprocess` | `services/mapping/date_parser.py` | Cleans up a raw date string before trying to parse it. |
| `_parse_partial_date` | `services/mapping/date_parser.py` | Handles incomplete dates like just a year or month-year. |
| `_try_date_masks` | `services/mapping/date_parser.py` | Tries known date formats one by one until one works. |
| `_profile_date_series` | `services/mapping/date_parser.py` | Examines sample dates to guess if the data uses day-first or month-first ordering. |
| `_parse_one_date` | `services/mapping/date_parser.py` | Full date parser for one value — tries every method to turn it into a proper date. |
| `parse_date_column` | `services/mapping/date_parser.py` | Profiles a column for date order (DMY/MDY) and parses every value to a datetime. |

### Spend Quality Assessment

| Function | File | What it does |
|----------|------|--------------|
| `executive_summary` | `routes/views_routes.py` | Receives the request to run the spend quality assessment and returns the cached or freshly computed results. |
| `_es_lock` | `routes/views_routes.py` | Prevents two quality assessments from running at the same time for the same session. |
| `_quote_id` | `services/spend_quality_assessment/data_quality.py` | Wraps a column or table name in quotes for safe use in database queries. |
| `_nn` | `services/spend_quality_assessment/data_quality.py` | Creates a database condition that checks whether a cell has real content (not blank or null). |
| `_compute_date_spend_pivot` | `services/spend_quality_assessment/data_quality.py` | Builds a year-by-month pivot table showing total spend per period. Rows with non-numeric spend are excluded (not counted as zero). |
| `_compute_spend_bifurcation` | `services/spend_quality_assessment/data_quality.py` | Splits spend into positive and negative totals. Returns both a reporting currency view (using total_spend) and a local currency view (using local_spend grouped by currency). Supports a frontend toggle between the two views. |
| `_compute_pareto_analysis` | `services/spend_quality_assessment/data_quality.py` | Calculates supplier concentration — groups all invoices by supplier, sums their spend, ranks suppliers from biggest to smallest, then walks down the list to find how many suppliers make up 80%, 85%, 90%, 95%, and 99% of total positive spend. For each threshold, also counts the total invoice rows and unique transaction types belonging to those suppliers. |
| `run_executive_summary` | `services/spend_quality_assessment/data_quality.py` | Convenience wrapper that runs both SQL and AI phases sequentially. Prefer calling the split functions so the AI phase runs outside the lock. |
| `run_executive_summary_sql` | `services/spend_quality_assessment/data_quality.py` | Runs the SQL-only phase: date-spend pivot, spend bifurcation, Pareto analysis, and description column stats (no AI). Must be called under the session lock. |
| `run_executive_summary_ai` | `services/spend_quality_assessment/data_quality.py` | Runs the AI phase: generates a categorization method recommendation from pre-computed SQL data. Safe to call without any lock. |
| `_compute_date_period` | `services/spend_quality_assessment/data_quality.py` | Finds the earliest and latest invoice date in the dataset, formats a period label (e.g. "Jan 2024 – Dec 2025"), and counts how many distinct months the data spans. |
| `_compute_spend_breakdown` | `services/spend_quality_assessment/data_quality.py` | Computes last-twelve-months spend, current and prior fiscal year spend, and the year-over-year change (both absolute and percentage). |
| `_compute_supplier_breakdown` | `services/spend_quality_assessment/data_quality.py` | Counts total suppliers, finds how many cover 80% of spend, lists the top 10 by share, and flags suppliers that look like duplicates (same name with different casing). |
| `_compute_categorization_effort` | `services/spend_quality_assessment/data_quality.py` | Measures description quality (word count, character length, fill rate, unique values) and estimates how much it would cost to run AI-based categorization on the dataset. |
| `_compute_flags` | `services/spend_quality_assessment/data_quality.py` | Checks the dataset for quality issues: months with abnormal spend, poor description fill rate or word count, low supplier fill rate, and columns with significant gaps or missing spend coverage. |
| `_compute_column_fill_rate` | `services/spend_quality_assessment/data_quality.py` | For every mapped column, calculates what percentage of rows are filled and what percentage of total spend sits in populated rows. |
| `_quote_id` | `services/spend_quality_assessment/description_quality.py` | Same as above — wraps a name in quotes for safe database use. |
| `_nn` | `services/spend_quality_assessment/description_quality.py` | Same as above — checks if a cell has real content. |
| `_build_null_proxy_sql` | `services/spend_quality_assessment/description_quality.py` | Creates conditions to catch placeholder descriptions that look filled in but are actually meaningless (e.g. "N/A", "TBD", "---"). |
| `_compute_description_column_stats` | `services/spend_quality_assessment/description_quality.py` | Calculates per-column stats: how much spend is covered, top 10 most common values, average length, word count, and null-proxy rates. |
| `_sample_descriptions_for_ai` | `services/spend_quality_assessment/description_quality.py` | Picks a sample of the most impactful descriptions (covering ~80% of spend) to send to the AI for quality assessment. |
| `_top_descriptions_by_frequency` | `services/spend_quality_assessment/description_quality.py` | Returns the most common description values along with how often they appear and how much spend they cover. |
| `_normalise_insight` | `services/spend_quality_assessment/description_quality.py` | Makes sure the AI insight output is always a list of up to 3 short strings, handling both old single-string and new array formats. |
| `_generate_description_insight` | `services/spend_quality_assessment/description_quality.py` | Asks the AI to assess description quality and returns exactly 3 concise bullet points (verdict, key issue, categorisation suitability). |
| `_generate_categorization_recommendation` | `services/spend_quality_assessment/description_quality.py` | Asks the AI to recommend a categorization method (MapAI vs Creactives) based on description metrics, and returns quality buckets plus a verdict. |
| `run_description_quality_analysis` | `services/spend_quality_assessment/description_quality.py` | Runs the full description quality check. When api_key is None, runs SQL only and leaves AI insight as None for the caller to fill in later. |
| `get_searchable_columns` | `services/spend_quality_assessment/not_procurable.py` | Returns the list of text columns in the analysis data that can be searched for keywords (e.g. Description, PO Material Description, L1–L4, Vendor Name). Only returns columns that actually exist in the session. |
| `search_keyword_spend` | `services/spend_quality_assessment/not_procurable.py` | Searches for a keyword across one or more selected columns using case-insensitive matching, and returns how many rows matched and the total spend (reporting currency) for those rows. |
| `_quote_id` | `services/spend_quality_assessment/not_procurable.py` | Wraps a column name in double quotes for safe use in database queries. |
| `not_procurable_columns` | `routes/views_routes.py` | Receives the request to list which text columns are available for keyword-based spend search and returns them. |
| `not_procurable_search` | `routes/views_routes.py` | Receives a keyword and list of columns, searches for matching rows, and returns the row count and total spend. |
| `get_vendor_searchable_columns` | `services/spend_quality_assessment/intercompany.py` | Returns the list of vendor-related columns in the analysis data that can be searched for intercompany keywords. Only returns columns that actually exist in the session. |
| `search_intercompany_keyword` | `services/spend_quality_assessment/intercompany.py` | Searches for a keyword across one or more selected vendor columns using case-insensitive matching, and returns how many rows matched and the total spend for those rows. |
| `_quote_id` | `services/spend_quality_assessment/intercompany.py` | Wraps a column name in double quotes for safe use in database queries. |
| `intercompany_columns` | `routes/views_routes.py` | Receives the request to list which vendor columns are available for intercompany keyword-based spend search and returns them. |
| `intercompany_search` | `routes/views_routes.py` | Receives a keyword and list of vendor columns, searches for matching rows, and returns the row count and total spend. |

### Analysis Feasibility

| Function | File | What it does |
|----------|------|--------------|
| `procurement_views` | `routes/mapping_routes.py` | Returns analysis feasibility results — which Spend X-ray dashboards and Category Navigator levers the data can support based on which columns were mapped. |
| `get_procurement_view_availability` | `services/procurement_views/procurement_views.py` | Checks both the Spend X-ray registry (24 dashboards) and Category Navigator registry (14 levers) against the current mapping and reports which are feasible and which fields are missing. |
| `_check_registry` | `services/procurement_views/procurement_views.py` | Checks every entry in one registry against the set of mapped field keys and returns feasibility with missing fields listed. |

### Select Views

| Function | File | What it does |
|----------|------|--------------|
| `available_views` | `routes/views_routes.py` | Returns the list of all chart/analysis views the user can pick from, based on which columns were mapped. |
| `compute` | `routes/views_routes.py` | Computes all the views the user selected and stores the results. |
| `recompute_view` | `routes/views_routes.py` | Re-runs one specific view with updated settings (e.g. different top-N or time period) and merges the result back. |
| `get_available_views` | `services/views/view_engine.py` | Filters the full list of possible views to only those whose required columns exist in the mapping. |
| `_load_analysis_df` | `services/views/view_engine.py` | Loads the analysis data from the database into a table in memory for chart calculations. |
| `_to_records` | `services/views/view_engine.py` | Converts an in-memory table into a list of row-objects for JSON output. |
| `compute_spend_over_time` | `services/views/view_engine.py` | Adds up spend by time period (month/quarter/year) for the spend-over-time chart. |
| `compute_supplier_ranking` | `services/views/view_engine.py` | Ranks suppliers by total spend and returns the top N with a summary table. |
| `compute_pareto` | `services/views/view_engine.py` | Builds the Pareto/concentration chart showing cumulative spend share across suppliers. |
| `compute_currency_spend` | `services/views/view_engine.py` | Breaks down total spend by currency for the currency chart. |
| `compute_country_spend` | `services/views/view_engine.py` | Breaks down total spend by country for the geography chart. |
| `compute_l1_spend` | `services/views/view_engine.py` | Adds up spend at the top-level (L1) category. |
| `_build_mekko_data` | `services/views/view_engine.py` | Shapes two-level category data into the format needed for Mekko (marimekko) charts. |
| `compute_l1_vs_l2_mekko` | `services/views/view_engine.py` | Builds the L1 vs L2 category breakdown for the Mekko chart. |
| `compute_l2_vs_l3_mekko` | `services/views/view_engine.py` | Builds the L2 vs L3 category breakdown for the Mekko chart. |
| `compute_category_drilldown` | `services/views/view_engine.py` | Builds a hierarchical tree of L1 > L2 > L3 categories with spend at each level. |
| `_extract_spend_over_time_metrics` | `services/views/view_engine.py` | Pulls out key numbers from the spend-over-time chart (total spend, average per period, etc.) for use in summaries. |
| `_extract_currency_spend_metrics` | `services/views/view_engine.py` | Pulls out key numbers from the currency chart for use in summaries. |
| `_extract_country_spend_metrics` | `services/views/view_engine.py` | Pulls out key numbers from the country chart for use in summaries. |
| `_extract_supplier_ranking_metrics` | `services/views/view_engine.py` | Pulls out key numbers from the supplier ranking for use in summaries. |
| `_extract_pareto_metrics` | `services/views/view_engine.py` | Pulls out key numbers from the Pareto analysis for use in summaries. |
| `_extract_l1_spend_metrics` | `services/views/view_engine.py` | Pulls out key numbers from the L1 spend chart for use in summaries. |
| `_extract_l1_vs_l2_metrics` | `services/views/view_engine.py` | Pulls out key numbers from the L1 vs L2 Mekko chart for use in summaries. |
| `_extract_l2_vs_l3_metrics` | `services/views/view_engine.py` | Pulls out key numbers from the L2 vs L3 Mekko chart for use in summaries. |
| `compute_views` | `services/views/view_engine.py` | The main dispatcher — loads the data once, then runs each selected view's calculation and metric extraction. |
| `_safe_float` | `services/views/view_engine.py` | Safely converts a value to a number, returning empty instead of crashing for bad input. |
| `_safe_list` | `services/views/view_engine.py` | Cleans up a list by removing null or invalid entries. |

### Dashboard

| Function | File | What it does |
|----------|------|--------------|
| `generate_summary` | `routes/views_routes.py` | Receives the request to generate an AI summary for one view and attaches it to the stored results. |
| `_extract_first_50_rows` | `services/dashboard/ai_summary.py` | Pulls up to 50 rows of table data from a view result to include in the AI prompt. |
| `generate_summary_for_view` | `services/dashboard/ai_summary.py` | Builds the AI prompt (title, metrics, sample data) and returns a markdown summary for one view. |
| `generate_summaries` | `services/dashboard/ai_summary.py` | Loops through all view results and generates an AI summary for each one. |

### Email

| Function | File | What it does |
|----------|------|--------------|
| `gen_email` | `routes/email_routes.py` | Receives the request to generate the client email, runs AI generation, and falls back to a template if AI fails. |
| `_assemble_email_json` | `services/email/email_generator.py` | Gathers all the metrics from computed views into a structured package the AI can use to write the email. |
| `generate_email` | `services/email/email_generator.py` | Sends the assembled data to the AI and gets back a subject line and email body. |
| `build_fallback_email` | `services/email/email_generator.py` | Creates a template-based email without AI — fills in numbers and context into a pre-written consulting-style format. |
| `generate_csv` | `services/email/export_service.py` | Converts a view's table data into CSV text for download. |

### Shared / Utility

| Function | File | What it does |
|----------|------|--------------|
| `_nan_to_none` | `app.py` | Cleans up data for JSON by replacing "not a number" values with proper empty values. |
| `health` | `app.py` | A simple endpoint that returns "ok" to confirm the server is running. |
| `cleanup_session` | `app.py` | Deletes a session's database file when the user closes their browser tab. |
| `test_key` | `app.py` | Verifies that an API key works by making a tiny test call to the AI. |
| `get_session_state` | `routes/upload_routes.py` | Returns all saved state for a session so the browser can restore where the user left off. |
| `invalidate_downstream` | `routes/upload_routes.py` | Clears results from later steps when the user goes back and changes something earlier. |
| `export_csv` | `routes/export_routes.py` | Finds a stored view result and sends it to the browser as a CSV download. |
| `_resolve_sessions_dir` | `shared/db.py` | Finds the folder on disk where session database files are stored. |
| `_db_path` | `shared/db.py` | Builds the full file path for a specific session's database. |
| `get_session_db` | `shared/db.py` | Opens (or retrieves from cache) the database connection for a session. Reuses existing connections from an LRU cache and evicts old ones when the cache is full. |
| `close_session_db` | `shared/db.py` | Closes a cached connection for a session and removes it from the cache. |
| `session_exists` | `shared/db.py` | Checks whether a session database file exists on disk. |
| `_ensure_meta` | `shared/db.py` | Creates the internal settings table if it doesn't exist yet. |
| `get_meta` | `shared/db.py` | Reads a stored setting from the session. |
| `set_meta` | `shared/db.py` | Saves a setting to the session. |
| `delete_meta` | `shared/db.py` | Removes a setting from the session. |
| `get_all_meta_keys` | `shared/db.py` | Lists all settings stored in the session. |
| `delete_session` | `shared/db.py` | Deletes a session's database file from disk. |
| `get_model` | `shared/ai_client.py` | Returns the name of the AI model being used. |
| `get_client` | `shared/ai_client.py` | Creates the AI client connection using the API key and server settings. |
| `_cache_key` | `shared/ai_client.py` | Creates a unique fingerprint for an AI request so identical requests can be served from cache. |
| `_extract_json` | `shared/ai_client.py` | Reads the AI's text response and pulls out the JSON data, handling markdown code fences if present. |
| `call_ai_json` | `shared/ai_client.py` | Sends a prompt to the AI and gets back structured JSON (with caching and retries). |
| `call_ai_json_validated` | `shared/ai_client.py` | Same as above but also checks the response matches the expected data format. |
| `format_spend` | `shared/formatting.py` | Formats a number as a compact currency string (e.g. 1200000 becomes "$1.2M"). |
| `format_pct` | `shared/formatting.py` | Formats a fraction as a percentage (e.g. 0.85 becomes "85.0%"). |

---

## Launcher (`launcher.py`)

### Startup & Configuration

| Function | File | What it does |
|----------|------|--------------|
| `_base_path` | `launcher.py` | Figures out where the app's files live — the extracted folder when running as a packaged app, or the project folder when running in development. |
| `_resolve` | `launcher.py` | Builds a full file path by joining pieces onto the base path. |
| `_setup_ssl` | `launcher.py` | Sets up the SSL certificate bundle for HTTPS calls. Checks for a user-provided bundle first, then a corporate-set environment variable, then the built-in certificates. |
| `_validate_resources` | `launcher.py` | Before starting, checks that all critical files (frontend pages, backend code, certificates) are present. Returns whether everything looks good. |
| `_cleanup_stale_sessions` | `launcher.py` | On startup, finds and deletes leftover session folders from previous runs that are older than 24 hours. |
| `_run_diagnostics` | `launcher.py` | When the user runs the app with --diagnostics, checks ports, certificates, disk space, DuckDB, and file presence, then prints a report. |

### Port Management

| Function | File | What it does |
|----------|------|--------------|
| `_port_available` | `launcher.py` | Checks whether a specific port number is free on the computer. |
| `_pick_port` | `launcher.py` | Tries to use the preferred port; if something else is using it, tries the next few port numbers until it finds a free one. |

### Module Import Isolation

| Function | File | What it does |
|----------|------|--------------|
| `_import_module_app` | `launcher.py` | Loads a module's backend code in a way that prevents its packages from clashing with other modules' identically-named packages. |
| `_post_load_cleanup` | `launcher.py` | After all modules are loaded, locks down the import system so packages from different modules can never accidentally get mixed up. |
| `_ModuleRedirectFinder` | `launcher.py` | A custom Python import hook that watches for package imports and automatically routes them to the correct module based on which module's code is asking. |

### Static File Serving

| Function | File | What it does |
|----------|------|--------------|
| `_add_static_serving` | `launcher.py` | Adds routes to a module's server so it can serve its frontend web pages. Returns 404 for missing asset files instead of a confusing fallback page. |
| `_add_config_endpoint` | `launcher.py` | Adds a /config.json endpoint to each server that tells the frontend the actual URLs of all modules (useful when ports shift dynamically). |

### Server Lifecycle

| Function | File | What it does |
|----------|------|--------------|
| `_run_server` | `launcher.py` | Starts a web server for one module on a specific port. Runs in a background thread. |
| `_wait_for_health` | `launcher.py` | After starting all servers, keeps checking each one until they all respond to confirm they're fully ready. |
| `_cleanup` | `launcher.py` | Shuts down all servers, cleans up session files, and deletes temporary caches. Runs both on normal exit and if the app crashes. |
| `main` | `launcher.py` | The main entry point — validates resources, picks ports, loads all modules, starts servers, waits for them to be healthy, opens the browser, and monitors everything until the user closes the app. |
