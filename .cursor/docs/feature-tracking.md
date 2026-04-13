# Feature Tracking

A detailed reference of what the user can do at each step in each module of the ProcIP Suite. Every feature is listed as its own item with a short explanation.

---

## Module 1 — DataStitcher (`DataConsolidationAppV7`)

The DataStitcher helps users combine multiple data files into one clean, merged dataset. It has 7 sidebar steps (8 internal steps; Merge covers 6 and 7). When only one file is uploaded, the app skips Append Strategy and Merge automatically.

### Upload + Settings

| # | Feature | Details |
|---|---------|---------|
| 1 | Drag-and-drop upload | Drag files or an entire folder onto the upload area. The area highlights when you drag over it. |
| 2 | Browse files | Click "Browse Files" to pick one or more files from your computer (ZIP, CSV, Excel). |
| 3 | Browse folder | Click "Browse Folder" to select a whole folder — all supported files inside are picked up. |
| 4 | Multi-file staging | Add files one at a time — they appear in a scrollable list. Hover any file to see its full path. |
| 5 | Remove single file | Click the X next to any staged file to remove just that one. |
| 6 | Remove all files | Click "Remove All" to clear the entire file list and start over. |
| 7 | Add more files | Click "Add Files" after staging some to add more from the file picker. |
| 8 | ZIP packaging | When you drop multiple non-ZIP files, the app automatically zips them together (shows a "Zipping files…" spinner). |
| 9 | API key entry | Type or paste your Portkey API key into a password field. This key is needed for AI-powered steps. |
| 10 | Initialize workspace | Click "Initialize Workspace" to upload everything and move to Data Preview. The button is disabled while uploading or if no files are selected. |

### Data Preview

| # | Feature | Details |
|---|---------|---------|
| 1 | Table list | See all extracted tables from your uploads, each showing its name, row count, and column count. |
| 2 | Upload warnings | If any files failed to parse, a warning panel at the top lists each one with the error message. |
| 3 | Expand/collapse tables | Click any table row (or its chevron) to expand or collapse the data preview for that table. |
| 4 | Redefine header row | Click the "Redefine header row" button to open an editor where you can pick which row should be the column headers. |
| 5 | Pick header row | In the header editor, click a row number on the left side to select that row as the header. |
| 6 | Custom header names | When the chosen header row has empty cells, text fields appear so you can type custom column names. |
| 7 | Confirm/cancel header | Click "Confirm Row N" to apply your chosen header, or "Cancel" to close without changes. |
| 8 | Delete table | Click the trash icon on a table, then confirm, to remove that table from the session entirely. |
| 9 | Select rows | Use checkboxes in the preview table to select individual rows. |
| 10 | Delete selected rows | Click "Delete Selected Rows" to remove the checked rows (asks for confirmation first). |
| 11 | Clear row selection | Click "Clear" to uncheck all selected rows. |
| 12 | Data preview grid | Scroll through a preview of the table's data (shows the first 50 rows). |
| 13 | Single table flow | If only one table was uploaded, a "Single Table Flow" option appears that skips the Append Strategy and Merge steps. |
| 14 | Proceed to Append | Click "Proceed to Append Strategy" to move to the next step. |

### Append Strategy

| # | Feature | Details |
|---|---------|---------|
| 1 | Generate append plan | Click "Generate Append Plan" to let the AI suggest how to group related tables together for stacking. |
| 2 | Group cards | Each AI-suggested group appears as a card showing the group name, file count, and the AI's reasoning. |
| 3 | Rename groups | Click a group's title to enter rename mode. Type a new name and press Enter to save, or Escape to cancel. |
| 4 | Select tables | Click table names to select them (multi-select). A coloured strip shows how many are selected. |
| 5 | Inline table preview | Click the eye icon on any table to show or hide a mini-preview (first 5 rows) right in the card. |
| 6 | Exclude tables | Click the X on a table or use the "…" menu to exclude it from processing. Excluded tables move to a separate section. |
| 7 | Restore excluded | In the Excluded section, click "Restore" to bring a table back into play. |
| 8 | Move between groups | Use the "…" menu on a table to move it to another group or to the Unassigned area. |
| 9 | Unassigned tables | Tables not in any group appear as chips in an "Unassigned" section. You can select and move them. |
| 10 | Create new group | Select tables and click "Create Group" (in the header or the dashed area at the bottom) to form a new group. |
| 11 | Analyse header alignment | Click "Analyze Header Alignment" to see how columns from different tables line up within each group. |
| 12 | Edit column mapping | In the alignment grid, use dropdowns to change which source column maps to which position. |
| 13 | Re-analyse mappings | If mappings already exist, click "Re-analyze Mappings" to get fresh suggestions. |
| 14 | Execute append | Click "Execute Append & Stack" to combine all tables within each group into one stacked table per group. |
| 15 | Append report | After stacking, view a report showing: groups created, total rows, integrity status, per-file row contributions, and column statistics (fill rate, nulls, distinct values). |
| 16 | Show all columns | In the report, click "Show all N columns" if the column stats table is truncated. |
| 17 | Proceed to normalisation | Click "Proceed to Header Normalisation" to move on. |

### Header Normalisation

| # | Feature | Details |
|---|---------|---------|
| 1 | Run AI normalisation | Click "Run Header Normalisation" to let the AI suggest which standard procurement field each column should map to. |
| 2 | Per-column action | For each column, choose an action from a dropdown: AUTO (accept AI suggestion), KEEP (keep original name), or DROP (remove the column). |
| 3 | Change mapped field | Use the "Mapped To" dropdown to pick a different standard field from the full list. |
| 4 | Custom column name | Click the pencil icon to switch to a text input where you can type any custom name. Click again to go back to the dropdown. |
| 5 | Expand group panels | Click a group's panel header to expand or collapse the mapping grid for that group. See row/column counts and AUTO/KEEP/DROP tallies. |
| 6 | Fullscreen view | Click "Expand Fullscreen" to open a group's mapping grid in a fullscreen modal (close with X or Escape). |
| 7 | Download Excel | Click "Download Excel" to get a workbook of the current mappings for offline review and editing. |
| 8 | Upload Excel | Toggle "Manual Excel Workflow" on, then click "Upload Excel" to load a mapping workbook that overrides the current decisions. |
| 9 | Apply mappings | Click "Apply & Proceed to Data Cleaning" to rename all columns according to the approved mappings. |
| 10 | Re-run AI | Click "Re-Run AI" to discard current suggestions and get fresh ones from the AI. |
| 11 | Skip normalisation | Click "Skip to Data Cleaning" to move on without applying any mappings. |
| 12 | Preview grid | Scroll through a table preview showing the data with current column names. Shows row and column counts in the footer. |

### Data Cleaning

| # | Feature | Details |
|---|---------|---------|
| 1 | Two sub-tabs | Switch between "Tab 5a — Cleaning" (basic cleaning) and "Tab 5b — Additional" (dedup, standardise, concat, remove). Each tab shows a count badge when work has been done. |

**Tab 5a — Cleaning**

| # | Feature | Details |
|---|---------|---------|
| 2 | Group selector | Pick a group from the left sidebar. Groups show a checkmark when they've been cleaned and display row counts. |
| 3 | Remove null rows | Toggle on to remove rows where key columns are empty. |
| 4 | Remove null columns | Toggle on to remove columns that are entirely empty. |
| 5 | Trim whitespace | Toggle on to strip leading and trailing spaces from all text values. |
| 6 | Standardise case | Toggle on, then choose "UPPER CASE" or "lower case" from a dropdown to convert all text. |
| 7 | Apply cleaning | Click "Apply Cleaning" to run all selected options on the chosen group. |
| 8 | Data preview | See a preview table of the selected group's data after cleaning. |

**Tab 5b — Deduplication**

| # | Feature | Details |
|---|---------|---------|
| 9 | Select key columns | Click column header buttons to mark which columns define a "duplicate" row. |
| 10 | Clear key columns | Click "Clear all" to uncheck all selected columns at once. |
| 11 | Preview dedup stats | Click "Preview Stats" to see how many duplicates would be removed. View stat tiles with counts. |
| 12 | Apply deduplication | Click "Apply Deduplication" to actually remove the duplicate rows. See a summary when done. |

**Tab 5b — Column Standardization**

| # | Feature | Details |
|---|---------|---------|
| 13 | Select columns | Click column header buttons to pick which columns to analyse. |
| 14 | Analyse formats | Click "Analyze" to see what formats and patterns exist in the selected columns. Per-column stat chips show things like leading-zero percentages and min/max lengths. |
| 15 | Choose operation | For each column, pick "No Change", "Strip Leading Zeros", or "Pad to Fixed Length" from a dropdown. If padding, enter the desired length in a number input. |
| 16 | Apply standardisation | Click "Apply Standardization" to apply the chosen operations. |

**Tab 5b — Concatenation**

| # | Feature | Details |
|---|---------|---------|
| 17 | Select columns to combine | Click column header buttons (in order) to pick which columns to concatenate. The order you click determines the order in the combined value. |
| 18 | Formula preview | See a live preview of what the new column name and value formula will look like. |
| 19 | Apply concatenation | Click "Apply Concatenation" (needs at least 2 columns) to create the new combined column. |
| 20 | Delete concat column | For each previously created concatenated column, click "Delete" to remove it. |

**Tab 5b — Column Removal**

| # | Feature | Details |
|---|---------|---------|
| 21 | Select columns to remove | Click column header buttons to mark columns for removal. |
| 22 | Remove columns | Click "Remove Columns" to drop the selected columns (they're backed up). |
| 23 | Restore columns | For each previously removed column, click "Restore" to bring it back. |
| 24 | Clear selection | Click "Clear all" to uncheck everything. |

**Footer**

| # | Feature | Details |
|---|---------|---------|
| 25 | Status summary | See badges showing how many groups were cleaned, deduped, concat columns created, and columns removed. |
| 26 | Skip | Click "Skip" to move on without any cleaning. |
| 27 | Proceed to merge | Click "Proceed to Merge" to continue. In single-table mode, this goes directly to Data Quality instead. |

### Merge

| # | Feature | Details |
|---|---------|---------|
| 1 | AI base recommendation | When the step loads, the AI recommends which table to use as the "base" for joining. |
| 2 | Choose base table | Pick the base table from a dropdown (shows name, row count, and column count). |
| 3 | Choose source table | Pick the source table from a separate dropdown. |
| 4 | Skip merge | Click "Skip merge entirely" if you don't want to join tables (e.g. only one group exists). |
| 5 | Side-by-side column view | See columns from both tables displayed side by side with colour coding (identifiers, descriptors, metrics, keys, etc.). |
| 6 | Pair join keys | Click key buttons above base columns, then matching source columns, to pair them as join keys. Key pairs appear as chips you can remove with X. |
| 7 | Pull source columns | Click source column headers to "pull" them into the merged result. Pull chips appear and can be removed with X. |
| 8 | Fullscreen preview | Click "Full Screen" on the matching card to see columns in a larger view (close with X or Escape). |
| 9 | Join simulation | Once keys are selected, a simulation runs automatically showing match rates, duplicate explosion risk, unmatched-row counts, and estimated null rates. |
| 10 | Execute merge | Click "Execute Merge" to run the join. A progress bar shows the current phase (Prepare, Dedup & Merge, Validate) with percentage. |
| 11 | Merge summary | After merging, see a summary with row/column counts and version label. |
| 12 | Merge details | See which keys were used, how many columns were pulled, and validation results. |
| 13 | Column quality table | Scroll a table showing fill rate bars, null counts, and distinct-value counts for each column in the merged result. |
| 14 | Merged data preview | Preview the first 50 rows of the merged table. |
| 15 | Redo merge | Click "Redo Merge" to go back and try different settings. |
| 16 | Merge another pair | Click "Perform Another Merge" to join a different pair of tables. |
| 17 | Go to data quality | Click "Run Data Quality Assessment" to move on to the quality step. |

**Merge Outputs Panel (slide-out, available from step 6 onward)**

| # | Feature | Details |
|---|---------|---------|
| 18 | Open/close panel | Click the floating tab on the right edge to open the merge outputs panel. Close with the X or collapse chevron. |
| 19 | Download single output | Click the download icon on any merge version to get it as CSV. |
| 20 | Download all outputs | Click "Download All" to get all merge versions bundled together. |
| 21 | Delete output | Click trash on an output, then confirm, to delete that version. |
| 22 | Send to Normalizer | Click "Send to Normalizer" to transfer the merged data to Module 2 (opens in a new tab). |
| 23 | Send to Summarizer | Click "Send to Summarizer" to transfer the merged data to Module 3 (opens in a new tab). |
| 24 | Select output for transfer | When sending, click an output card to select it, then click "Send" or "Cancel". |
| 25 | Download on failure | If the transfer fails, click "Download instead" to get the file locally. |

**Merge Report (step 7 — shown when merge is skipped or after completing)**

| # | Feature | Details |
|---|---------|---------|
| 26 | Output summary | See a summary of all merge outputs and their row counts. |
| 27 | Merge history | Expand the "Merge History" section to see all past merge versions. |
| 28 | Go back to merge | Click "Go Back to Merge" to return to the merge setup. |

### Data Quality

| # | Feature | Details |
|---|---------|---------|
| 1 | Choose merge version | If multiple merges were run, pick which version to assess from a dropdown. In single-table mode, the table is selected automatically. |
| 2 | Re-run all | Click "Re-run All" to refresh every quality assessment at once. |
| 3 | Date panel | Expand the "Date" panel to see date format distributions and timeline charts. If multiple date columns exist, pick which one to analyse from a dropdown. |
| 4 | Currency panel | Expand the "Currency" panel to see which currencies appear, their distribution, and consistency metrics. |
| 5 | Payment Terms panel | Expand the "Payment Terms" panel to see how payment terms are distributed across spend. |
| 6 | Country/Region panel | Expand the "Country & Region" panel to see completeness of country and region fields, with value chips (shows "+N more" when there are many). |
| 7 | Supplier panel | Expand the "Supplier" panel to see supplier concentration, top-supplier breakdowns, and related metrics. |
| 8 | AI summaries | Each panel includes an AI-written narrative explaining the findings in plain language. |
| 9 | Retry on error | If a panel fails to load, click "Retry" to try again for just that one. |
| 10 | Expand/collapse panels | Click any panel's header to expand or collapse it (chevron rotates to show state). |

### App-Wide Features (available across all steps)

| # | Feature | Details |
|---|---------|---------|
| 1 | Sidebar navigation | Click any step you've already reached in the sidebar to jump back to it. Locked steps show "Complete previous steps first". |
| 2 | Back to Home | Click "Back to Home" in the top bar to return to the landing page. |
| 3 | Theme toggle | Click the sun/moon icon to switch between light and dark mode. |
| 4 | API key status | The sidebar footer shows whether your API key is set (green) or missing (red). Click it to jump to the Upload step. |
| 5 | Status log | A collapsible "Live pipeline activity" bar at the bottom shows what the app is doing. Click to expand. Click "Clear" (with confirmation) to empty it. |
| 6 | Loading overlay | During AI operations, a full-screen overlay shows the current action. Some operations offer a "Cancel" button. |
| 7 | Step-change warning | If going back would wipe results from later steps, a warning dialog appears with "Cancel" and "Continue" options. |
| 8 | Error banner | When something goes wrong, a red error banner appears with the message. Click "Retry" if available. |
| 9 | Raw Data Preview overlay | Click the table icon to open a full-screen overlay showing raw data from any table (with tab switching). |
| 10 | Results Preview overlay | Click the database icon to open a similar overlay showing processed results. |

---

## Module 2 — Normalizer (`ProcIP_Module2-main`)

The Normalizer takes a single data table (uploaded directly or received from the DataStitcher) and standardises key columns. It has 3 main steps, with step 3 broken into 7 sub-operations plus a Download Pack.

### Upload Data

| # | Feature | Details |
|---|---------|---------|
| 1 | Drag-and-drop upload | Drag files or a folder onto the upload area. The area highlights when you drag over it. |
| 2 | Browse files | Click "Browse Files" to pick one or more files from your computer (ZIP, CSV, Excel). |
| 3 | Browse folder | Click "Browse Folder" to select an entire folder. |
| 4 | File list | Staged files appear in a list. See a success indicator and file count when ready. |
| 5 | Remove file | Click the X on the upload area to clear the staged file and reset. |
| 6 | API key entry | Type or paste your API key into a password field. |
| 7 | Upload | Click "Upload & Analyze" to upload and process the files. Disabled while uploading or if no file is selected. |
| 8 | Import from DataStitcher | The app can receive data automatically from Module 1's "Send to Normalizer" button — data loads and you jump straight to Data Preview. |

### Data Preview

| # | Feature | Details |
|---|---------|---------|
| 1 | Table list | See all extracted tables with their name, row count, and column count. |
| 2 | Import source badge | If data came from the DataStitcher, a badge shows "Imported from DataStitcher". |
| 3 | Expand/collapse tables | Click a table row or its chevron to expand or collapse the data preview. |
| 4 | Redefine header row | Click the header-row button to open an editor where you pick which row to use as headers. |
| 5 | Pick header row | In the editor, click a row number to select it as the header. |
| 6 | Custom header names | Type custom names for any empty header cells in the text fields that appear. |
| 7 | Confirm/cancel header | Click "Confirm" to apply or "Cancel" to discard. |
| 8 | Delete table | Click trash, then confirm, to remove a table from the session. |
| 9 | Select and delete rows | Use checkboxes to select rows, then click "Delete Selected Rows" (with confirmation). Click "Clear" to uncheck all. |
| 10 | Data preview grid | Scroll through a preview of the data (up to 50 rows). |
| 11 | Proceed with table | Click "Proceed with this Table" to lock in one table and advance to Normalization. |

### Normalization — Supplier Names

| # | Feature | Details |
|---|---------|---------|
| 1 | Disabled status | This operation is currently disabled in the app and cannot be clicked in the sidebar. |
| 2 | Run (when enabled) | When enabled, clicking "Run" would clean supplier names by removing legal suffixes (Inc, Ltd), trimming spaces, and merging near-duplicate names. |
| 3 | Result message | After running, a success or error message appears with details. |
| 4 | Download Excel | After a successful run, download the result as Excel. |
| 5 | Send to Summarizer | After a successful run, send the data to Module 3. |

### Normalization — Supplier Country

| # | Feature | Details |
|---|---------|---------|
| 1 | Choose country column | Select which column contains country data from a dropdown (required). Changing the selection resets the assessment. |
| 2 | Assess column | Click "Assess" to check how well-populated the country column is. See the population percentage and row counts. |
| 3 | Low population warning | If less than 60% of rows have a country value, a warning badge appears. Otherwise, a green "Ready to normalize" message shows. |
| 4 | Confirm and run | Click "Confirm & Run" to standardise country names using a built-in lookup of abbreviations and aliases, with AI as a fallback for unclear values. |
| 5 | Normalization summary | After running, see a summary: total rows normalised, how many used the lookup vs. AI, number of distinct countries found, and any rows that couldn't be matched. |
| 6 | AI error list | If the AI couldn't handle some values, they appear in an amber warning list. |
| 7 | Download Excel | Download the normalised data as Excel. |
| 8 | Send to Summarizer | Send the data directly to Module 3. |
| 9 | Preview table | After running, see a preview of the data with the normalised country column highlighted. Click "Refresh" to reload the preview. |

### Normalization — Dates

| # | Feature | Details |
|---|---------|---------|
| 1 | Choose target format | Select your preferred date format from a dropdown: "dd-mm-yyyy" or "mm-dd-yyyy". |
| 2 | Run normalization | Click "Run" to convert all date-like columns to the chosen format. The app handles Excel serial numbers, partial dates, and many international formats. |
| 3 | Re-run | Click "Re-run" to normalise again (e.g. after changing the target format). |
| 4 | Result message | See a success or error message with details after running. |
| 5 | Download Excel | Download the result as Excel. |
| 6 | Send to Summarizer | Send data to Module 3. |
| 7 | Preview table | See a preview with the new normalised date columns highlighted next to the originals. |

### Normalization — Currency Conversion

| # | Feature | Details |
|---|---------|---------|
| 1 | Info callout | A note recommends doing Date Normalization first for better exchange rate matching. |
| 2 | Choose currency column | Select which column contains currency codes (required). Changing resets any assessment. |
| 3 | Choose spend column | Select which column contains the spend/amount values (required). |
| 4 | Choose date column | Select a date column, or choose "No Date col" if dates aren't available. |
| 5 | Scope year | When "No Date col" is selected, pick a scope year (2023–2026) for exchange rate lookup. |
| 6 | Assess | Click "Assess" to check for unsupported currencies, missing exchange rates, and column population. |
| 7 | Unsupported currency panel | If some currencies aren't in the FX table, a detailed panel appears showing which currencies are unsupported, how many rows they affect, and their total spend. |
| 8 | FX rate mode toggle | Switch between "Yearly" and "Monthly" override modes. Monthly is only available when a date column is selected. |
| 9 | Enter manual FX rates | For each unsupported currency, type exchange rates into number fields (yearly: one per year; monthly: one per month). Empty fields mean those rows will be skipped. |
| 10 | Validation feedback | If required FX rates are missing, fields get a red border and a warning message appears. |
| 11 | Confirm and run | Click "Confirm & Run" to convert all spend values to USD using the exchange rate table. |
| 12 | Conversion summary | After running, see: rows converted, rows using fallback rates, and a breakdown of any rows not converted (missing currency, unsupported, invalid spend, unparseable date). |
| 13 | Download Excel | Download the result. |
| 14 | Send to Summarizer | Send data to Module 3. |
| 15 | Preview table | See a preview with the converted spend columns highlighted. |

### Normalization — Payment Terms

| # | Feature | Details |
|---|---------|---------|
| 1 | Run normalization | Click "Run" to standardise payment terms. The app uses pattern matching to extract days, discount, and a "doubt" flag, then uses AI for clean English descriptions. |
| 2 | Re-run | Click "Re-run" to process again. |
| 3 | Result message | See a success or error message. |
| 4 | Download Excel | Download the result. |
| 5 | Send to Summarizer | Send data to Module 3. |
| 6 | Preview table | See a preview with the new payment terms columns highlighted. |

### Normalization — Regions

| # | Feature | Details |
|---|---------|---------|
| 1 | Info callout | A note recommends doing Supplier Country normalization first so regions can be derived from standardised country names. |
| 2 | Choose column | Select which column contains region or country data from a dropdown (required). |
| 3 | Assess | Click "Assess" to see how well-populated the column is. See population percentage and row counts. |
| 4 | Derivation explanation | An info panel explains whether the app will use the column directly as a region or derive regions from country names. |
| 5 | Low population warning | Below-60% population triggers a warning badge. Otherwise, a green "Ready" message shows. |
| 6 | Confirm and run | Click "Confirm & Run" to assign standard regions (NA, EMEA, APAC, LATAM) using built-in rules, with AI for edge cases. |
| 7 | Normalization summary | See totals: deterministic matches, AI matches, country-derived fills, and any unmatched rows. |
| 8 | AI error list | Any AI failures appear in an amber list. |
| 9 | Download Excel | Download the result. |
| 10 | Send to Summarizer | Send data to Module 3. |
| 11 | Preview table | See a preview with the normalised region column highlighted. |

### Normalization — Plant / Site

| # | Feature | Details |
|---|---------|---------|
| 1 | Run normalization | Click "Run" to clean up all plant, site, and location columns using rules and AI. |
| 2 | Re-run | Click "Re-run" to process again. |
| 3 | Result message | See a success or error message. |
| 4 | Download Excel | Download the result. |
| 5 | Send to Summarizer | Send data to Module 3. |
| 6 | Preview table | See a preview with the normalised plant columns highlighted. |

### Download Pack

| # | Feature | Details |
|---|---------|---------|
| 1 | Download Excel | Download the fully normalised dataset as an Excel file. A note shows how many of the 7 normalizations were completed. |
| 2 | Send to Summarizer | Click "Send to Summarizer" to transfer data to Module 3. An inline confirmation strip appears — click "Send" to proceed or "Cancel" to back out. |
| 3 | Transfer result | After sending, see a green success or red error banner with a message. |
| 4 | Download on failure | If the transfer fails, click "Download instead" to get the file locally. |
| 5 | Dismiss banner | Click X on the result banner to close it. |

### App-Wide Features (available across all steps)

| # | Feature | Details |
|---|---------|---------|
| 1 | Sidebar navigation | Click any reachable step or normalization sub-operation in the sidebar. Unreachable steps appear greyed out with a tooltip. |
| 2 | Back to Home | Click "Back to Home" to return to the landing page. |
| 3 | Theme toggle | Click the sun/moon button to switch between light and dark mode. |
| 4 | API key status | The sidebar footer shows green ("API Key Set") or red ("API Key Missing"). Click it to jump to Upload. |
| 5 | Status log | A "Live pipeline activity" bar at the bottom shows what the app is doing. Click to expand. Click trash (with confirmation) to clear. |
| 6 | Loading overlay | During long operations, a full-screen overlay shows the current action with a spinner. Some operations offer "Cancel". |
| 7 | Step-change warning | If going back would reset later work, a "Reset Downstream Progress?" dialog appears with "Cancel" and "Continue" options. |
| 8 | Error banner | A red banner appears when something goes wrong. Click "Dismiss" to close it. |
| 9 | Transfer overlay | When sending data to the Summarizer, an animated overlay shows the transfer progress. |

---

## Module 3 — Spend Summarizer (`SummarizationModule`)

The Spend Summarizer takes a procurement dataset and produces charts, quality assessments, and a client-ready email summary. It has 8 steps.

### Upload

| # | Feature | Details |
|---|---------|---------|
| 1 | Drag-and-drop upload | Drag files or a folder onto the upload area. The area highlights when dragging over. |
| 2 | Browse files | Click "Browse Files" to pick files from your computer (ZIP, CSV, Excel). |
| 3 | Browse folder | Click "Browse Folder" to select an entire folder. |
| 4 | Multi-file staging | Files appear in a scrollable list. Each shows its name with a remove (X) button. |
| 5 | Add more files | Click "Add Files" to add more after the initial selection. |
| 6 | Remove single file | Click X on any file to remove it. |
| 7 | Remove all files | Click "Remove All" to clear everything. |
| 8 | File size limit | The app checks that total upload size is under 300 MB. |
| 9 | API key entry | Type or paste your Portkey API key for AI-powered mapping and summaries. |
| 10 | Initialize workspace | Click "Initialize Workspace" to upload and proceed. Disabled while uploading or with no files. |
| 11 | Import from other modules | The app can receive data from the DataStitcher or Normalizer via their "Send to Summarizer" buttons. |

### Data Preview

| # | Feature | Details |
|---|---------|---------|
| 1 | Table list | See all extracted tables with name, row count, and column count. |
| 2 | Import source badge | If data came from another module, a badge shows the source (e.g. "Imported from Data Normalizer"). |
| 3 | Upload warnings | Any files that failed to parse are listed in an amber warning panel. |
| 4 | Expand/collapse tables | Click a table row or chevron to expand or collapse its preview. |
| 5 | Redefine header row | Open the header editor to pick which row to use as column headers. |
| 6 | Pick header row | Click a row number in the raw grid to select it as the header. |
| 7 | Custom header names | Type custom names for empty header cells. |
| 8 | Confirm/cancel header | Apply the new header or cancel without changes. |
| 9 | Delete table | Click trash then confirm to remove a table. |
| 10 | Select and delete rows | Use checkboxes, then "Delete Selected Rows" (with confirmation). "Clear" unchecks all. |
| 11 | Data preview grid | Scroll through up to 50 rows of preview data. |
| 12 | Proceed to Map Columns | Click "Proceed to Map Columns" to continue. |

### Map Columns

| # | Feature | Details |
|---|---------|---------|
| 1 | Detect columns | Click "Detect Columns" to run AI column detection that suggests which of your columns match standard procurement fields. |
| 2 | Mapping grid | Review a table showing each standard field, its expected type (numeric/datetime/string), the AI's suggested match, and status (mapped or unmapped). |
| 3 | Change mapping | Use the dropdown on any field to pick a different column. Options include the AI's pick, alternatives, and a full list of all your columns. |
| 4 | Unmap a field | Select "-- Not mapped --" in the dropdown to leave a field unmapped. |
| 5 | Mapped count | A counter at the top shows how many fields have been mapped out of the total. |
| 6 | Field descriptions | Hover over truncated field descriptions to see the full text in a tooltip. |
| 7 | Show sample data | Click "Show Data Preview" to see a table of sample values from your columns, helping you decide which column matches which field. Click "Hide Preview" to close. |
| 8 | Confirm mapping | Click "Confirm Mapping" to lock in your choices. The app creates a typed analysis table and converts values. |
| 9 | Cast report | After confirming, see a report showing each field's parse success rate (% of values successfully converted) and null counts. |
| 10 | Error display | If confirmation fails, an error message appears inline. |

### Spend Quality Assessment

| # | Feature | Details |
|---|---------|---------|
| 1 | Auto-run | The assessment runs automatically when you reach this step (if session and API key are set). |
| 2 | Loading state | While running, a large spinner shows with "Generating Spend Quality Assessment" and a note that it takes 15–30 seconds. |
| 3 | Total rows stat | The header shows the total number of rows being assessed. |
| 4 | Re-run assessment | Click the refresh icon in the header to re-run the entire assessment. |
| 5 | Date-spend pivot panel | Expand this panel to see a pivot table of spend by year and month. Cells with zero spend are dimmed. |
| 6 | Pareto analysis panel | Expand to see supplier concentration metrics at different thresholds (80%, 85%, 90%, 95%, 99%) — shows total spend, number of transactions, unique transactions, and supplier count at each level. |
| 7 | Description quality panel | Expand to see how many description columns were mapped and their quality. Shows spend coverage per description type. |
| 8 | Top 10 descriptions | Click "Show top 10" on any description type to see the most common descriptions ranked by spend. Click "Hide top 10" to collapse. |
| 9 | AI insight per description | Each description type shows an AI-written insight (rendered as formatted text with bullet points) assessing specificity, length, and usefulness. |
| 10 | Not-mapped indicators | Description types that weren't mapped show "Not mapped" / "N/A" styling. |
| 11 | Retry on failure | If the assessment fails, a "Retry" button appears. |
| 12 | Proceed to Select Views | Click "Proceed to Select Views" to move on. |

### Select Views

| # | Feature | Details |
|---|---------|---------|
| 1 | View checklist | See a list of all available chart and analysis views. Each shows an icon, title, and description. |
| 2 | Toggle view selection | Click any view row to check or uncheck it. |
| 3 | Unavailable views | Views that need columns you didn't map are dimmed with a lock icon and a note saying which fields are missing. |
| 4 | Selection validation | If you try to proceed with nothing selected, an error message appears. |
| 5 | Generate views | Click "Generate Views" to compute all selected views (shows "Computing…" with a spinner). |

### Dashboard

| # | Feature | Details |
|---|---------|---------|
| 1 | View count | The header shows how many views were generated. |
| 2 | Detailed summary | Click the "Detailed Summary" header to expand or collapse the full list of view panels. |
| 3 | Charts | Each view shows its chart: bar charts, horizontal bar charts, Pareto curves, Mekko (marimekko) charts, or category drill-down trees, depending on the view type. Hover over chart elements to see tooltips with values. |
| 4 | Data tables | Click "Show Data Table" / "Hide Data Table" to toggle a sortable table below the chart. Click any column header to sort. Shows up to 50 rows. |
| 5 | Category drill-down tree | For category views, a tree table shows L1 > L2 > L3 hierarchy. Click rows to expand/collapse children. Use "Expand All" or "Collapse All" buttons. See spend, % of parent, % of total, and a share bar for each row. |
| 6 | Pareto threshold slider | For Pareto views, drag the slider (50–95%) to change the concentration threshold. The chart and supplier count update on the fly. |
| 7 | Top-N supplier slider | For supplier ranking views, drag the slider (5–50) to change how many top suppliers are shown. |
| 8 | L1 filter dropdown | For L2 vs L3 Mekko views, choose an L1 category from a dropdown to filter which data is shown. |
| 9 | AI analysis panel | Each view (except category drill-down) has an "AI Analysis" side panel with a markdown-formatted summary. On large screens, click the edge button to collapse or expand this panel. |
| 10 | Export CSV | Click the CSV button on any view to download its data as a CSV file. |
| 11 | Excluded rows note | If rows were excluded due to null values, a footnote shows how many. |
| 12 | Loading spinners | While a view is recomputing (e.g. after changing a slider), a spinner overlay appears on the chart. |
| 13 | View Procurement Feasibility | Click this button at the bottom to continue to Procurement Views. |

### Procurement Views

| # | Feature | Details |
|---|---------|---------|
| 1 | Loading state | A spinner with "Checking procurement view feasibility…" appears while results load. |
| 2 | Feasibility cards | A responsive grid of cards shows each advanced procurement analysis. Available views have a green check. Unavailable views show which required columns are missing. |
| 3 | Error display | If the check fails, an error message is shown. |
| 4 | Continue to Email | If an API key is set, click "Continue to Email" to open the email context form. |

### Email

**Context Modal**

| # | Feature | Details |
|---|---------|---------|
| 1 | Recipient name | Type the name of the person receiving the email. |
| 2 | Client name | Type the client company name. |
| 3 | Sender name | Type your name. |
| 4 | Sender role | Type your role/title. |
| 5 | Scope note | Optionally type a note about the scope of the analysis. |
| 6 | Next steps | Add rows for recommended next steps. Each row has Action, Owner, and Timeline fields. Click "Add Row" for more, or trash to delete a row. |
| 7 | Generate email | Click "Generate Email" to submit the context and produce the email using AI. |
| 8 | Cancel | Click X or "Cancel" to close the modal without generating. |

**Email Step**

| # | Feature | Details |
|---|---------|---------|
| 9 | Loading state | While the AI generates the email, a spinner shows with "Generating your email…" and a time estimate. |
| 10 | Back to Dashboard | Click "Back to Dashboard" to return to Procurement Views. |
| 11 | Edit/Preview toggle | Switch between "Edit" mode (editable text fields) and "Preview" mode (formatted display). |
| 12 | Edit subject | In edit mode, change the email subject line in a text field. |
| 13 | Edit body | In edit mode, edit the email body in a large text area. |
| 14 | Preview rendering | In preview mode, the subject shows as plain text and the body renders as formatted markdown. |
| 15 | Regenerate | Click "Regenerate" to generate a new email using the same context you submitted earlier. |
| 16 | Copy to clipboard | Click "Copy to Clipboard" to copy the subject and body. A brief "Copied!" confirmation appears. |
| 17 | AI failure handling | If AI generation fails, an amber banner shows the error with a "Retry" button. |
| 18 | Use fallback template | If AI fails and a fallback is available, click "Use Template Fallback" to get a pre-written consulting-style email filled with your data. |

### App-Wide Features (available across all steps)

| # | Feature | Details |
|---|---------|---------|
| 1 | Sidebar navigation | Click any completed step to jump back. Locked steps show a tooltip. Each step shows a number badge or checkmark. |
| 2 | Start new analysis | Click "Start New Analysis" in the sidebar to clear the session and return to Upload. |
| 3 | Back to Home | Click "Back to Home" to return to the landing page. |
| 4 | Theme toggle | Click sun/moon to switch light/dark mode. |
| 5 | Hero banner | The top banner shows the current step name, description, and step counter. |
| 6 | Loading overlay | Full-screen overlay with spinner during long operations. |
| 7 | Step-change warning | Warning dialog when going back would reset later work. |
| 8 | Error banner | Red error banner for steps 1–4 (steps 5–7 handle errors inline). |
| 9 | Error boundary | If a step crashes, a "Something went wrong" screen appears with "Try Again". |
| 10 | Session restore | The app can restore your session from the URL or local storage so you pick up where you left off. |
