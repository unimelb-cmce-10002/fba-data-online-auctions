# --- Packages ---
# install.packages(c("rjson","tidyverse","janitor","arrow","DBI","duckdb","writexl","googlesheets4"))
library(rjson); library(tidyverse); library(janitor); library(arrow)
library(DBI); library(duckdb); library(writexl); library(googlesheets4)
library(googledrive)


# --- Config (JSON array only) ---
json_array_path <- "output/ebay_auctions_large.json"
out_dir <- "data_out"; dir.create(out_dir, showWarnings = FALSE)

# --- Read JSON array ---
# x <- rjson::fromJSON(file = json_array_path)
# raw_tbl <- as_tibble(dplyr::bind_rows(x))

x_raw <- rjson::fromJSON(file = json_array_path)

rows <- purrr::map(x_raw, function(a) {
  tibble::tibble(
    item_id     = as.character(a$item_id),
    title       = as.character(a$title),
    category    = as.character(a$category),
    start_time  = as.character(a$start_time),
    end_time    = as.character(a$end_time),
    seller      = list(a$seller),   # <- keep nested as list-col
    bids        = list(a$bids),     # <- keep nested as list-col
    final_price = as.numeric(a$final_price),
    winner_id   = as.character(a$winner_id)
  )
})

raw_tbl <- dplyr::bind_rows(rows)

# --- Normalize: sellers, auctions, bids ---
sellers <- raw_tbl |>
  unnest_wider(seller, names_sep = "_") |>
  clean_names() |>
  transmute(
    seller_user_id = seller_user_id,
    seller_rating  = as.numeric(seller_rating),
    seller_country = seller_country
  ) |>
  distinct()

auctions <- raw_tbl |>
  unnest_wider(seller, names_sep = "_") |>
  clean_names() |>
  mutate(
    start_time = as.POSIXct(start_time, tz = "UTC"),
    end_time   = as.POSIXct(end_time,   tz = "UTC")
  ) |>
  transmute(
    item_id, title, category, start_time, end_time,
    seller_user_id = seller_user_id,
    final_price = as.numeric(final_price),
    winner_id
  )

bids <- raw_tbl |>
  select(item_id, bids) |>
  unnest_longer(bids) |>
  unnest_wider(bids, names_sep = "_") |>
  rename(
    bidder_id = bids_bidder_id,
    bid_amount = bids_amount,
    bid_time   = bids_time
  ) |>
  mutate(
    bid_amount = as.numeric(bid_amount),
    bid_time   = as.POSIXct(bid_time, tz = "UTC")
  ) |>
  arrange(item_id, bid_time)

# --- Save: CSV / TSV / Pipe ---
write_delimited <- function(df, base, delim, ext)
  readr::write_delim(df, file.path(out_dir, paste0(base, ".", ext)), delim = delim)

save_text <- function(df, base) {
  readr::write_csv(df, file.path(out_dir, paste0(base, ".csv")))
  write_delimited(df, base, "\t", "tsv")
  write_delimited(df, base, "|",  "psv")
}
save_text(auctions, "auctions"); save_text(bids, "bids"); save_text(sellers, "sellers")

# --- Save: Excel (3 sheets) ---
writexl::write_xlsx(
  list(auctions = auctions, bids = bids, sellers = sellers),
  file.path(out_dir, "auctions_workbook.xlsx")
)

# --- Save: Google Sheets (optional; run gs4_auth() once) ---
# 0) Use a truly temporary, in-memory token (won’t touch your global cache)
options(gargle_oauth_cache = FALSE)

# 1) Start clean and auth with SHEETS scope only
gs4_deauth()
gs4_auth(
    scopes = "https://www.googleapis.com/auth/spreadsheets",  # no Drive scope
    cache  = FALSE
)

# 2) Refer to your already-created spreadsheet by ID (NOT by Drive path)
#    Replace <YOUR_SHEET_ID> with the long ID from the URL:
#    https://docs.google.com/spreadsheets/d/<YOUR_SHEET_ID>/edit#gid=0
ss <- as_sheets_id("1oGYbwymD_hRoi-RcD40lSEjYiC81TEOy29RebhPNwTI")   # this does NOT hit the Drive API

# 3) Write the tabs (Sheets API only)
sheet_write(auctions, ss = ss, sheet = "auctions")
sheet_write(bids,     ss = ss, sheet = "bids")
sheet_write(sellers,  ss = ss, sheet = "sellers")

# --- Save: DuckDB (3 tables) ---
save_duckdb <- function(auctions, bids, sellers, dbname = file.path(out_dir, "auctions.duckdb")) {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = dbname)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  dbWriteTable(con, "auctions", auctions, overwrite = TRUE)
  dbWriteTable(con, "bids",     bids,     overwrite = TRUE)
  dbWriteTable(con, "sellers",  sellers,  overwrite = TRUE)
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_auctions_seller ON auctions(seller_user_id)")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_bids_item ON bids(item_id)")
  dbExecute(con, "CREATE INDEX IF NOT EXISTS idx_sellers_id ON sellers(seller_user_id)")
}
save_duckdb(auctions, bids, sellers)

# --- Save: Parquet (one file per table) ---
arrow::write_parquet(auctions, file.path(out_dir, "auctions.parquet"))
arrow::write_parquet(bids,     file.path(out_dir, "bids.parquet"))
arrow::write_parquet(sellers,  file.path(out_dir, "sellers.parquet"))

message("Done. Files in: ", normalizePath(out_dir))
