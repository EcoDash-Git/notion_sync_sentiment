#!/usr/bin/env Rscript

# ─────────────────────────────────────────────────────────────────────────────
# Supabase (Postgres) → Notion (paged, resumable)
# Source table: twitter_raw_plus_sentiment
# Fields used: tweet_id, ave_sentiment, sentiment, anger, anticipation,
#              disgust, fear, joy, sadness, surprise, trust, clean_text
# ─────────────────────────────────────────────────────────────────────────────

# --- packages ----------------------------------------------------------------
need <- c("httr2","jsonlite","DBI","RPostgres")
new  <- need[!need %in% rownames(installed.packages())]
if (length(new)) install.packages(new, repos = "https://cloud.r-project.org")
invisible(lapply(need, library, character.only = TRUE))

# --- config ------------------------------------------------------------------
NOTION_TOKEN       <- Sys.getenv("NOTION_TOKEN")
DB_ID              <- Sys.getenv("NOTION_DATABASE_ID")
NOTION_VERSION     <- "2022-06-28"

NUM_NA_AS_ZERO     <- tolower(Sys.getenv("NUM_NA_AS_ZERO","false")) %in% c("1","true","yes")
INSPECT_FIRST_ROW  <- tolower(Sys.getenv("INSPECT_FIRST_ROW","false")) %in% c("1","true","yes")
DUMP_SCHEMA        <- tolower(Sys.getenv("DUMP_SCHEMA","false"))       %in% c("1","true","yes")
RATE_DELAY_SEC     <- as.numeric(Sys.getenv("RATE_DELAY_SEC","0.20"))
IMPORT_ALL         <- TRUE   # no date column in this table; always read all (paged)
RUN_SMOKE_TEST     <- tolower(Sys.getenv("RUN_SMOKE_TEST","false"))    %in% c("1","true","yes")

CHUNK_SIZE         <- as.integer(Sys.getenv("CHUNK_SIZE","800"))
CHUNK_OFFSET       <- as.integer(Sys.getenv("CHUNK_OFFSET","0"))
ORDER_DIR          <- "ASC"  # stable iteration by tweet_id

# Resumability budgets (stop early; workflow will auto-continue)
MAX_ROWS_PER_RUN   <- as.integer(Sys.getenv("MAX_ROWS_PER_RUN","1200"))
MAX_MINUTES        <- as.numeric(Sys.getenv("MAX_MINUTES","110"))
t0 <- Sys.time()

if (!nzchar(NOTION_TOKEN) || !nzchar(DB_ID)) stop("Set NOTION_TOKEN and NOTION_DATABASE_ID.")

# --- helpers -----------------------------------------------------------------
`%||%` <- function(x, y) if (is.null(x) || is.na(x) || x == "") y else x

rtxt <- function(x) {
  s <- as.character(x %||% "")
  if (identical(s, "")) list()
  else list(list(type="text", text=list(content=substr(s, 1, 1800))))
}

perform <- function(req, tag = "", max_tries = 6, base_sleep = 0.5) {
  last <- NULL
  for (i in seq_len(max_tries)) {
    resp <- tryCatch(req_perform(req), error = function(e) e)
    last <- resp
    if (inherits(resp, "httr2_response")) {
      sc <- resp$status_code
      if (sc != 429 && sc < 500) return(resp)
      ra <- resp_headers(resp)[["retry-after"]]
      wait <- if (!is.null(ra)) suppressWarnings(as.numeric(ra)) else base_sleep * 2^(i - 1)
      Sys.sleep(min(wait, 10))
    } else {
      Sys.sleep(base_sleep * 2^(i - 1))
    }
  }
  err <- list(
    tag = tag,
    status = if (inherits(last, "httr2_response")) last$status_code else NA_integer_,
    body = if (inherits(last, "httr2_response")) tryCatch(resp_body_string(last), error = function(...) "<no body>")
    else paste("R error:", conditionMessage(last))
  )
  structure(list(.err = TRUE, err = err), class = "notion_err")
}
is_err   <- function(x) inherits(x, "notion_err") && isTRUE(x$.err %||% TRUE)
show_err <- function(x, row_i = NA, tweet_id = NA) {
  if (!is_err(x)) return(invisible())
  er <- x$err
  cat(sprintf("⚠️ Notion error%s%s [%s] Status: %s\nBody: %s\n",
              if (!is.na(row_i)) paste0(" on row ", row_i) else "",
              if (!is.na(tweet_id)) paste0(" (tweet_id=", tweet_id, ")") else "",
              er$tag %||% "request",
              as.character(er$status %||% "n/a"),
              er$body %||% "<empty>"))
}

notion_req <- function(url) {
  request(url) |>
    req_headers(
      Authorization    = paste("Bearer", NOTION_TOKEN),
      "Notion-Version" = NOTION_VERSION,
      "Content-Type"   = "application/json"
    )
}

# --- Notion schema -----------------------------------------------------------
get_db_schema <- function() {
  resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID)) |> perform(tag="GET /databases")
  if (is_err(resp)) { show_err(resp); stop("Could not read Notion database schema.") }
  resp_body_json(resp, simplifyVector = FALSE)
}
.DB <- get_db_schema()
PROPS <- .DB$properties
TITLE_PROP <- names(Filter(function(p) identical(p$type, "title"), PROPS))[1]
if (is.null(TITLE_PROP)) stop("This Notion database has no Title (Name) property.")
if (DUMP_SCHEMA) {
  cat("\n--- Notion schema ---\n")
  cat(paste(
    vapply(names(PROPS), function(n) sprintf("%s : %s", n, PROPS[[n]]$type), character(1)),
    collapse = "\n"
  ), "\n----------------------\n")
}

to_num <- function(x) {
  if (inherits(x, "integer64")) x <- as.character(x)
  v <- suppressWarnings(as.numeric(x))
  if (is.na(v) && NUM_NA_AS_ZERO) v <- 0
  v
}

set_prop <- function(name, value) {
  p <- PROPS[[name]]; if (is.null(p)) return(NULL)
  tp <- p$type
  if (tp == "title") {
    list(title = list(list(type="text", text=list(content=as.character(value %||% "untitled")))))
  } else if (tp == "rich_text") {
    list(rich_text = rtxt(value))
  } else if (tp == "number") {
    v <- to_num(value); if (!is.finite(v)) return(NULL)
    list(number = v)
  } else if (tp == "select") {
    v <- as.character(value %||% ""); if (!nzchar(v)) return(NULL)
    list(select = list(name = v))
  } else NULL
}

props_from_row <- function(r) {
  pr <- list()
  # Title = tweet_id
  ttl <- as.character(r$tweet_id %||% "untitled")
  pr[[TITLE_PROP]] <- set_prop(TITLE_PROP, ttl)
  
  wanted <- c(
    "tweet_id", "ave_sentiment", "sentiment", "anger", "anticipation",
    "disgust", "fear", "joy", "sadness", "surprise", "trust", "clean_text"
  )
  for (nm in wanted) {
    if (!is.null(PROPS[[nm]]) && !is.null(r[[nm]])) pr[[nm]] <- set_prop(nm, r[[nm]])
  }
  pr
}

find_page_by_title_eq <- function(val) {
  body <- list(filter = list(property = TITLE_PROP, title = list(equals = as.character(val %||% ""))),
               page_size = 1)
  resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID, "/query")) |>
    req_body_json(body, auto_unbox = TRUE) |>
    perform(tag="POST /databases/query")
  if (is_err(resp)) return(structure(NA_character_, class="notion_err", .err=TRUE, err=resp$err))
  out <- resp_body_json(resp, simplifyVector = TRUE)
  if (length(out$results)) out$results$id[1] else NA_character_
}

# --- Minimal Notion index (by tweet_id and by_title) -------------------------
build_index_for_rows <- function(rows) {
  by_tid <- new.env(parent=emptyenv())
  by_ttl <- new.env(parent=emptyenv())
  
  tids <- unique(na.omit(as.character(rows$tweet_id))); tids <- tids[nzchar(tids)]
  run_chunk <- function(tids_chunk) {
    ors <- list()
    if (!is.null(PROPS$tweet_id)) {
      if (identical(PROPS$tweet_id$type, "rich_text")) {
        for (t in tids_chunk) ors[[length(ors)+1]] <- list(property="tweet_id", rich_text=list(equals=t))
      } else if (identical(PROPS$tweet_id$type, "number")) {
        for (t in tids_chunk) {
          num <- suppressWarnings(as.numeric(t))
          if (is.finite(num)) ors[[length(ors)+1]] <- list(property="tweet_id", number=list(equals=num))
        }
      }
    }
    if (!length(ors)) return(invisible(NULL))
    body <- list(filter = list(or = ors), page_size = 100)
    resp <- notion_req(paste0("https://api.notion.com/v1/databases/", DB_ID, "/query")) |>
      req_body_json(body, auto_unbox = TRUE) |>
      perform(tag="POST /databases/query (lazy)")
    if (is_err(resp)) { show_err(resp); return(invisible(NULL)) }
    out <- resp_body_json(resp, simplifyVector = FALSE)
    
    for (pg in out$results) {
      pid <- pg$id
      # title
      tnodes <- pg$properties[[TITLE_PROP]]$title
      ttl <- if (length(tnodes)) paste0(vapply(tnodes, \(x) x$plain_text %||% "", character(1L)), collapse = "") else ""
      if (nzchar(ttl)) by_ttl[[ttl]] <- pid
      # tweet_id
      if (!is.null(PROPS$tweet_id)) {
        if (identical(PROPS$tweet_id$type, "rich_text")) {
          rt <- pg$properties$tweet_id$rich_text
          tid <- if (length(rt)) paste0(vapply(rt, \(x) x$plain_text %||% "", character(1L)), collapse = "") else ""
          if (nzchar(tid)) by_tid[[tid]] <- pid
        } else if (identical(PROPS$tweet_id$type, "number")) {
          num <- pg$properties$tweet_id$number %||% NA
          if (!is.na(num)) by_tid[[as.character(num)]] <- pid
        }
      }
    }
  }
  
  i <- 1L
  while (i <= length(tids)) {
    t_slice <- if (length(tids)) tids[i:min(i+49, length(tids))] else character()
    run_chunk(t_slice)
    i <- i + 50L
    Sys.sleep(RATE_DELAY_SEC/2)
  }
  
  list(by_tid=by_tid, by_title=by_ttl)
}

# --- CRUD --------------------------------------------------------------------
create_page <- function(pr) {
  body <- list(parent = list(database_id = DB_ID), properties = pr)
  resp <- notion_req("https://api.notion.com/v1/pages") |>
    req_body_json(body, auto_unbox = TRUE) |>
    perform(tag="POST /pages")
  if (is_err(resp)) return(structure(NA_character_, class="notion_err", .err=TRUE, err=resp$err))
  resp_body_json(resp, simplifyVector = TRUE)$id
}
update_page <- function(page_id, pr) {
  resp <- notion_req(paste0("https://api.notion.com/v1/pages/", page_id)) |>
    req_method("PATCH") |>
    req_body_json(list(properties = pr), auto_unbox = TRUE) |>
    perform(tag="PATCH /pages/:id")
  if (is_err(resp)) return(structure(FALSE, class="notion_err", .err=TRUE, err=resp$err))
  TRUE
}

# --- Upsert (tweet_id → title fallback) --------------------------------------
upsert_row <- function(r, idx = NULL) {
  title_val <- as.character(r$tweet_id %||% "untitled")
  pr_full   <- props_from_row(r)
  
  pid <- NA_character_
  if (!is.null(idx)) {
    tid_chr <- as.character(r$tweet_id %||% "")
    if (!is.null(PROPS$tweet_id) && nzchar(tid_chr)) {
      pid <- idx$by_tid[[tid_chr]]; if (is.null(pid)) pid <- NA_character_
    }
    if (is.na(pid) || is.null(pid)) {
      pid <- idx$by_title[[title_val]]; if (is.null(pid)) pid <- NA_character_
    }
  } else {
    pid <- find_page_by_title_eq(title_val)
  }
  
  if (!is.na(pid[1])) {
    ok <- update_page(pid, pr_full)
    return(is.logical(ok) && ok)
  }
  
  pid2 <- create_page(pr_full)
  if (!is.na(pid2[1])) return(TRUE)
  
  pr_min <- list(); pr_min[[TITLE_PROP]] <- set_prop(TITLE_PROP, title_val)
  pid3 <- create_page(pr_min)
  if (is.na(pid3[1])) return(FALSE)
  ok2 <- update_page(pid3, pr_full)
  is.logical(ok2) && ok2
}

# --- Supabase connection -----------------------------------------------------
supa_host <- Sys.getenv("SUPABASE_HOST")
supa_user <- Sys.getenv("SUPABASE_USER")
supa_pwd  <- Sys.getenv("SUPABASE_PWD")
if (!nzchar(supa_host) || !nzchar(supa_user) || !nzchar(supa_pwd)) stop("Set SUPABASE_HOST, SUPABASE_USER, SUPABASE_PWD.")

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = supa_host,
  port = as.integer(Sys.getenv("SUPABASE_PORT", "5432")),
  dbname = as.character(Sys.getenv("SUPABASE_DB", "postgres")),
  user = supa_user,
  password = supa_pwd,
  sslmode = "require"
)

# No date column → always read all rows, but page using LIMIT/OFFSET
base_where  <- "TRUE"

# Expected DISTINCT tweets
exp_sql <- sprintf("
  SELECT COUNT(*) AS n FROM (
    SELECT DISTINCT CAST(tweet_id AS TEXT) AS key
    FROM twitter_raw_plus_sentiment
    WHERE %s
  ) t
", base_where)
expected_raw <- DBI::dbGetQuery(con, exp_sql)$n[1]
expected_num <- suppressWarnings(as.numeric(expected_raw))
if (!is.finite(expected_num)) expected_num <- 0
expected_i <- as.integer(round(expected_num))
message(sprintf("Expected distinct tweets under this filter: %d", expected_i))

# Optional smoke test
if (RUN_SMOKE_TEST) {
  smoke_title <- paste0("ping ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"))
  smoke_pid <- create_page(setNames(list(set_prop(TITLE_PROP, smoke_title)), TITLE_PROP))
  if (is_err(smoke_pid) || is.na(smoke_pid[1])) {
    cat("\n🔥 Smoke test FAILED — cannot create even a minimal page in this database.\n",
        "Confirm the integration has access to THIS database.\n", sep = "")
    if (is_err(smoke_pid)) show_err(smoke_pid)
    DBI::dbDisconnect(con); quit(status = 1L, save = "no")
  } else {
    notion_req(paste0("https://api.notion.com/v1/pages/", smoke_pid)) |>
      req_method("PATCH") |>
      req_body_json(list(archived = TRUE), auto_unbox = TRUE) |>
      perform(tag="ARCHIVE ping")
  }
}

# Optional one-row inspector
if (INSPECT_FIRST_ROW) {
  test_q <- sprintf("
    WITH ranked AS (
      SELECT s.*,
             ROW_NUMBER() OVER (
               PARTITION BY CAST(s.tweet_id AS TEXT)
               ORDER BY CAST(s.tweet_id AS TEXT) %s
             ) AS rn
      FROM twitter_raw_plus_sentiment s
      WHERE %s
    )
    SELECT
      tweet_id, ave_sentiment, sentiment, anger, anticipation,
      disgust, fear, joy, sadness, surprise, trust, clean_text
    FROM ranked
    WHERE rn = 1
    ORDER BY CAST(tweet_id AS TEXT) %s
    LIMIT 1 OFFSET %d
  ", ORDER_DIR, base_where, ORDER_DIR, CHUNK_OFFSET)
  one <- DBI::dbGetQuery(con, test_q)
  if (nrow(one)) {
    # print(one) # uncomment for debugging
    # explain_props(one[1, , drop = FALSE]) # if you have that helper
  }
}

# --- Main loop: page through Supabase ----------------------------------------
offset <- CHUNK_OFFSET
total_success <- 0L
total_seen    <- 0L

repeat {
  qry <- sprintf("
    WITH ranked AS (
      SELECT s.*,
             ROW_NUMBER() OVER (
               PARTITION BY CAST(s.tweet_id AS TEXT)
               ORDER BY CAST(s.tweet_id AS TEXT) %s
             ) AS rn
      FROM twitter_raw_plus_sentiment s
      WHERE %s
    )
    SELECT
      tweet_id, ave_sentiment, sentiment, anger, anticipation,
      disgust, fear, joy, sadness, surprise, trust, clean_text
    FROM ranked
    WHERE rn = 1
    ORDER BY CAST(tweet_id AS TEXT) %s
    LIMIT %d OFFSET %d
  ", ORDER_DIR, base_where, ORDER_DIR, CHUNK_SIZE, offset)
  
  rows <- DBI::dbGetQuery(con, qry)
  n <- nrow(rows)
  if (!n) break
  
  message(sprintf("Fetched %d rows (offset=%d). CHUNK_SIZE=%d", n, offset, CHUNK_SIZE))
  
  # Build a tiny Notion index just for this page of rows
  idx <- build_index_for_rows(rows)
  
  success <- 0L
  for (i in seq_len(n)) {
    r <- rows[i, , drop = FALSE]
    message(sprintf("Upserting tweet_id=%s", as.character(r$tweet_id)))
    ok <- upsert_row(r, idx = idx)
    if (ok) success <- success + 1L else message(sprintf("Row %d failed (tweet_id=%s)", i, as.character(r$tweet_id)))
    if (i %% 50 == 0) message(sprintf("Processed %d/%d in this page (ok %d)", i, n, success))
    Sys.sleep(RATE_DELAY_SEC)
  }

  total_success <- total_success + success
  total_seen    <- total_seen + n
  offset        <- offset + n

  message(sprintf("Page done. %d/%d upserts ok (cumulative ok %d, seen %d of ~%d).",
                  success, n, total_success, total_seen, expected_i))

  # --- stop early to avoid hard timeout & chain next run
  elapsed_min <- as.numeric(difftime(Sys.time(), t0, units = "mins"))
  if (total_seen >= MAX_ROWS_PER_RUN || elapsed_min >= MAX_MINUTES) {
    message(sprintf("Stopping early (seen=%d, elapsed=%.1f min). Next offset = %d",
                    total_seen, elapsed_min, offset))
    go <- Sys.getenv("GITHUB_OUTPUT")
    if (nzchar(go)) write(paste0("next_offset=", offset), file = go, append = TRUE)
    DBI::dbDisconnect(con)
    quit(status = 0, save = "no")
  }
}

DBI::dbDisconnect(con)
message(sprintf("All pages done. Upserts ok: %d. Expected distinct under filter: %d", total_success, expected_i))

# Tell the workflow we’re finished
go <- Sys.getenv("GITHUB_OUTPUT")
if (nzchar(go)) write("next_offset=done", file = go, append = TRUE)




