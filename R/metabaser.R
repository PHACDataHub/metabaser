#' @importFrom magrittr %>%
NULL

#' Assemble URL
#' @keywords internal
build_url <- function(base_url = Sys.getenv("METABASE_BASE_URL"), path) {
    paste(base_url, path, sep = "/")
}

#' Setup Metabase connection
#'
#' Sets environment variables for the connection to Metabase.
#'
#' @param base_url Base URL for the Metabase API
#' @param database_id Database ID to connect to
#' @param creds_file File containing Metabase account credentials to connect with
#' @export
metabase_setup <- function(base_url, database_id, creds_file = "~/metabase_creds") {
    Sys.setenv(
        METABASE_BASE_URL = base_url,
        METABASE_DATABASE_ID = database_id,
        METABASE_CREDS_FILE = creds_file
    )
}

#' Login to Metabase
#'
#' This will login to the Metabase API with the username and password specified in the given file.
#' The file should have "username=username" on one line and "password=password" on the next.
#' If the login is successful, a cookie will be set behind the scense with a session ID for all future requests.
#' If \code{\link{metabase_setup}} was used, \code{metabase_login} will be automatically configured with a creds_file.
#'
#' @param base_url Base URL for the Metabase API
#' @param database_id Database ID to connect to
#' @param creds_file File containing Metabase account credentials to connect with
#' @param auto_setup If TRUE, will automatically setup the connection before login,
#' if FALSE, requires \code{\link{metabase_setup}} to be executed before
#' @export
metabase_login <- function(base_url, database_id, creds_file, auto_setup = TRUE) {
    if (auto_setup)
        metabase_setup(base_url = base_url, database_id = database_id, creds_file = creds_file)
    else
        creds_file <- Sys.getenv("METABASE_CREDS_FILE")
    creds <- stringr::str_split(readr::read_lines(creds_file), "=", simplify = TRUE)[,2]
    username <- creds[1]
    password <- creds[2]

    resp <- httr::POST(
        url = build_url(path = "session"),
        body = list(username = username, password = password),
        encode = "json"
    )

    if (httr::http_error(resp)) {
        stop(
            stringr::str_glue(
                "Metabase login failed [{httr::status_code(resp)}]",
                unlist(httr::content(resp)$errors),
                .sep = "\n"
            )
        )
    } else {
        message(
            stringr::str_glue(
                "Metabase login successful for {username} !"
            )
        )
    }
}

#' Logout of Metabase
#'
#' Logs out the user by sending a request to delete the user session.
#' @export
metabase_logout <- function() {
    resp <- httr::DELETE(
        url = build_url(path = "session")
    )

    if (httr::http_error(resp)) {
        stop(
            stringr::str_glue(
                "Metabase logout failed [{httr::status_code(resp)}]",
                httr::content(resp, type = "text", encoding = "UTF-8"),
                .sep = "\n"
            )
        )
    } else {
        message(
            "Metabase logout successful!"
        )
    }
}

#' Check Metabase status
#' @return TRUE if a session is active with a logged-in user, FALSE otherwise.
#' @export
metabase_status <- function() {
    session_id <- httr::handle_find("https://atlas-metabase.hres.ca/api") %>%
        httr::cookies() %>%
        dplyr::filter(name == "metabase.SESSION") %>%
        dplyr::pull(value)
    if (length(session_id) == 0) return(FALSE)
    !is.na(session_id)
}

#' Query Metabase
#'
#' Sends an SQL query to Metabase for the given database.
#' Data is limited to 2000 records by the server using this approach.
#' This might be faster for small lookups of the DB.
#'
#' @param sql_query SQL query to execute
#' @param database_id Database ID to query, will be set by \code{\link{metabase_setup}}.
#'
#' @return data.frame containing the results of the query
#' @export
metabase_query2 <- function(sql_query, database_id = Sys.getenv("METABASE_DATABASE_ID")) {
    if (!metabase_status()) stop("No connection to Metabase.")
    database_id <- as.integer(database_id)
    resp <- httr::POST(
        url = build_url(path = "dataset"),
        body = list(database = database_id,
                    native = list(query = sql_query),
                    type = "native"),
        encode = "json"
    )
    parsed <- jsonlite::fromJSON(httr::content(resp, as = "text", encoding = "UTF-8"))
    if (parsed$status == "failed") {
        stop(
            stringr::str_glue(
                "Metabase query failed.",
                parsed$error,
                .sep = "\n"
            )
        )
    } else if (parsed$status == "completed") {
        data <- parsed$data$rows
        colnames(data) <- parsed$data$cols$name
        dplyr::as_tibble(data)
    }
}

#' Query Metabase
#'
#' Sends an SQL query to Metabase for the given database.
#' Data is limited to 1 million records by the server using this approach.
#' This might be faster for small lookups of the DB.
#'
#' @param sql_query SQL query to execute
#' @param database_id Database ID to query, will be set by \code{\link{metabase_setup}}.
#'
#' @return data.frame containing the results of the query
#' @export
metabase_query <- function(sql_query, database_id = Sys.getenv("METABASE_DATABASE_ID")) {
    if (!metabase_status()) stop("No connection to Metabase.")
    database_id <- as.integer(database_id)
    resp <- httr::POST(
        url = build_url(path = "dataset/csv"),
        body = list(query = jsonlite::toJSON(list(
            database = 17,
            native = list(query = sql_query),
            type = "native"),
            auto_unbox = TRUE)),
        encode = "form",
        httr::content_type("application/x-www-form-urlencoded")
    )
    # api will return json instead of csv if there is an error so check if we have json
    resp_type <- ifelse(rawToChar(httr::content(resp, as = "raw")[1]) == "{", "json", "csv")
    if (resp_type == "json") {
        parsed <- jsonlite::fromJSON(httr::content(resp, as = "text", encoding = "UTF-8"))
        if (parsed$status == "failed") {
            stop(
                stringr::str_glue(
                    "Metabase query failed.",
                    parsed$error,
                    .sep = "\n"
                )
            )
        } else {
            stop("Incorrect format returned by Metabase (json instead of csv).")
        }
    } else if (resp_type == "csv") {
        readr::read_csv(httr::content(resp, as = "text", encoding = "UTF-8"))
    }
}
