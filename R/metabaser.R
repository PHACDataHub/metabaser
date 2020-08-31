#' @importFrom magrittr %>%
NULL

#' Assemble URL
#' @keywords internal
build_url <- function(base_url = metabase_url(), path) {
    paste(base_url, path, sep = "/")
}

#' Get/Set Metabase URL
#'
#' If no argument is given, retrieves the current Metabase URL.
#' If an argument is given, sets the Metabase URL.
#'
#' @export
metabase_url <- function(url = NULL) {
    if (!is.null(url)) {
        Sys.setenv(METABASE_BASE_URL = url)
        return(invisible(url))
    } else {
        url <- Sys.getenv("METABASE_BASE_URL", unset = NA)
        if (is.na(url)) {
            warning("env var METABASE_BASE_URL is not set to the Metabse API.", call. = FALSE)
        }
    }
    url
}

#' Get/Set Metabase DB ID
#'
#' If no argument is given, retrieves the current Metabase database ID.
#' If an argument is given, sets the Metabase database ID.
#'
#' @export
metabase_db <- function(db_id = NULL) {
    if (!is.null(db_id)) {
        Sys.setenv(METABASE_DATABASE_ID = db_id)
        return(invisible(db_id))
    } else {
        db_id <- Sys.getenv("METABASE_DATABASE_ID", unset = NA)
        if (is.na(db_id)) {
            stop("env var METABASE_DATABASE_ID is not set to the ID number of the database.", call. = FALSE)
        }
    }
    db_id
}

#' Login to Metabase
#'
#' This will login to the Metabase API with the given username and password.
#' Credentials must be specified either directly through the \code{username} and \code{password} parameters
#' or as a file through the \code{creds_file} parameter.
#'
#' The file should have "username=username" on one line and "password=password" on the next.
#' If the login is successful, a cookie will be set behind the scenes with a session ID for all future requests.
#'
#' @param base_url Base URL for the Metabase API
#' @param database_id Database ID to connect to
#' @param creds_file File containing Metabase account credentials to connect with
#' @param username Username
#' @param password Password
#' if FALSE, requires \code{\link{metabase_setup}} to be executed before
#' @export
metabase_login <- function(base_url, database_id, creds_file = NULL, username = NULL, password = NULL, auto_setup = TRUE) {
    if (metabase_status()) {
        warning("Already logged-in to Metabase. Please logout before trying to login.", call. = FALSE)
        return(invisible(NULL))
    }

    metabase_url(base_url)
    metabase_db(database_id)

    if (!is.null(creds_file)) {
        creds <- stringr::str_split(readr::read_lines(creds_file), "=", simplify = TRUE)[,2]
        username <- creds[1]
        password <- creds[2]
    } else if (!is.null(username) & !is.null(password)) {
        username <- username
        password <- password
    } else {
        stop("One of creds_file or username and password must be specified.", call. = FALSE)
    }

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
            ),
        call. = FALSE)
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
    if (!metabase_status()) {
        warning("Cannot logout. There is no active Metabase session.", call. = FALSE)
        return(invisible(NULL))
    }

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
#'
#' @return TRUE if a session is active with a logged-in user, FALSE otherwise.
#' @export
metabase_status <- function() {
    base_url <- suppressWarnings(metabase_url())
    session_id <- httr::handle_find(base_url) %>%
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
#' @param database_id Database ID to query, will be set automatically upon login
#'
#' @return data.frame containing the results of the query
#' @export
metabase_query2 <- function(sql_query, database_id = metabase_db()) {
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
#' @param col_types Column types to use for parsing as specified in \code{\link[readr]{read_csv}}
#' @param database_id Database ID to query, will be set automatically upon login
#'
#' @return data.frame containing the results of the query
#' @export
metabase_query <- function(sql_query, col_types = NULL, database_id = metabase_db()) {
    if (!metabase_status()) stop("No connection to Metabase.")
    database_id <- as.integer(database_id)
    resp <- httr::POST(
        url = build_url(path = "dataset/csv"),
        body = list(query = jsonlite::toJSON(list(
            database = database_id,
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
        readr::read_csv(httr::content(resp, as = "text", encoding = "UTF-8"), col_types = col_types)
    }
}
