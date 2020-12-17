#' @importFrom magrittr %>%
NULL

#' Assemble URL
#'
#' @param handle metabase_handle object
#' @param path API endpoint path
#' @keywords internal
build_url <- function(handle, path) {
    paste(handle$base_url, path, sep = "/")
}



#' Sets a default to be used for matabaser
#'
#' typically the user will set all values at once from the function metabase_set_defaults
#'
#' @param param normally one of base_url, database_id, creds_file, username, password
#' @param value the value for which ever one is being set
metabase_set_default <- function(param, value){

    if(purrr::is_null(value)){
        return(FALSE)
    }

    if(rlang::is_na(value)){
        return(FALSE)
    }

    tryCatch({
        #print(glue::glue("param={param}"))
        #print(glue::glue("value={value}"))
        keyring::key_set_with_value("metabaser_defaults",
                                    username = param,
                                    password = as.character(value)
                                    )
        return(TRUE)



    },
    error = function(x){
                print(glue::glue("metabaser default param='{param}' did not set."))
                return(FALSE)
        },
    finally = print("")

    )

    return(TRUE)
}


#' Sets the defaults to be used for metabaser
#'
#'
#' @param base_url Base URL for the Metabase API
#' @param database_id Database ID to connect to
#' @param creds_file File containing Metabase account credentials to connect with
#' @param username Username
#' @param password Password
#' @export
metabase_set_defaults <- function(
    base_url = NA,
    database_id = NA,
    creds_file = NA,
    username = NA,
    pw = NA
){
    metabase_set_default(param = "base_url", value = base_url)
    metabase_set_default(param = "database_id", value = database_id)
    metabase_set_default(param = "creds_file", value = creds_file)
    metabase_set_default(param = "username", value = username)
    metabase_set_default(param = "password", value = pw)
}



#' gets a default to be used for metabaser
#'
#'
#' @param param normally one of base_url, database_id, creds_file, username, password
metabase_get_default <- function(param){
    tryCatch({
        keyring::key_get("metabaser_defaults", param)
        },
        error = function(x){
            print(glue::glue("metabaser default param='{param}' is missing."))
            return(NULL)
            },
        finally =
    )
}


#' Construct a Metabase API Handle
#'
#' This is constructor to make a metabase_handle object which
#' is used in subsequent functions to interact with the API.
#' The handle should not be created manually, but should be created
#' using \code{\link{metabase_login}}.
#'
#' @param base_url Base URL for the Metabase API
#' @param database_id Database ID to connect to
#' @param username Username
#' @keywords internal
metabase_handle <- function(base_url, database_id, username) {
    structure(list(
        base_url = base_url,
        database_id = as.integer(database_id),
        username = username,
        handle = httr::handle(base_url),
        token = NA,
        status = FALSE
    ), class = "metabase_handle")
}

#' @describeIn metabase_handle Check for metabase_handle
#' @keywords internal
is_metabase_handle <- function(handle) {
    inherits(handle, "metabase_handle")
}
#' Login to Metabase using default credentials
#'
#' see metabase_login for description of parameters
#'
#' @export
metabase_login_default <- function(base_url = metabase_get_default("base_url"),
                           database_id = metabase_get_default("database_id"),
                           creds_file = metabase_get_default("creds_file"),
                           username = metabase_get_default("username"),
                           password = metabase_get_default("password"),
                           ...
) {
    metabase_login(base_url = base_url,
                           database_id = database_id,
                           creds_file = creds_file,
                           username = username,
                           password = password,
                           ...)
}



#' Login to Metabase
#'
#' This will login to the Metabase API with the given username and password.
#' Credentials must be specified either directly through the \code{username} and \code{password} parameters
#' or as a file through the \code{creds_file} parameter.
#'
#' If used, the creds_file should have "username=username" on one line and "password=password" on the next.
#' If the login is successful, a \code{metabase_handle} object will be returned to use in subsequent functions.
#'
#' @param base_url Base URL for the Metabase API
#' @param database_id Database ID to connect to
#' @param creds_file File containing Metabase account credentials to connect with
#' @param username Username
#' @param password Password
#' @export
metabase_login <- function(base_url = NULL,
                           database_id = NULL,
                           creds_file = NULL,
                           username = NULL,
                           password = NULL
                           ) {
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

    handle <- metabase_handle(base_url = base_url, database_id = database_id, username = username)

    resp <- httr::POST(
        url = build_url(handle, path = "session"),
        body = list(username = username, password = password),
        encode = "json",
        handle = handle$handle
    )

    if (httr::http_error(resp)) {
        stop(stringr::str_glue("Metabase login failed [{httr::status_code(resp)}]",
                               unlist(httr::content(resp)$errors),
                               .sep = "\n"), call. = FALSE)
    }

    handle$token <- handle$handle %>%
        httr::cookies() %>%
        dplyr::filter(name == "metabase.SESSION") %>%
        dplyr::pull(value)
    handle$status <- TRUE
    message(stringr::str_glue("Metabase login successful for {username} !"))
    handle
}

#' Logout of Metabase
#'
#' Logs out the user by sending a request to delete the user session.
#'
#' @param handle metabase_handle object
#' @export
metabase_logout <- function(handle) {
    handle_name <- deparse(substitute(handle))
    stopifnot(is_metabase_handle(handle))
    if (!handle$status) {
        warning("Cannot logout. There is no active Metabase session.", call. = FALSE)
        return(invisible(NULL))
    }

    resp <- httr::DELETE(
        url = build_url(handle, path = "session"),
        handle = handle$handle
    )

    if (httr::http_error(resp)) {
        stop(stringr::str_glue("Metabase logout failed [{httr::status_code(resp)}]",
                               httr::content(resp, type = "text", encoding = "UTF-8"),
                               .sep = "\n"), call. = FALSE)
    } else {
        message("Metabase logout successful!")
        handle$status <- FALSE
        assign(handle_name, handle, pos = 1) # update handle in-place in parent env with updated status
    }
    invisible(handle) # return invisibly
}

#' Query Metabase
#'
#' Sends an SQL query to Metabase for the given database.
#' Data is limited to 2000 records by the server using this approach.
#' This might be faster for small lookups of the DB.
#'
#' @param handle metabase_handle object
#' @param sql_query SQL query to execute
#'
#' @return data.frame containing the results of the query
#' @export
metabase_query2 <- function(handle, sql_query) {
    resp <- httr::POST(
        url = build_url(handle, path = "dataset"),
        body = list(database = handle$database_id,
                    native = list(query = sql_query),
                    type = "native"),
        encode = "json"
    )
    parsed <- jsonlite::fromJSON(httr::content(resp, as = "text", encoding = "UTF-8"))
    if (parsed$status == "failed") {
        stop(stringr::str_glue("Metabase query failed.", parsed$error, .sep = "\n"), call. = FALSE)
    } else if (parsed$status == "completed") {
        data <- parsed$data$rows
        colnames(data) <- parsed$data$cols$name
        dplyr::as_tibble(data)
    }
}






#' Query Metabase
#'
#' Sends an SQL query to Metabase for the given database.
#' Data is limited to 2000 records by the server using this approach.
#' This might be faster for small lookups of the DB.
#'
#' @param sql_query SQL query to execute
#' @param handle metabase_handle object
#'
#' @return data.frame containing the results of the query
#' @export
metabase_query_sql <- function(sql_query,
                                  handle = metabase_login_default(),
                                  col_types = readr::cols(.default = readr::col_character()),
                               ...){
    metabase_query(handle = handle, sql_query = sql_query, col_types = col_types, ...)
}




#' Query Metabase
#'
#' Sends an SQL query to Metabase for the given database.
#' Data is limited to 1 million records by the server using this approach.
#' This might be faster for small lookups of the DB.
#'
#' @param handle metabase_handle object
#' @param sql_query SQL query to execute
#' @param col_types Column types to use for parsing as specified in \code{\link[readr]{read_csv}}
#'
#' @return data.frame containing the results of the query
#' @export
metabase_query <- function(handle, sql_query, col_types = NULL) {
    stopifnot(is_metabase_handle(handle))
    if (!handle$status) stop("No connection to Metabase. Try logging in again.", call. = FALSE)

    resp <- httr::POST(
        url = build_url(handle, path = "dataset/csv"),
        body = list(query = jsonlite::toJSON(list(
            database = handle$database_id,
            native = list(query = sql_query),
            type = "native"),
            auto_unbox = TRUE)),
        encode = "form",
        config = list(httr::content_type("application/x-www-form-urlencoded")),
        handle = handle$handle
    )

    # api will return json instead of csv if there is an error so check if we have json
    resp_type <- ifelse(rawToChar(httr::content(resp, as = "raw")[1]) == "{", "json", "csv")
    if (resp_type == "json") {
        parsed <- jsonlite::fromJSON(httr::content(resp, as = "text", encoding = "UTF-8"))
        if (parsed$status == "failed") {
            stop(stringr::str_glue("Metabase query failed.", parsed$error, .sep = "\n"), call. = FALSE)
        } else {
            stop("Incorrect format returned by Metabase (json instead of csv).", call. = FALSE)
        }
    } else if (resp_type == "csv") {
        readr::read_csv(httr::content(resp, as = "text", encoding = "UTF-8"), col_types = col_types)
    }
}
