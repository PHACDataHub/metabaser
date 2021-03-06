% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/metabaser.R
\name{metabase_query2}
\alias{metabase_query2}
\title{Query Metabase}
\usage{
metabase_query2(handle, sql_query)
}
\arguments{
\item{handle}{metabase_handle object}

\item{sql_query}{SQL query to execute}
}
\value{
data.frame containing the results of the query
}
\description{
Sends an SQL query to Metabase for the given database.
Data is limited to 2000 records by the server using this approach.
This might be faster for small lookups of the DB.
}
