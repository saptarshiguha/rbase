\name{rb.table.info}
\alias{rb.table.info}
\title{Returns information about the schema of a given table}
\usage{
  rb.table.info(tablename, getStartEndKeys = FALSE)
}
\arguments{
  \item{tablename}{is the name of a table}

  \item{getStartEndKeys}{if TRUE will return a list of
  start/end keys for every region (and this can time
  consuming)}
}
\value{
  A data frame of relevant information
}
\description{
  Returns information about the schema of a given table
}

