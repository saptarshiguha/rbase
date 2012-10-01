\name{rb.columns.add}
\alias{rb.columns.add}
\title{Add another column to a table (column should not be present)}
\usage{
  rb.columns.add(ra, tablename, ...)
}
\arguments{
  \item{ra}{Object returned from rb.init}

  \item{tablename}{is the name of the table}

  \item{...}{the structure of the columns as given in
  \link{\code{rb.table.new}}}
}
\description{
  Add another column to a table (column should not be
  present)
}
\examples{
\dontrun{
  rb.columns.add(ra,"anewtable",x=list(maxversions=1),z=list(blockCacheEnabled=TRUE,maxversions=10))
}
}

