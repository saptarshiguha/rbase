\name{rb.columns.modify}
\alias{rb.columns.modify}
\title{Modify  column in a table (already present)}
\usage{
  rb.columns.modify(ra, tablename, ...)
}
\arguments{
  \item{ra}{Object returned from rb.init}

  \item{tablename}{is the name of the table}

  \item{...}{the structure of the columns as given in
  \link{\code{rb.table.new}}}
}
\description{
  Modify column in a table (already present)
}
\examples{
\dontrun{
  rb.columns.modify(ra,"anewtable",x=list(maxversions=11),z=list(blockCacheEnabled=FALSE,maxversions=10))
}
}

