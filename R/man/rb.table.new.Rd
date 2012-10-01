\name{rb.table.new}
\alias{rb.table.new}
\title{Create a new table}
\usage{
  rb.table.new(ra, tablename, ...)
}
\arguments{
  \item{ra}{is the object returned \link{\code{rb.init}}}

  \item{tablename}{is the name of the new table}

  \item{...}{the table specs}
}
\description{
  Create a new table
}
\examples{
\dontrun{
  ra = rb.init()
  rb.table.new(ra,"anewtable",x,y=list (blockCacheEnabled=TRUE,maxversions=10))
  rb.table.new(ra,"anewtable",x,y=list(maxversions=1))
}
}

