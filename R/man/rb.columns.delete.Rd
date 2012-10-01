\name{rb.columns.delete}
\alias{rb.columns.delete}
\title{Delete another column to a table (column should be present)}
\usage{
  rb.columns.delete(ra, tablename, ...)
}
\arguments{
  \item{ra}{Object returned from rb.init}

  \item{tablename}{is the name of the table}

  \item{...}{the structure of the columns as given in
  \link{\code{rb.table.new}}}
}
\description{
  Delete another column to a table (column should be
  present)
}
\examples{
\dontrun{
  rb.columns.delete(ra,"anewtable",x)
}
}

