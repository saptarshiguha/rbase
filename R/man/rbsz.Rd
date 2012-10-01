\name{rbsz}
\alias{rbsz}
\title{Serialize using RHIPE serialization scheme}
\usage{
  rbsz(x)
}
\arguments{
  \item{x}{the R object}
}
\description{
  Serialize using RHIPE serialization scheme
}
\details{
  Not all objects can be serialized e.g. S4 objects. For
  those objects, pass rbsz \code{serialize(x,NULL)}
}

