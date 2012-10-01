\name{rb.table.connect}
\alias{rb.table.connect}
\title{Connect to a HBase Table (and then add/delete/get rows)}
\usage{
  rb.table.connect(ra, tablename)
}
\arguments{
  \item{ra}{is the object returned from
  \link{\code{rb.init}}}

  \item{tablename}{obvious}
}
\value{
  An object that has methods e.. putOne (to put one object,
  putMany, get and the table object itself)
}
\description{
  Connect to a HBase Table (and then add/delete/get rows)
}
\examples{
Currently, putOne needs at least two names in the values
\dontrun{
rb.table.new(ra,"anewtable",x,y=list(blockCacheEnabled=TRUE,maxversions=10))
tb = rb.table.connect(ra,"anewtable")
tb$get("b",c("y:f","y:c","y:w"))
tb$get("b","y:")
tb$get(c("-x","b"),c("y:c","y:f"))
tb$get(c("-x","b"),c("y:","x"))

tb$putOne( key="b", values=list("y:f"=TRUE))
tb$putOne( key="b", values=list("y:f"=TRUE, "y:c"=10,"x:w"=20))
tb$putOne( key="c", values=list("y:f"=FALSE, "y:w"=20))

tb$putMany( key=list("b","c")
           ,values=list(
               list("y:x"=runif(1), "y:j"=runif(100))
              ,list("y:f"=runif(1), "y:c"=runif(100)))
           )
Running in MapReduce (needs RHIPE)
setup <- expression(map={
 library(rJava)
 library(rbase)
 ra <- rb.init()
 tb <- rb.table.connect(ra,"anewtable")
})
j <- rhwatch(rhmr(
                  map=rhmap(after={
                    tb$putMany(map.keys, values=lapply(map.values,function(r) list("y:a"=r,"y:b"=runif(1))))
                  })
                  ,reduce=0
                  ,setup=setup
                  ,N=1e6,mapred=list(mapred.map.tasks=5000))
}
}

