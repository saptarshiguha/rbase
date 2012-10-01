putOne <- function(ra,tb,key, values,sz=rbsz,kszf=sz){
  ## values = list("f1:c1"=,"f2:c2"=,...)
  .jcall(ra,"V","putOne",tb, .jarray(kszf(key),"[B"), names(values),  .jarray(lapply(values, function(r) .jarray(sz(r))),"[B"))
  .jcheck()
}
putMany <- function(ra,tb,key,values,sz,kszf){
  ksz <- .jarray(lapply(key, function(r) .jarray(kszf(r))),"[B")
  cfs <- .jarray(lapply(values, function(r) .jarray(names(r))),"[Ljava/lang/String;")
  varray <- .jarray(lapply(values, function(r) .jarray(lapply(r, function(s) .jarray(sz(s))),"[B")),"[[B")
  .jcall(ra,"V","putMany",tb, ksz, cfs, varray)
  .jcheck()
}
get <- function(ra, tb1, key, what,usz=TRUE,un,sz, kszf){
  ksz <- .jarray(lapply(as.list(key), function(r) .jarray(kszf(r))),"[B")
  what <- if(!is.list(what)) rep(list(what), length.out=length(key)) else what
  cfs <- .jarray(lapply(what, function(r) .jarray(r)),"[Ljava/lang/String;")
  a <- .jcall(ra,"[B","getMany",tb1, ksz,cfs)
  if(usz)
    un(a)
}
getUntransformed <- function(ra, tb1, key, what,usz=TRUE){
  get(ra,tbl, key, what, usz, kszf = function(i) i)
}

#' Connect to a HBase Table (and then add/delete/get rows)
#' @param ra is the object returned from \link{\code{rb.init}}
#' @param tablename obvious
#' @return An object that has methods e.. putOne (to put one object, putMany, get and the table object itself)
#' @examples
#' Currently, putOne needs at least two names in the values
#' \dontrun{
#' rb.table.new(ra,"anewtable",x,y=list(blockCacheEnabled=TRUE,maxversions=10))
#' tb = rb.table.connect(ra,"anewtable")
#' tb$get("b",c("y:f","y:c","y:w"))
#' tb$get("b","y:")
#' tb$get(c("-x","b"),c("y:c","y:f"))
#' tb$get(c("-x","b"),c("y:","x"))
#' 
#' tb$putOne( key="b", values=list("y:f"=TRUE))
#' tb$putOne( key="b", values=list("y:f"=TRUE, "y:c"=10,"x:w"=20))
#' tb$putOne( key="c", values=list("y:f"=FALSE, "y:w"=20))
#' 
#' tb$putMany( key=list("b","c")
#'            ,values=list(
#'                list("y:x"=runif(1), "y:j"=runif(100))
#'               ,list("y:f"=runif(1), "y:c"=runif(100)))
#'            )
#' Running in MapReduce (needs RHIPE)
#' setup <- expression(map={
#'  library(rJava)
#'  library(rbase)
#'  ra <- rb.init()
#'  tb <- rb.table.connect(ra,"anewtable")
#' })
#' j <- rhwatch(rhmr(
#'                   map=rhmap(after={
#'                     tb$putMany(map.keys, values=lapply(map.values,function(r) list("y:a"=r,"y:b"=runif(1))))
#'                   })
#'                   ,reduce=0
#'                   ,setup=setup
#'                   ,N=1e6,mapred=list(mapred.map.tasks=5000))
#' }
#' @export
rb.table.connect <- function(ra,tablename){
  tb <- .jnew("org/apache/hadoop/hbase/client/HTable",tablename)
  j <- function(ra,tb){
    return(list(putOne   = function(key, values, sz=rhsz, kzsf=sz)     putOne(ra=ra,tb=tb,key=key,values,sz,kzsf)
                ,putMany = function(key, values,sz=rhsz,kzsf=sz)    putMany(ra=ra,tb=tb,key=key,values,sz,kzsf)
                ,get     = function(key, what,usz=TRUE, un=rhuz, sz=rhsz, kzsf=sz) get(ra=ra,tb=tb,key=key,what,usz, un, sz, kzsf)
                ,table   = tb))
  }
  return(j(ra,tb))
}

