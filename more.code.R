ra <- rb.init()

rb.list.tables(ra)
rb.table.info("crash_reports")

rb.delete(ra,"anewtable")
rb.table.new(ra,"anewtable",x,y=list(blockCacheEnabled=TRUE,maxversions=10))
rb.table.info("anewtable")

rb.columns.add(ra,"anewtable",x=list(maxversions=1),z=list(blockCacheEnabled=TRUE,maxversions=10))
rb.columns.delete(ra,"anewtable",x)

source("~/tmp/example.R.code.R")
ra <- rb.init()
tb = rb.table.connect(ra,"anewtable")
tb$get("b",c("y:f","y:c","y:w"))
tb$get("b","y:")
tb$get(c("-x","b"),c("y:c","y:f"))
tb$get(c("-x","b"),c("y:","x"))

tb$putOne( key="b", values=list("y:f"=TRUE)) ## Does not work
tb$putOne( key="b", values=list("y:f"=TRUE, "y:c"=10,"x:w"=20))
tb$putOne( key="c", values=list("y:f"=FALSE, "y:w"=20))

tb$putMany( key=list("b","c")
           ,values=list(
               list("y:x"=runif(1), "y:j"=runif(100))
              ,list("y:f"=runif(1), "y:c"=runif(100)))
           )




## ## test java and .jinit
## setup <- expression(map={
##   library(rJava)
##   .jinit()
## }) 
## j <- rhwatch(rhmr(map=rhmap({
##   rhcollect(1,Sys.getenv("LD_LIBRARY_PATH"))
## }), reduce=0,setup=setup, N=1))

## Example of Above! It Works! and crashed when i try to write 1e6 values ....
## but doesn't crash if i use PutMany
rhput("~/tmp/rbase.jar","/user/sguha/tmp")
setup <- expression(map={
 library(rJava)
 rhsz <- function(x) .Call("rh_sz",x)
 rhuz <- function(x) .Call("rh_uz",x)
 ra <- rb.init(rbaseJar="./rbase.jar")
 tb <- rb.table.connect(ra,"anewtable")
}) 
j <- rhwatch(rhmr(
                  map=rhmap(after={
                    tb$putMany(map.keys, values=lapply(map.values,function(r) list("y:a"=r,"y:b"=runif(1))))
                  })
                  ,reduce=0
                  ,setup=setup
                  ,N=1e6,mapred=list(mapred.map.tasks=5000)
                  ,param=alist(rb.init, rb.table.connect, putOne,putMany,get)
                  ,shared=c("/user/sguha/tmp/rbase.jar")))

rapply(tb$getOne(1,"y:"),rhuz)
## lapply(1:1e6,function(k){
##   tb$putOne(key=k, values=list("y:a"=k,"y:b"=runif(1)))
## })
## setup <- expression(map={
##  library(rJava)
##   .jinit()
##  HBASE.HOME  = "/usr/lib/hbase"
##  HADOOP.HOME = "/usr/lib/hadoop"
##  HADOOP.CONF = sprintf("%s/conf",HADOOP.HOME)
##  HBASE.CONF  = sprintf("%s/conf",HBASE.HOME)
##  rhipeJar <- list.files(paste(system.file(package="Rhipe"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)
##  hadoopJars <- list.files(HADOOP.HOME,pattern="jar$",full.names=TRUE,rec=TRUE)
##  hbaseJars  <- list.files(HBASE.HOME,pattern="jar$",full.names=TRUE,rec=TRUE)
##  hadoopConf <- list.files(HADOOP.CONF,pattern="-site.xml$",full.names=TRUE,rec=TRUE)
##  hbaseConf  <- list.files(HBASE.CONF,pattern="-site.xml$",full.names=TRUE,rec=TRUE)
##  jars <- c(HADOOP.CONF, HBASE.CONF,rhipeJar,"./rbase.jar",hadoopJars,hbaseJars)
##  .jaddClassPath(jars)
##  rbadmin <-.jnew("org/godhuli/rhipe/Rbase/Admin")
## }) 
## j <- rhwatch(rhmr(map=rhmap({
##   rhcollect(1,list(.jclassPath(),list.files(".",full.names=TRUE),capture.output(rbadmin),system.file(package="Rhipe")))
##   rhcollect(2,rbadmin$listTables()) 
## }), reduce=0,setup=setup, N=1,shared=c("/user/sguha/tmp/rbase.jar")))
