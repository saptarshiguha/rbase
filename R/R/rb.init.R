#' Initialiazes rbase
#'
#' Returns an adminstrative object of class rbadmin
#'
#' @param requestAdmin Set to TRUE and returns the adminsitrator java object if required, and usually is so.
#' @param otherConfigs A character vector of other configuration files which are loaded after the default config files
#' @details Call this function first and store the result, else it will get garbage collected.
#' @keywords MapReduce Map
#' @export
rb.init <- function(requestAdmin=TRUE,otherConfigs=NULL,HBASE.HOME="/usr/lib/hbase",HADOOP.HOME="/usr/lib/hadoop"
                    ,HADOOP.CONF = sprintf("%s/conf",HADOOP.HOME)
                    ,HBASE.CONF  = sprintf("%s/conf",HBASE.HOME)
                    ,rhipeJar    = list.files(paste(system.file(package="Rhipe"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)
                    ,rbaseJar    = list.files(paste(system.file(package="rbase"),"java",sep=.Platform$file.sep),pattern="jar$",full=T)){
  hadoopJars <- list.files(HADOOP.HOME,pattern="jar$",full.names=TRUE,rec=TRUE)
  hbaseJars  <- list.files(HBASE.HOME,pattern="jar$",full.names=TRUE,rec=TRUE)
  hadoopConf <- list.files(HADOOP.CONF,pattern="-site.xml$",full.names=TRUE,rec=TRUE)
  hbaseConf  <- list.files(HBASE.CONF,pattern="-site.xml$",full.names=TRUE,rec=TRUE)
  ## order is important, HBASE has older protobuf jars
  .jinit()
  jars <- c(HADOOP.CONF, HBASE.CONF,rhipeJar,rbaseJar,hadoopJars,hbaseJars)
  .jaddClassPath(jars)
  if(requestAdmin){
    f <- if(!is.null(otherConfigs) && is.character(otherConfigs))
      .jnew("org/godhuli/rhipe/Rbase/Admin",otherConfigs) else .jnew("org/godhuli/rhipe/Rbase/Admin")
  }else f <- NULL
  .jcheck()
  f
}
