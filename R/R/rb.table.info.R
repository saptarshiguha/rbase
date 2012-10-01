#' Returns information about the schema of a given table
#' @param tablename is the name of a table
#' @param getStartEndKeys if TRUE will return a list of start/end keys for every region (and this can time consuming)
#' @return A data frame of relevant information
#' @export
rb.table.info <- function(tablename,getStartEndKeys=FALSE){
  tablehandle <- .jnew("org/apache/hadoop/hbase/client/HTable",tablename)
  isenabled <- .jcall("org/apache/hadoop/hbase/client/HTable","Z","isTableEnabled",tablename)
  descriptor <- tablehandle$getTableDescriptor()

  adj <- data.frame(name=tablename,metaregion= descriptor$isMetaRegion(), metatable = descriptor$isMetaTable(),
                    readonly = descriptor$isReadOnly(), rootregion=descriptor$isRootRegion()
                    ,maxfilesize=descriptor$getMaxFileSize() 
                    ,stringsAsFactors=FALSE)

  families <- .jevalArray(descriptor$getColumnFamilies())
  l <- lapply(families,function(r){
    data.frame(family=r$getNameAsString() ,blocksize = r$getBlocksize()
               ,bloomtype = .jstrVal(r$getBloomFilterType())
               ,compression=.jstrVal(r$getCompressionType())
               ,maxversions = r$getMaxVersions()
               ,ttl=r$getTimeToLive()
               ,blockcache =r$isBlockCacheEnabled()
               ,inmemory = r$isInMemory(),stringsAsFactors=FALSE
               )
  })
  y <- do.call(rbind,l)
  y <- y[order(y$family),];rownames(y) <- NULL
  X <- list(families=y,tableinfo=adj)
  if(getStartEndKeys){
    message("Computing Start-End keys")
    a <- tablehandle$getEndKeys()
    endkeys <-lapply(a, .jevalArray)
    a <- tablehandle$getStartKeys()
    startkeys <- lapply(a, .jevalArray)
    Y <- list(start=startkeys, end=endkeys)
    X$keyinfo <- Y
  }
  .jcheck()
  X
}
