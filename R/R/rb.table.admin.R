makeColumnDescriptors <- function(ra,fams){
  if(is.null(names(fams))) name.fams <- rep("",length(fams)) else name.fams <- names(fams)
  fams <- lapply(seq_along(fams),function(i){
    n <- name.fams[[i]]
    l <- if(n==""){
      ## just a family, using defaults
      list(name=as.character(fams[[i]]))
    }else{
        c(name=n, eval(fams[[i]]))
      }
    cd <- .jnew("org/apache/hadoop/hbase/HColumnDescriptor",l$name)
    if(!is.null(l$blockCacheEnabled))
      cd$setBlockCacheEnabled(as.logical(l$blockCacheEnabled))
    if(!is.null(l$blocksize))
      cd$setBlocksize(as.integer(l$blocksize))
    if(!is.null(l$inmemory))
      cd$setInMemory(as.logical(l$inmemory))
    if(!is.null(l$maxversions))
        cd$setMaxVersions(as.integer(l$maxversions))
    if(!is.null(l$ttl))
      cd$setTimeToLive(as.integer(l$ttl))
    ra$completeColumnDescriptor(cd,l$bloomtype, l$compressiontype,l$compactioncompression)
  })
  .jcheck()
  fams
}




#' Exclude some table names
#' @param tablename  is the proposed tablename
#' @param exclude a regular expression of exclusions
#' @export
rb.excludeTables <- function(tablename, exclude=options("rb.excludes")){
  if(is.null(exclude)) exclude="^(m|c|t)"
  if(grepl(exclude, tablename)) stop(sprintf("Tablename  %s banned", tablename))
}



#' Create a new table
#' @param ra is the object returned \link{\code{rb.init}}
#' @param tablename is the name of the new table
#' @param ... the table specs
#' @examples
#' \dontrun{
#'   ra = rb.init()
#'   rb.table.new(ra,"anewtable",x,y=list (blockCacheEnabled=TRUE,maxversions=10))
#'   rb.table.new(ra,"anewtable",x,y=list(maxversions=1))
#' }
#' @export
rb.table.new <- function(ra,tablename, ...){
  ## e.g. data=list(blocksize=, compression=, maxversions=, ttl=,blockcache=, inmemory=)
  ## e.g. rb.table.new("FOO", "x1","x2","x3") (just set to default)
  rb.excludeTables(tablename)
  fams <- as.list(match.call(expand.dots=FALSE))[["..."]]
  if(is.null(fams)) stop("Please provide at least one family")
  htd <- .jnew("org/apache/hadoop/hbase/HTableDescriptor",tablename)
  if(is.null(names(fams))) name.fams <- rep("",length(fams)) else name.fams <- names(fams)
  cds <- makeColumnDescriptors(ra,fams)
  lapply(cds,function(r) htd$addFamily(r))
  ra$getAdmin()$createTable(htd)
}


rb.modify.table <- function(ra,tablename,op,fams){
  rb.excludeTables(tablename)
  if(is.null(fams)) stop("Please provide at least one family")
  cds <- makeColumnDescriptors(ra,fams)
  admin <- ra$getAdmin()
  admin$disableTable(tablename)
  if(op=="add") {
      lapply(cds,function(r) admin$addColumn(tablename, r))
    }else if (op=="delete"){
      lapply(cds,function(r)  admin$deleteColumn(tablename,r$getNameAsString()))
    }else if (op=="modify"){
      lapply(cds,function(r)  admin$modifyColumn(tablename, r))
    }
  admin$enableTable(tablename)
  .jcheck()
}

#' Add another column to a table (column should not be present)
#' @param ra Object returned from rb.init
#' @param tablename is the name of the table
#' @param ... the structure of the columns as given in \link{\code{rb.table.new}}
#' @examples
#' \dontrun{
#'   rb.columns.add(ra,"anewtable",x=list(maxversions=1),z=list(blockCacheEnabled=TRUE,maxversions=10))
#' }
#' @export
rb.columns.add <- function(ra,tablename, ...){
  ## will not modify a column
  fams <- as.list(match.call(expand.dots=FALSE))[["..."]]
  rb.modify.table(ra,tablename,"add",fams)
  .jcheck()
}
#' Delete another column to a table (column should be present)
#' @param ra Object returned from rb.init
#' @param tablename is the name of the table
#' @param ... the structure of the columns as given in \link{\code{rb.table.new}}
#' @examples
#' \dontrun{
#'   rb.columns.delete(ra,"anewtable",x)
#' }
#' @export
rb.columns.delete <- function(ra,tablename, ...){
  fams <- as.list(match.call(expand.dots=FALSE))[["..."]]
  rb.modify.table(ra,tablename,"delete",fams)
  .jcheck()
}
#' Modify  column in a table (already present)
#' @param ra Object returned from rb.init
#' @param tablename is the name of the table
#' @param ... the structure of the columns as given in \link{\code{rb.table.new}}
#' @examples
#' \dontrun{
#'   rb.columns.modify(ra,"anewtable",x=list(maxversions=11),z=list(blockCacheEnabled=FALSE,maxversions=10))
#' }
#' @export
rb.columns.modify <- function(ra,tablename, ...){
  ## columns must already by present
  fams <- as.list(match.call(expand.dots=FALSE))[["..."]]
  rb.modify.table(ra,tablename,"modify",fams)
  .jcheck()
}

#' Delete a table
#' @param ra Object returned from rb.init
#' @param tablename is the name of the table
#' @export
rb.delete <- function(ra,tablename){
  rb.excludeTables(tablename)
  admin <-  ra$getAdmin()
  admin$disableTable(tablename)
  admin$deleteTable(tablename)
  .jcheck()
}



