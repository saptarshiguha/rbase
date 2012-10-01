#' Lists all tables present in HBase
#' @param ra Is the object returned by \link{\code{rb.init}}
#' @return A character vector of tables or null
#' @export
rb.list.tables <- function(ra){
  f <- rhuz(ra$listTables())
  .jcheck()
  f
}

