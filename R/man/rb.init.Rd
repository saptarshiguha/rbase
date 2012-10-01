\name{rb.init}
\alias{rb.init}
\title{Initialiazes rbase}
\usage{
  rb.init(requestAdmin = TRUE, otherConfigs = NULL,
    HBASE.HOME = "/usr/lib/hbase",
    HADOOP.HOME = "/usr/lib/hadoop",
    HADOOP.CONF = sprintf("\%s/conf", HADOOP.HOME),
    HBASE.CONF = sprintf("\%s/conf", HBASE.HOME),
    rhipeJar = list.files(paste(system.file(package = "Rhipe"), "java", sep = .Platform$file.sep), pattern = "jar$", full = T),
    rbaseJar = list.files(paste(system.file(package = "rbase"), "java", sep = .Platform$file.sep), pattern = "jar$", full = T))
}
\arguments{
  \item{requestAdmin}{Set to TRUE and returns the
  adminsitrator java object if required, and usually is
  so.}

  \item{otherConfigs}{A character vector of other
  configuration files which are loaded after the default
  config files}
}
\description{
  Returns an adminstrative object of class rbadmin
}
\details{
  Call this function first and store the result, else it
  will get garbage collected.
}
\keyword{Map}
\keyword{MapReduce}

