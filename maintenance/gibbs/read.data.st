data <- read.csv("scoredata_hg38.csv")
for (i in 1:ncol(data)){
  assign(as.character(colnames(data)[i]), data[,i][!is.na(data[,i])])
}
