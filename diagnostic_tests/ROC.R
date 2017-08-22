##################################################################################3
#
# ROC.R
#
# Generates an ROC and PR curve from VEST scores and actual labels.
#
# Usage: /usr/bin/R --slave --vanilla < ROC.R arg4 arg5 arg6 arg7 arg8
#
# Args:
# arg4 = inputfile: Data with true class labels prediction (numeric) and (Disease vs Neutral) from which to construct the curves (expected not to have a header)
# arg5 = truecol: column number of column with true class labels
# arg6 = predcol: column number of column with predictions (numeric)
# arg7 = destdir: location where image files will be saved
# arg8 = figtitle: title for figures

getAPR <- function(xvals,yvals){
   area <- c()
   for (i in 1:(length(xvals)-1)){
      area <- c(area, (xvals[i+1]-xvals[i])*((yvals[i]+yvals[i+1])/2))
   }
   return(sum(area))
}

#############################  THE MAIN FUNCTION  ################################

argv <- function(x) {
         args <-  commandArgs()
         return(args[x])
}

inputfile = argv(4)  # File with score info (should have a column with prediction and a column with true class label
truecol = argv(5)    # column number of true class label
predcol = argv(6)    # column number of predicted class label
destdir = argv(7)    # directory where plots should be stored
figtitle = argv(8)   # title to place on figure (underscores will be converted to spaces)

title <- gsub("_"," ",figtitle)

# Construct file names for output images
fname = strsplit(inputfile,"/")
fname2 = strsplit(fname[[1]][length(fname[[1]])],"\\.")
name = fname2[[1]][1]
figfilename = paste(destdir,name,"_ROC.eps",sep="")
figfilename2 = paste(destdir,name,"_PR.eps",sep="")

library(ROCR)

#Read input data into a table (No Header!)
perf.data <- read.table(file=inputfile, sep="\t",header=TRUE)
pred.labels <- as.vector(perf.data[,as.numeric(predcol)])
class.labels <- as.vector(perf.data[,as.numeric(truecol)]) 
class.labels[which(class.labels == "Neutral")] <- 0
class.labels[which(class.labels == "Disease")] <- 1

# Determine actual number of each class for ROC plot
numPos <- length(which(class.labels == 1))
numNeg <- length(which(class.labels == 0))

#Get ROC curve
class.pred <- prediction(as.numeric(pred.labels),as.numeric(class.labels))#,label.ordering=c(1,0)) # use label ordering if the scores are reversed (eg. 0 means disease rather than 1) as with SIFT
class.perf <- performance(class.pred,'auc')
class.auc <- attr(class.perf,'y.values')
class.rates <- performance(class.pred,"tpr","fpr")
class.rates2 <- performance(class.pred,"prec","rec")

#Estimate area under ROC curve
xvals <- unlist(class.rates@x.values)
yvals <- unlist(class.rates@y.values)
if(is.nan(yvals[1])) { yvals[1] <- yvals[2]} # First yvalue is NaN -> set to second (AUC returned matches that returned by ROCR commands)
class.AUC <- getAPR(xvals,yvals)

#Estimate area under PR curve
xvals <- unlist(class.rates2@x.values)
yvals <- unlist(class.rates2@y.values)
if(is.nan(yvals[1])) { yvals[1] <- yvals[2]} # First yvalue is NaN . set to second
class.PR <- getAPR(xvals,yvals)

cat("ROC AUC = ")
cat(unlist(class.auc),"\n")
cat("PR AUC = ")
cat(unlist(class.PR),"\n")

#Image ROC curve
x11()
plot(class.rates,main=as.expression(title),colorize=TRUE,colorkey.pos="top",lwd=5,xlim=c(0,1),ylim=c(0,1))
text(x=0.0,y=0.95,as.expression("AUC="),adj=c(0,0))
text(x=0.0,y=0.90,as.expression(class.AUC),adj=c(0,0))
text(x=0.0,y=0.85,as.expression("#Pos="),adj=c(0,0))
text(x=0.0,y=0.80,as.expression(numPos),adj=c(0,0))
text(x=0.0,y=0.75,as.expression("#Neg="),adj=c(0,0))
text(x=0.0,y=0.70,as.expression(numNeg),adj=c(0,0))
dev.copy2eps(file=figfilename)
dev.off()

#Image PR curve 2
x11()
plot(class.rates2,main=as.expression(title),colorize=TRUE,colorkey.pos="top",lwd=5,xlim=c(0,1),ylim=c(0,1))
text(x=0.0,y=0.95,as.expression("AUC="),adj=c(0,0))
text(x=0.0,y=0.90,as.expression(class.PR),adj=c(0,0))
text(x=0.0,y=0.85,as.expression("#Pos="),adj=c(0,0))
text(x=0.0,y=0.80,as.expression(numPos),adj=c(0,0))
text(x=0.0,y=0.75,as.expression("#Neg="),adj=c(0,0))
text(x=0.0,y=0.70,as.expression(numNeg),adj=c(0,0))
dev.copy2eps(file=figfilename2)
dev.off()
