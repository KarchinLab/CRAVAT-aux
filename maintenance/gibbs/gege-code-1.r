---
title: "VEST null"
output: pdf_document
---

## Read data

```{r Read Data, echo=FALSE, message=FALSE,warning=FALSE}
dat = read.csv("/Users/gege/Dropbox/graduate/Bioinformatics/Final-Project/data/scoredata.csv")
for (i in 1:ncol(dat)){
  assign(as.character(colnames(dat)[i]), dat[,i][!is.na(dat[,i])])
}
MS = read.table("/Users/gege/Dropbox/graduate/Bioinformatics/Final-Project/VEST-analytical-null/missense.1kafr.scores")
FD = read.table("/Users/gege/Dropbox/graduate/Bioinformatics/Final-Project/VEST-analytical-null/F.average.null.scores")
ID = read.table("/Users/gege/Dropbox/graduate/Bioinformatics/Final-Project/VEST-analytical-null/I.null.scores")
MS = MS[,1]
FD = FD[,1]
ID = ID[,1]
```

## Use Beta and Exponential distribution as the null 

```{r Beta and Exponential, echo=FALSE, message=FALSE,warning=FALSE}
para = function(data, plotdis){
v = var(data)
xbar = mean(data)

# methods of moment for Beta distribution
x = seq(0, 1, by = 0.01)
al = xbar * (xbar * (1 - xbar)/v - 1)
be = (1 - xbar) * (xbar * (1 - xbar)/v - 1)

# mle for exponential distribution
lambda = 1/xbar

if (plotdis == T){
# plot(density(data), lwd = 2, xlim = c(0, 1), main = paste("Parametric:", substitute(data)))
hist((data), breaks = 25, col="lightsteelblue1", main = paste("Parametric:", substitute(data)), probability = T, xlim = c(0, 1))
lines(x, dbeta(x, al, be), col = "blue", lwd = 2)
lines(x, dexp(x, lambda), col = "red", lwd = 2)
legend("topright", legend = c("Data", "Beta", "Exponential"), col = c("black", "blue", "red"), lty = 1)
}

# return(list(paste("beta(", al, ",", be, "), ", "exp(", lambda, ")", sep = ""), al, be, lambda ))
return(list(paste("beta(", round(al, 3), ",", round(be, 3), "), ", "exp(", round(1/xbar, 3), ")", sep = ""), al, be, lambda ))
}
```

```{r Beta and Exponential usage, echo=FALSE, message=FALSE,warning=FALSE}
test_para = para(data = ID, 1)
```


## Use mixture of normal to approximate data distribution

```{r Normal Components, echo=FALSE, message=FALSE,warning=FALSE}
normal_compo = function(dat, iter, n, plotdistri){
  library(gtools)
  l = length(dat)
#  d = as.data.frame(matrix(NA, nrow = iter, ncol = 12))
# colnames(d) = c("p1", "p2", "p3", "n1", "n2", "n3", "theta1", "theta2", "theta3", "sigma1sq", "sigma2sq" , "sigma3sq")
  v0 = 3
  mu = mean(dat)
  tau0sq= 0.5
  sigsq = var(dat)

  p0 = rdirichlet(1, rep(1, n))
  p = as.data.frame(matrix(NA, nrow = iter, ncol = n))
  for (i in 1:n){
    p[1, i] = p0[i]
  }
  
  N = as.data.frame(matrix(NA, nrow = iter, ncol = n))
  for (i in 1:(n - 1)){
    N[1, i] = floor(l/n)
  }
  N[1, n] = l - (n - 1) * floor(l/n)

  theta = as.data.frame(matrix(NA, nrow = iter, ncol = n))
  for (i in 1:n){
    theta[1, i] = rnorm(1, mu, sqrt(tau0sq))
  }
  
  sigmasq = as.data.frame(matrix(NA, nrow = iter, ncol = n))
  for (i in 1:n){
    sigmasq[1, i] = 1/(rgamma(1, shape = v0/2, rate = v0/2*sigsq))
  }

for (i in 2:iter){
  p_tilde = as.data.frame(matrix(NA, nrow = length(dat), ncol = n))
  s = rep(0 ,length(dat))
  for (j in 1:n) {
    s = s + p[i - 1, j] * dnorm(dat, theta[i - 1, j], sqrt(sigmasq[i - 1, j]))
  }
  
  for (j in 1:n){
    p_tilde[, j] = p[i - 1, j] * dnorm(dat, theta[i - 1, j], sqrt(sigmasq[i - 1, j]))/s
  }
  
  index = matrix(rep(NA, nrow(p_tilde)*n), ncol = n)  
  for (j in 1:nrow(p_tilde)){
    temp = rmultinom(1, 1, p_tilde[j, ])
    for (w in 1:n){
      index[j, w] = temp[w]
    }
  }
  
  ydat = list()
  for (j in 1:n){
    ydat[[j]] = dat[index[,j] == 1]
  }
  
  prob = c()
  for (j in 1:n){
    N[i, j] = length(ydat[[j]])
    prob = c(prob, length(ydat[[j]]) + 1)
  }
  
  p_hat = rdirichlet(1, prob)
  p[i, ] = p_hat
  
  for (j in 1:n){
    mutemp = (sum(ydat[[j]])/sigmasq[i - 1, j] + mu/tau0sq)/(N[i, j]/sigmasq[i - 1, j] + 1/tau0sq)
    tautemp = 1/(N[i, j]/sigmasq[i - 1, j] + 1/tau0sq)
    theta[i, j] = rnorm(1, mutemp, sqrt(tautemp))
  }
  
  for (j in 1:n){
    vtemp = N[i, j] + v0
    sigmatemp = 1/vtemp * (v0*sigsq + sum((ydat[[j]] - theta[i, j])^2))
    sigmasq[i, j] = 1/(rgamma(1, shape = vtemp/2, rate = vtemp/2*sigmatemp))
  }
}

z = matrix(c(rep(NA, iter*n)), ncol = n)
y = rep(NA, iter)
for (i in 1:iter){
  z_o = rmultinom(1, 1, p[i, ])
  j = which(z_o == 1)
  y[i] = rnorm(1, theta[i, j], sqrt(sigmasq[i, j]))
}

# hist(y, probability = T, breaks = 40, ylim = c(0, max(y, dat)), xlim = c(-6, 2), col = "lightsteelblue1", main = "Normal n = 3")
if (plotdistri == T){
plot(density(y), lwd = 2, col = "blue", ylim = c(0, max(max(density(dat)$y), max(density(y)$y))), xlim = c(0, 1), main = paste("Plot using parameters from each iteration with", n, "component(s)"))
lines(density(dat), col = "red", lwd = 2)
legend("topright", legend = c("Data", "Gibbs"), col = c("red", "blue"), lty = 1)
}
re = list(y, p, N, theta, sigmasq)
return(re)
}
```

```{r Normal Components usage, echo=FALSE, message=FALSE,warning=FALSE}
test_normal = normal_compo(dat = Inframe, iter = 10, n = 3, 1)
```

## Use Gibbs to sample from average parameter

```{r Gibbs Sampler, echo=FALSE, message=FALSE,warning=FALSE}
gibbs_sample = function(data, iter, n, size, plotdistri){
test = normal_compo(data, iter, n, 0)
p0 = rep(NA, ncol(test[[2]]))
N0 = rep(NA, ncol(test[[3]]))
theta0 = rep(NA, ncol(test[[4]]))
sigmasq0 = rep(NA, ncol(test[[5]]))
for (i in 1:ncol(test[[2]])){
  p0[i] = mean(test[[2]][floor(iter/2): iter,i])
  N0[i] = mean(test[[3]][floor(iter/2): iter,i])
  theta0[i] = mean(test[[4]][floor(iter/2): iter,i])
  sigmasq0[i] = mean(test[[5]][floor(iter/2): iter,i])
}

y = rep(NA, size)
for (i in 1:size){
  z_o = rmultinom(1, 1, p0)
  j = which(z_o == 1)
  y[i] = rnorm(1, theta0[j], sqrt(sigmasq0[j]))
}

if (plotdistri == T){
  plot(density(data), lwd = 2, ylim = c(0, max(max(density(data)$y), max(density(y)$y))), xlim = c(min(data, y) - 0.1, max(data, y) + 0.1), main = paste(n, "component(s):", substitute(data), sep = " "))
lines(density(y), col = "green", lwd = 2)
legend("topright", legend = c("Data", "Gibbs"), col = c("black", "green"), lty = 1)
}
return(list(y, p0, N0, theta0, sigmasq0, data, test))
}
```

```{r Gibbs Sampler usage, echo=FALSE, message=FALSE,warning=FALSE}
test_gibbs = gibbs_sample(ID, 100, 3, 100, 1)
```

## Compare mixture of Gaussian, Beta and Exponential (when data are between 0 and 1)

```{r Distribution P_value Results and Plots, echo=FALSE, message=FALSE,warning=FALSE}
output = function(input, data, iter, n, size, plotdistri){
  y = gibbs_sample(data, iter, n, size, 0)[[1]]
  out = para(data, 0)
  al = out[[2]]
  be = out[[3]]
  lambda = out[[4]]
  x = seq(0, 1, by = 0.01)
  if (plotdistri == T){
  plot(density(data), lwd = 2, ylim = c(0, max(max(density(data)$y), max(density(y)$y))), xlim = c(0, 1), main = paste(n, "component(s):",substitute(data), sep = " "))
lines(density(y), col = "green", lwd = 2)
lines(x, dbeta(x, al, be), col = "blue", lwd = 2)
lines(x, dexp(x, lambda), col = "red", lwd = 2)
legend("topright", legend = c("Data", "Gibbs", "Beta", "Exponential"), col = c("black", "green", "blue", "red"), lty = 1)
  }
  pval1 = (length(c(input, y)) - (rank(c(input, y), ties.method = "first")[1] - 1))/length(c(input, y))
  pval2 = pbeta(input, al, be, lower.tail = F)
  pval3 = pexp(input, lambda, lower.tail = F)
  re = as.data.frame(matrix(rep(NA, 3), nrow = 1))
  colnames(re) = c("Gibbs", paste("Beta(", round(al, 3), ",", round(be, 3), ")", sep = ""), paste("Exp(", round(lambda, 3), ")", sep = ""))
  rownames(re) = "P-Value"
  re[1,] = c(pval1, pval2, pval3)
  return(re)
}
```


```{r Distribution P_value Results and Plots usage, echo=FALSE, message=FALSE,warning=FALSE}
test_pval = output(0.5, ID, iter = 100, n = 3, size = 100, 1)
```


```{r How to check Execution Time, echo=FALSE, message=FALSE,warning=FALSE}
library(tictoc)
tic()
toc()
```

## Change data to log scale

```{r data log transformation, echo=FALSE, message=FALSE,warning=FALSE}
for (i in 1:ncol(dat)){
  test = dat[,i][!is.na(dat[,i])]
  test = -log(test[test!=0])
  assign(paste("log_", as.character(colnames(dat)[i]), sep = ""), test)
}
```

## Bayes factor

```{r Bayes factor, echo=FALSE, message=FALSE,warning=FALSE}
# log.like0 = function(data, iter, n){
# the <- matrix(rnorm(iter*n, mean(data), 10), ncol=n)
# tau <- rgamma(iter, n, scale=1/3)
# library(gtools)
# p <- rdirichlet(iter, alpha=rep(1,n))
# log.mat = sapply(data, function(x){
#   s_li = 0
#   for (i in 1:n){
#     s_li = s_li + p[,i] * dnorm(x, mean=the[,i], sd=sqrt(1/tau))
#   }
#   return(log(s_li))
# })
# logl = apply(log.mat,1,sum)
# LogSumExp <- function(X){
#   max(X) + log(sum(exp(X - max(X))))
# }
# logMarg = LogSumExp(logl)
# return(logMarg)
# }
# 
# b.factor0 = function(data, iter, n1, n2){
#   return(log.like0(data, iter, n1) - log.like0(data, iter, n2))
# }
```

```{r BIC, echo=FALSE, message=FALSE,warning=FALSE}
bic = function(nor_result){
  y = nor_result[[1]]
  p = nor_result[[2]]
  the = nor_result[[4]]
  sig = nor_result[[5]]
  data = nor_result[[6]]
library(gtools)
  log_like = function(x){
    s_li = 0
    for (i in 1:length(p)){
      s_li = s_li + p[i] * dnorm(x, mean=the[i], sd=sqrt(sig[i]))
    }
    return(log(s_li))
  }
  b = length(p) * 3 * log(length(data)) - 2 * sum(log_like(data))
# LogSumExp <- function(X){
#   max(X) + log(sum(exp(X - max(X))))
# }
# logMarg = LogSumExp(logl)
return(b)
}
```

# Create p-value table

```{r Create p-value table, echo=FALSE, message=FALSE,warning=FALSE}
pval_tab = function(y, deci, p_val_deci){
  tol = 10^(deci)
  tab = as.data.frame(rep(0, tol + 1))
  colnames(tab) = "p-value"
  y = y[y>=0 & y<=1]
  for (i in 1:(tol)){
    input = 1/tol * i - 1/tol
    rownames(tab)[i] = input
    tab[i, 1] = round((length(c(input, y)) - (rank(c(input, y), ties.method = "first")[1] - 1))/length(c(input, y)), p_val_deci)
  }
  rownames(tab)[tol+1] = 1
  return(tab)
}

pval_tab_log = function(y, deci, p_val_deci){
  tol = 10^(deci)
  tab = as.data.frame(rep(0, tol + 1))
  colnames(tab) = "p-value"
  y = y[y>=0]
  y = exp(-y)
  for (i in 1:(tol)){
    input = 1/tol * i - 1/tol
    rownames(tab)[i] = input
    tab[i, 1] = round((length(c(input, y)) - (rank(c(input, y), ties.method = "first")[1] - 1))/length(c(input, y)), p_val_deci)
  }
  rownames(tab)[tol+1] = 1
  return(tab)
}
```

```{r p-value table usage, echo=FALSE, message=FALSE,warning=FALSE}
test_tab = pval_tab(test_gibbs[[1]], deci = 2, p_val_deci = 4)
test_tab
```

# Plot

```{r Plot density, echo=FALSE, message=FALSE,warning=FALSE}
plot_dens = function(data){
  hist(data, breaks = 25, col="lightsteelblue1", main = substitute(data), probability = T)
  lines(density(data), col = "blue", lwd = 2)
}

plot_hist = function(data, data2, line){
  title = substitute(data)
  if (max(data) <= 1){
    data2 = data2[data2<=1 & data2>=0]
  }else{
    data2 = data2[data2>=0]
    }
  hist(data, breaks = 25, col=rgb(0,0,1,0.3), main = title, probability = T, ylim = c(0, max(density(data)$y, density(data2)$y)))
  hist(data2, breaks = 25, col=rgb(1,0,0,0.3), add=T, probability = T)
  if (line == 1){
  lines(density(data), col = "blue", lwd = 2)
  lines(density(data2), col = "red", lwd = 2)
  }
  legend("topright", legend = c(title, "Gibbs"), fill = c(rgb(0,0,1,0.3), rgb(1,0,0,0.3)), cex = 0.7)
}
```

```{r Plot iteration of parameters, echo=FALSE, message=FALSE,warning=FALSE}
plot_iter = function(data){
  # data = FS_gibbs3[[7]]
  p = data[[2]]
  # N = data[[3]]
  theta = data[[4]]
  sigma = data[[5]]
  par(mfrow=c(1,3))
  plot(p[1:nrow(p), 1], type="l", main="Normal Component Probability", ylim = c(0,max(p)), col = rgb(0,0,1,0.3), xlab = "Iteration", ylab = "")
  for (i in 2:ncol(p)){
    lines(p[1:nrow(p), i], col = rgb(0,0,1,0.3 + (i-1) * 0.3))
  }
  
  plot(theta[1:nrow(theta), 1], type="l", main="Distribution Mean", ylim = c(0,max(theta)), col = rgb(0,0,1,0.3), xlab = "Iteration", ylab = "")
  for (i in 2:ncol(theta)){
    lines(theta[1:nrow(theta), i], col = rgb(0,0,1,0.3 + (i-1) * 0.3))
  }
  co = rgb(0,0,1,0.1)
  le = "1 component"
  plot(sigma[1:nrow(sigma), 1], type="l", main="Distribution Variance", ylim = c(0,max(sigma)), col = rgb(0,0,1,0.3), xlab = "Iteration", ylab = "")
  for (i in 2:ncol(sigma)){
    c1 = rgb(0,0,1,0.3 + (i-1) * 0.3)
    lines(sigma[1:nrow(sigma), i], col = c1)
    co = c(co, c1)
    le = c(le, paste(i, "component"))
  }
  legend("topright", le, col = co, lty = 1)
}
```

## Calculate P-value using created table

```{r Plot iteration of parameters, echo=FALSE, message=FALSE,warning=FALSE}
pval_calcu = function(table, p){
  x = nrow(table) - 1
  ind = p * x + 1
  return(table[ind,])
}
```


## Data analysis

Plot data trend to see how many components in the model are reasonable.

```{r Plot the data trend, echo=FALSE, message=FALSE,warning=FALSE}
plot_dens(FrameShift)
plot_dens(log_FrameShift)
length(FrameShift)
```


Specify iteration number and size: how many sample points we need to draw from Gibbs sampling posterior.

```{r Iter and size, echo=FALSE, message=FALSE,warning=FALSE}
iter = 10000
size = 100000
```

Fit model for raw data and compare their BIC.

```{r Raw data for BIC, echo=FALSE, message=FALSE,warning=FALSE}
FS_gibbs2 = gibbs_sample(FrameShift, iter, 2, size, 1)
FS_gibbs3 = gibbs_sample(FrameShift, iter, 3, size, 1)
FS_gibbs4 = gibbs_sample(FrameShift, iter, 4, size, 1)

bic(FS_gibbs2)
bic(FS_gibbs3)
bic(FS_gibbs4)
```

MCMC diagnosis

```{r MCMC diagnosis, echo=FALSE, message=FALSE,warning=FALSE}
test = FS_gibbs3[[7]]
da_p = test[[2]]
da_theta = test[[4]]
da_sigma = test[[5]]

library(coda)
par(mfrow = c(3, 3))
for (i in 1:ncol(da_p)){
  acf(da_p[5000:10000,i])
}
for (i in 1:ncol(da_theta)){
  acf(da_theta[5000:10000,i])
}
for (i in 1:ncol(da_sigma)){
  acf(da_sigma[5000:10000,i])
}

effectiveSize(a[1])
effectiveSize(a[2])
effectiveSize(a[3])

```

Use the sample points generated by the best model to create a p-value table and check time we need to get the p-value.

```{r Create table and calculate p_val, echo=FALSE, message=FALSE,warning=FALSE}
table1 = pval_tab(FS_gibbs3[[1]], 2, 8)
library(tictoc)
tic()
pval_calcu(table1, 0.9)
toc()

# cairo_ps("/Users/gege/Desktop/3.eps", width = 8, height = 5.5)
plot_hist(FrameShift, FS_gibbs3[[1]], 0)  ## best one with smallest BIC
# dev.off()
```

```{r Raw data, echo=FALSE, message=FALSE,warning=FALSE}
log_FS_gibbs2 = gibbs_sample(log_FrameShift, iter, 2, size, 1)
log_FS_gibbs3 = gibbs_sample(log_FrameShift, iter, 3, size, 1)
log_FS_gibbs4 = gibbs_sample(log_FrameShift, iter, 4, size, 1)
log_FS_gibbs5 = gibbs_sample(log_FrameShift, iter, 5, size, 1)
log_FS_gibbs6 = gibbs_sample(log_FrameShift, iter, 6, size, 1)




plot_hist(log_FrameShift, log_FS_gibbs2[[1]], 1)
plot_hist(log_FrameShift, log_FS_gibbs3[[1]], 1)
plot_hist(log_FrameShift, log_FS_gibbs[[1]], 1)
bic(log_FS_gibbs2)
bic(log_FS_gibbs)
plot_iter(data = FS_gibbs3[[7]])







# check with data that have limited observations
length(Stop.Loss)
```

```{r}
save.image(file = "/Users/gege/Dropbox/graduate/Bioinformatics/Final-Project/Rdata/test.Rdata")
```
