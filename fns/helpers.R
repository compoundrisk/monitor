# Finds the percentile of a given value in a vector 
reverse_percentile <- function(vector, values, na.rm = F, desc = F) {
    if (na.rm) vector <- subset(vector, !is.na(vector))
    percentiles <- sapply(values, function(x) {
        under <- subset(vector, vector <= x)
        percentile <- length(under)/length(vector)
        if (desc) percentile <- 1 - percentile
        return(percentile)
    })
    return(percentiles)
}