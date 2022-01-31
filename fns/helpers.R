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

slugify <- function(x, non_alphanum_replace="", space_replace="_", tolower=TRUE, toupper = FALSE) {
  x <- gsub("[^[:alnum:] ]", non_alphanum_replace, x)
  x <- gsub(" ", space_replace, x)
  if (tolower) {
    x <- tolower(x)
  }
  if (toupper) {
    x <- toupper(x)
  }
  return(x)
}

`%ni%` <- Negate(`%in%`)

which_not <- function(v1, v2) {
  v1[v1 %ni% v2]
}

is_string_number <- function(x, index = F) {
  if(index) {
    out <- grep("[^[:digit:][:punct:]]", x)
  } else {
    out <- grepl("[^[:digit:][:punct:]]", x)
    }
    return(out)
}

# https://stackoverflow.com/questions/55249599/create-a-col-types-string-specification-for-read-csv-based-on-existing-dataframe
get_col_types_short <- function(.df) {
    # Get column classes from input dataframe
    lst_col_classes__ <- purrr::map(.df, ~ class(.x))

    # Map classes to known single-character col_types indicator
    vl_col_class_char__ <- purrr::map_chr(lst_col_classes__, function(.e) {
        dplyr::case_when(
              "logical" %in% .e   ~ "l"
            , "integer" %in% .e   ~ "i"
            , "numeric" %in% .e   ~ "d"
            , "double" %in% .e    ~ "d"
            , "character" %in% .e ~ "c"
            , "factor" %in% .e    ~ "f"
            , "Date" %in% .e      ~ "D"
            , "POSIXct" %in% .e   ~ "T"
            , TRUE                ~ "c"
        )
    })

    # Return vector of single-character col_type indicator.
    # Element name is the source column it came from.
    vl_col_class_char__
}

paste_path <- function(...) {
  path <- paste(..., sep = "/") %>%
    { gsub("/+", "/", .) }
  return(path)
}

multi_write.csv <- function(data, filename, paths) {
  for(i in 1:length(paths)) {
    write.csv(data, paste_path(paths[i], filename), row.names = F)
  }
}

column_differences <- function(df1, df2) {
    n1 <- names(df1)[which(names(df1) %ni% names(df2))]
    n2 <- names(df2)[which(names(df2) %ni% names(df1))]
    cat("First dataframe has", length(n1), "unique columns:", n1,
        "\nSecond dataframe has", length(n2), "unique columns:", n2)
}

# new_dons <- add_new_input_cols(read_csv('output/inputs-archive/who_dons.csv'), who_dons)
# write.csv(new_dons, 'output/inputs-archive/who_dons.csv', row.names = F)


replace_NAs_0 <- function(df, cols) {
    for(c in cols) {
        df <- df %>%
            mutate(
                !!c := case_when(
                    is.na(get(c)) ~ 0,
                    TRUE ~ get(c)
                ))
    }
    return(df)
}

curl_and_delete <- function(url, FUN, ...) {
  curl::curl_download(url, "temporary")
  data <- FUN("temporary", ...)
  file.remove("RBTracker.xls")
  return(data)
}
