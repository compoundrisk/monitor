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

vsubset <- function(v, condition) v[eval(str2expression(paste("v", condition)))]

which_not <- function(v1, v2, swap = F, both = F) {
  if (both) {
    list(
      "In V1, not in V2" = v1[v1 %ni% v2],
      "In V2, not in V1" = v2[v2 %ni% v1]
    )
  } else
  if (swap) {
    v2[v2 %ni% v1]
  } else {
    v1[v1 %ni% v2]
  }
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
get_col_types_short <- function(.df, collapse = T) {
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

    if (collapse) vl_col_class_char__ <- paste0(vl_col_class_char__, collapse = "")
    # Return vector of single-character col_type indicator.
    # Element name is the source column it came from.
    return(vl_col_class_char__)
}

paste_path <- compiler::cmpfun(function(...) {
  items <- c(...)
  if (items[1] == "") items <- items[-1]
  path <- paste(items, collapse = "/") %>%
    { gsub("/+", "/", .) }
  return(path)
})

multi_write.csv <- compiler::cmpfun(function(data, filename, paths) {
  for(i in 1:length(paths)) {
    write.csv(data, paste_path(paths[i], filename), row.names = F)
  }
})

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

curl_and_delete <- compiler::cmpfun(function(url, FUN, ...) {
  curl::curl_download(url, "temporary")
  data <- FUN("temporary", ...)
  file.remove("temporary")
  return(data)
})

# See IFES and DONS paste summarizes for what this is trying to generalize
# summarize_many_columns <- function(df, group_by, new_col, old_cols, sep) {
#   df <- group_by(df, group_by)
# ...
# }


define_iso2name <- function() {
# Writes the iso2name function using the latest country code list

  # Check if country_groups already is defined, and if it has the appropriate columns
  if (ifelse(exists("country_groups"), "Economy" %in% names(country_groups), F)) {
    dictionary <- country_groups$Economy
    names(dictionary) <- country_groups$Code
  } else if ( file.exists("src/country-groups.csv")) {
    warning("`country_groups` is not defined or does not have variable `Economy`")
    country_groups <- read_csv("src/country-groups.csv", col_types = "ccccccccc")
    dictionary <- country_groups$Economy
    names(dictionary) <- country_groups$Code
  } else {
    warning("`src/country-groups.csv` does not exist and `country_groups` is not defined")
    dictionary <- c(
        "XKX" = "Kosovo",
        "CIV" = "Cote d'Ivoire",
        "COD" = "Congo, DR",
        "COG" = "Congo, Republic")
  }

  compiler::cmpfun(function(v) {
    names <- countrycode::countrycode(v, origin = "iso3c", destination = "country.name", custom_match = dictionary)
    return(names)
  })
}

iso2name <- define_iso2name()

define_name2iso <- function() {

    # Check if country_groups already is defined, and if it has the appropriate columns
  if (ifelse(exists("country_groups"), "Economy" %in% names(country_groups), F)) {
    dictionary <- country_groups$Code
    names(dictionary) <- country_groups$Economy
  } else if ( file.exists("src/country-groups.csv")) {
    warning("`country_groups` is not defined or does not have variable `Economy`")
    country_groups <- read_csv("src/country-groups.csv", col_types = "ccccccccc")
    dictionary <- country_groups$Code
    names(dictionary) <- country_groups$Economy
  } else {
    warning("`src/country-groups.csv` does not exist and `country_groups` is not defined")
    dictionary <- c()
  }
dictionary <- c(
    dictionary, c(
    "Kosovo" = "XKX",
    "kosovo" = "XKX",
    "Micronesia" = "FSM",
    "micronesia" = "FSM",
    "Türkiye" = "TUR",
    "türkiye" = "TUR",
    "Turkiye" = "TUR",
    "turkiye" = "TUR"))
dictionary <- dictionary[unique(names(dictionary))]  

    name2iso_internal <- function(v) {
    names <- countrycode::countrycode(v, destination = "iso3c", origin = "country.name", custom_match = dictionary)
    if (is.logical(names)) names <- as.character(names)
    return(names)
  }

  function(v, multiple_matches = F) {
  # This new multiple_matches = T argument makes this a much more complicated function,
  # though perhaps it could be written more simply. If the arg is set to TRUE, the function
  # looks at all NAs and tries to find all the matches it can with the country names list.
  # (It looks both backwards and forwards; otherwise as soon as Guinea was found, Papua
  # New Guinea wouldn't be possibly found.) It then scans each country name to see if its
  # detected within another select country's name. If it is, it's name is removed from the
  # original list, and the original list is scanned for it again. For example, if the list 
  # is "Algeria Papua New Guinea", the initial result is "DZA GIN PGA"; but Guinea is within
  # Papua New Guinea, so it is removed: "DZA, "GIN". However, if the original list is
  # "Algeria Guinea Papua New Guinea", the first "Guinea" is removed, but "Guinea" is still
  # deteced in the list: "DZA GIN PGA" 

  output <- name2iso_internal(v)

  if (!any(is.na(output))) {
      return(output)
  } else {
      # names <- v[is.na(output)]
      na_index <- which(is.na(output))

      cnames <- setNames(codelist$iso3c, tolower(codelist$country.name.en))
      cnames["kosovo"] <- "XKX"
      # cnames_no_space <- setNames(cnames, str_replace_all(names(cnames), " ", ""))
      cnames_regex <- setNames(codelist$iso3c, codelist$country.name.en.regex)
      cnames_regex["kosovo"] <- "XKX"
      names(cnames_regex) <- str_replace_all(names(cnames_regex), "\\.(?=[a-z])", ".?")

      names(dictionary) <- tolower(names(dictionary))
      dictionary_no_space <- setNames(dictionary, str_replace_all(names(dictionary), " ", ""))

      v <- tolower(v)

      multiple_isos <- lapply(na_index, function(n) {
        isos <- c(
              # Look backwards and forwards through multiple dictionaries to find country names
              # Backwards is important so that, eg, Guinea doesn't prevent Papua New Guinea from being found
              str_replace_all(v[n], cnames),
              str_replace_all(v[n], rev(cnames)),
              # str_replace_all(v[n], cnames_no_space),
              # str_replace_all(v[n], rev(cnames_no_space)),
              str_replace_all(v[n], cnames_regex),
              str_replace_all(v[n], rev(cnames_regex)),
              str_replace_all(v[n], dictionary),
              str_replace_all(v[n], rev(dictionary)),
              str_replace_all(v[n], dictionary_no_space),
              str_replace_all(v[n], rev(dictionary_no_space))) %>%
              paste(collapse = " ") %>%
              str_replace_all("[A-Z]{3}[A-Z]+", "") %>%
              str_extract_all("[A-Z]{3}", simplify = T)  %>% as.vector() %>% unique()
        new_isos <- lapply(seq_along(isos), function(i, data) {
            iso <- data[i]
            name <- iso2name(iso)
            # Check if any country names are part of another country name (eg Guinea in PNG)
            if (!any(str_detect(iso2name(data[-i]), name))) {
                return (iso)
            } else {
              # Remove the country name; if it is still found, it appears 2x and therefore is actually present
                if (str_replace(v[n], tolower(name), "") %>% str_detect(tolower(name))) {
                    return(iso)
                }
            }}, data = isos) %>%
            unlist() %>% paste(collapse = ", ")
            print(paste0(v[n], ": ", new_isos))
            return(c(index = n, isos = new_isos))
      }) %>% bind_rows() %>% 
        subset(isos != "")

      output[as.numeric(multiple_isos$index)] <- multiple_isos$isos
      # print("Dev Note: Edit this function to identify which of the matches that name2iso couldn't match were matched by name2match multiple_matches = T")
      return(output)
  }
  }
}

name2iso <- define_name2iso()

# FUNCTION TO READ MOST RECENT FILE IN A FOLDER
# Requires re-structuring `Indicator_dataset/` in `compoundriskdata` repository
# Also could mean saving all live-downloaded data somewhere
# I need to save the filename so I can use the date in it as the access_date
read_most_recent <- compiler::cmpfun(function(directory_path, FUN = read.csv, ..., as_of, date_format = "%Y-%m-%d", return_date = F, n = 1) {
    file_names <- list.files(directory_path)
    # Reads the date portion of a filename in the format of acaps-2021-12-13
    name_dates <- sub(".*(20[[:digit:][:punct:]]+)\\..*", "\\1", file_names) %>%
        str_replace_all("[:punct:]", "-") %>%
        as.Date(format = date_format) %>% sort()
    if (n == "all") n <- length(name_dates)
    selected_dates <- subset(name_dates, name_dates <= as_of) %>% tail(n)

    data <- lapply(selected_dates, function(date) {
      most_recent_file <- file_names[which(name_dates == date)]
      data <- FUN(paste_path(directory_path, most_recent_file), ...)
    })

    if (n == 1) data <- data[[1]]

    if (return_date) {
      return(list(data = data, date = selected_dates))
    }
    return(data)
})

count_extremes <- function(v) {
# how many peaks and valleys are in vector?
# the last value in a plateau is counted as the extreme
# the ends are not counted as extremes (a monotonic line has no extremes)
    d <- ifelse(v - lag(v, 1) > 0, 1,
        ifelse(v - lag(v, 1) < 0, -1,
        ifelse(v - lag(v, 1) == 0, 0, NA)))
    while(any(d == 0, na.rm = T)) {
        d <- ifelse(d != 0, d, lag(d))
    }

    extremes <- d + lead(d)
    sum(extremes == 0, na.rm = T)
}

first_ordered_instance <- function(v, na.eq = T) {
    if (na.eq){
        l <- !(v == lag(v) | (is.na(v) & is.na(lag(v))))
        l[is.na(l)] <- T
        l[1] <- T # Should this be for na.eq = F as well?
    } else {
        l <- v != lag(v)
        l[!is.na(v) & is.na(l)] <- T
    }
    return(l)
}

expr2text <- function(x) {
  getAST <- function(ee) purrr::map_if(as.list(ee), is.call, getAST)

  sc <- sys.calls()
  ASTs <- purrr::map( as.list(sc), getAST ) %>%
    purrr::keep( ~identical(.[[1]], quote(`%>%`)) )  # Match first element to %>%

  if( length(ASTs) == 0 ) return( enexpr(x) )        # Not in a pipe
  dplyr::last( ASTs )[[2]]    # Second element is the left-hand side
}


delay_error <- function(expr, return = NULL, no_stop = F, on = T, file.path = paste_path(output_directory, "errors.log")) {
  # no_stop=T means that a delayed_error variable will be created which will err when release_delayed_errors() is run
  # on means to delay errors, !on means to ignore the function; this is useful so that I can turn off all delays when debugging
  # fun <- sub("\\(.*", "", deparse(substitute(expr)))
  fun <- expr2text(expr)

  if (on) {
    tryCatch({
      expr
    },
      error = function(e) {
        if (!no_stop) {
          if (!exists("delayed_error")) {
            assign("delayed_error", fun, envir = .GlobalEnv)
          } else {
            assign("delayed_error", c(delayed_error, fun), envir = .GlobalEnv)
          }
        }
        if (file.exists('paste_path(output_directory, "errors.log")')) {
          write(paste(Sys.time(), "Error on", fun, "\n", e), file = file.path, append = F)
        } else {
          write(paste(Sys.time(), "Error on", fun, "\n", e), file = file.path, append = T)
        }
        if (!is.null(return)) {
          return(return)
        }
      })
  } else { expr }
}

release_delayed_errors <- function() {
  if (exists("delayed_error")) {
    de <- delayed_error
    rm(delayed_error, envir = .GlobalEnv) # removing from global env so it doesnt continue to stop future runs
    stop(paste("Error on", paste(de, collapse = ", ")))
  }
}

# Move this basic function elsewhere, somewhere more basic
leading_zeros <- function(data, length, filler = "0") {
  strings <- sapply(data, function(x) {
    if(is.na(x)) {
      x <- 0
      warning("NAs converted to 0. Likelihood of duplicate strings")
    }
    if(nchar(x) > length) {
      stop(paste("String is longer than desired length of", length, ":", x, "in", deparse(substitute(data))))
    }
    string <- paste0(paste0(rep(filler, length - nchar(x)), collapse = ""), x)
    return(string)
  })
  return(strings)
}

lap_start <- function() .GlobalEnv$lap_start_time <- Sys.time()
lap_print <- function(message = NULL) {
  duration <- Sys.time() - lap_start_time
  output <- paste0(
    ifelse(!is.null(message), paste0(message, ": "), ""),
    round(duration,2), " ", units(duration))
  print(output)
}

add_empty_rows <- function(dataframe, total_rows, fill = NA) {
  # Function is useful for forcing multiple dataframes to same length before column binding 
  # (e.g. when making a table for printing)
    if (is.data.frame(total_rows)) {total_rows <- nrow(total_rows) 
    } else {
        if (length(total_rows) > 1) total_rows <- length(total_rows)
    }
    dataframe[(nrow(dataframe)+1):total_rows,] <- fill
    return(dataframe)
}

bind_cols_fill <- function(df1, df2, fill = NA) {
  # Column binds dataframes or vectors of differing lengths
    if (is.data.frame(df1)) a <- nrow(df1) else a <- length(df1)
    if (is.data.frame(df2)) b <- nrow(df2) else b <- length(df2)
    if (a < b) {
        if (is.data.frame(df1)) df1[(nrow(df1)+1):b,] <- fill else df1[(length(df1)+1):b] <- fill
    }
    if (a > b) {
        if (is.data.frame(df2)) df2[(nrow(df2)+1):a,] <- fill else df2[(length(df2)+1):a] <- fill
    }
    dplyr::bind_cols(df1, df2)
}

vstring <- function(string, sep = "%%") {
  vars <- stringr::str_extract_all(string, "%%[^(%%)]*%%", simplify = T)
  reps <- sapply(vars, function(v) {
    as.character(eval(parse(text = stringr::str_extract(v, "[^(%%)].+[^(%%)]"))))
  })
  string <- stringr::str_replace_all(string, reps)
  return(string)
}

paste_and <- function(v) {
    if (length(v) == 1) {
    string <- paste(v)
  } else {
    # l[1:(length(l)-1)] %>% paste(collapse = ", ")
    paste(head(v, -1), collapse = ", ") %>%
    paste("and", tail(v, 1))
  }
}
