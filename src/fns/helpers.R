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

slugify <- function(x, non_alphanum_replace = "", space_replace = "_", tolower = TRUE, toupper = FALSE) {
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
  if (index) {
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
  for (i in seq_along(paths)) {
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
    for (c in cols) {
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

get_script_path <- function() {
  # location of script can depend on how it was invoked:
  # source() and knit() put it in sys.calls()
  path <- NULL

  if (!is.null(sys.calls())) {
    # get name of script - hope this is consisitent!
    path <- as.character(sys.call(1))[2]
    # make sure we got a file that ends in .R, .Rmd or .Rnw
    if (grepl("..+\\.[R|Rmd|Rnw]", path, perl=TRUE, ignore.case = TRUE) )  {
      return(path)
    } else {
      message("Obtained value for path does not end with .R, .Rmd or .Rnw: ", path)
    }
  } else{
    # Rscript and R -f put it in commandArgs
    args <- commandArgs(trailingOnly = FALSE)
  }
  return(path)
}
getsd <- function() stringr::str_replace(get_script_path(), "/[^/]*$", "")

get_country_groups_dictionary <- function() {
  # Check if country_groups already is defined, and if it has the appropriate columns
  if (ifelse(exists("country_groups"), "Economy" %in% names(country_groups), F)) {
    dictionary <- country_groups$Economy
    names(dictionary) <- country_groups$Code
  } else if ( file.exists("src/country-groups.csv")) {
    # message("`country_groups` is not defined or does not have variable `Economy`")
    country_groups <- read_csv("src/country-groups.csv", col_types = "ccccccccc")
    dictionary <- country_groups$Economy
    names(dictionary) <- country_groups$Code
  } else {
    warning("`src/country-groups.csv` does not exist and `country_groups` is not defined")
    dictionary <- tryCatch({
      country_groups <- read_csv("https://github.com/compoundrisk/monitor/raw/databricks/src/country-groups.csv", col_types = "ccccccccc")
      dictionary <- country_groups$Economy
      names(dictionary) <- country_groups$Code
      dictionary
    },
      error = function(e) {
        dictionary <- c(
          "XKX" = "Kosovo",
          "CIV" = "Cote d'Ivoire",
          "COD" = "Congo, DR",
          "COG" = "Congo, Republic")
        return(dictionary)
        })
  }
  return(dictionary)
}

define_iso2name <- function() {
  # Writes the iso2name function using the latest country code list
  dictionary <- get_country_groups_dictionary()
  compiler::cmpfun(function(v) {
    names <- countrycode::countrycode(v, origin = "iso3c", destination = "country.name", custom_match = dictionary)
    return(names)
  })
}

iso2name <- define_iso2name()

define_name2iso <- function() {
  dictionary <- get_country_groups_dictionary()
  dictionary <- setNames(names(dictionary), dictionary)
  dictionary <- c(
    dictionary, c(
    "Kosovo" = "XKX",
    "Micronesia" = "FSM",
    "Türkiye" = "TUR",
    "Turkiye" = "TUR",
    "São Tomé and Príncipe" = "STP"
    ))
  dictionary <- dictionary[unique(names(dictionary))]
  names(dictionary) <- tolower(names(dictionary))

if (file.exists("src/region-names.csv")) {
  multi_country_dictionary_df <- read_csv("src/region-names.csv", col_types = "c") %>%
    mutate(isos = paste0("%%", isos, "%%"))
} else {
  message("`src/region-names.R` downloaded from online repository")
  multi_country_dictionary_df <- read_csv("https://raw.githubusercontent.com/compoundrisk/monitor/databricks/src/region-names.csv", col_types = "c") %>%
    mutate(isos = paste0("%%", isos, "%%"))
}
  multi_country_dictionary <- multi_country_dictionary_df$isos %>%
    setNames(multi_country_dictionary_df$name)
  rm(multi_country_dictionary_df)
# setwd(wd)

  name2iso_internal <- function(v) {
    names <- countrycode::countrycode(v, destination = "iso3c", origin = "country.name", custom_match = dictionary)
    if (is.logical(names)) names <- as.character(names)
    return(names)
  }

  function(v, multiple_matches = F, region_names = F) {
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

  V <- v
  v <- tolower(v)

  output <- suppressWarnings(name2iso_internal(v))
  na_index <- which(is.na(output))
  region_index <- if (!is.null(multi_country_dictionary) & region_names == T) {
    which(stringr::str_detect(v, paste(names(multi_country_dictionary), collapse = "|")))
  } else {NA}
  combined_index <- sort(unique(c(na_index, region_index)))
  # If there are no NAs and no region names detected, just return the output
  if (length(combined_index) == 0) {
      return(output)
  }
  if (length(na_index) > 0) {
      # names <- v[is.na(output)]
      codelist <- countrycode::codelist
      cnames <- setNames(codelist$iso3c, tolower(codelist$country.name.en))
      cnames["kosovo"] <- "XKX"
      # cnames_no_space <- setNames(cnames, stringr::str_replace_all(names(cnames), " ", ""))
      cnames_regex <- setNames(codelist$iso3c, codelist$country.name.en.regex)
      cnames_regex["kosovo"] <- "XKX"
      names(cnames_regex) <- stringr::str_replace_all(names(cnames_regex), "\\.(?=[a-z])", ".?")

      # names(dictionary) <- tolower(names(dictionary))
      dictionary_no_space <- setNames(dictionary, stringr::str_replace_all(names(dictionary), " ", ""))

      multiple_isos <- lapply(na_index, function(n) {
        isos <- c(
              # Look backwards and forwards through multiple dictionaries to find country names
              # Backwards is important so that, eg, Guinea doesn't prevent Papua New Guinea from being found
              stringr::str_replace_all(v[n], cnames),
              stringr::str_replace_all(v[n], rev(cnames)),
              # stringr::str_replace_all(v[n], cnames_no_space),
              # stringr::str_replace_all(v[n], rev(cnames_no_space)),
              stringr::str_replace_all(v[n], cnames_regex),
              stringr::str_replace_all(v[n], rev(cnames_regex)),
              stringr::str_replace_all(v[n], dictionary),
              stringr::str_replace_all(v[n], rev(dictionary)),
              stringr::str_replace_all(v[n], dictionary_no_space),
              stringr::str_replace_all(v[n], rev(dictionary_no_space))) %>%
              paste(collapse = " ") %>%
              stringr::str_replace_all("[A-Z]{3}[A-Z]+", "") %>%
              stringr::str_extract_all("[A-Z]{3}", simplify = T)  %>% as.vector() %>% unique()
        new_isos <- lapply(seq_along(isos), function(i, data) {
            iso <- data[i]
            name <- iso2name(iso)
            # Check if any country names are part of another country name (eg Guinea in PNG)
            if (!any(stringr::str_detect(iso2name(data[-i]), name))) {
                return (iso)
            } else {
              # Remove the country name; if it is still found, it appears 2x and therefore is actually present
                if (stringr::str_replace(v[n], tolower(name), "") %>% stringr::str_detect(tolower(name))) {
                    return(iso)
                }
            }}, data = isos) %>%
            unlist() %>% paste(collapse = ", ")
            # warning(paste0(v[n], ": ", new_isos))
            return(c(index = n, isos = new_isos))
      }) %>% bind_rows() %>%
        subset(isos != "")

      output[as.numeric(multiple_isos$index)] <- multiple_isos$isos
    }
    if (length(region_index) > 0 & region_names == T) {
      # Look for region or country groups, eg. Sahel or Central America
      output_groups <- stringr::str_replace_all(v, multi_country_dictionary) %>% stringr::str_extract("%%([A-Z]{3},? ?)*%%") %>% stringr::str_extract("[^%]+")

      output <- sapply(seq_along(output), function(i) {
        if (is.na(output[i])) {
          output_groups[i]
        } else if (is.na(output_groups[i])) {
          output[i]
        } else {
          paste(output[i], output_groups[i], sep = ", ")
        }
        })
    }

      message_df <- tibble(Original = V, New = output)[combined_index,]
      successes <- filter(message_df, !is.na(New))
      failures <- filter(message_df, is.na(New))
      if (nrow(successes) > 0) message(paste0(nrow(successes), " items were matched as country groups or multi-country strings:\n"), paste0(capture.output(successes), collapse = "\n"))
      if (nrow(failures) > 0) warning(paste(nrow(failures), ifelse(nrow(failures) == 1, "item was", "items were"), "not matched\n"), paste0(capture.output(failures), collapse = "\n"))

      # print("Dev Note: Edit this function to identify which of the matches that name2iso couldn't match were matched by name2match multiple_matches = T")
      return(output)
  }
}


name2iso <- define_name2iso()
rm(define_name2iso)

# Function for checking whether a country's regex appears in a piece of text; vectorized
# `remove_negative_looks` removes all negative lookaheads from regex.
# E.g., for Guinea, "^(?!.*eq)(?!.*span)(?!.*bissau)(?!.*portu)(?!.*new).*guinea.*" becomes
# "^.*guinea", which prevents any "eq" in a string of text from preventing the verification
verify_country <- \(text, iso3, remove_negative_looks = F) {
  if (any(str_detect(iso3, "[^A-Za-z]") & !is.na(iso3))) stop ("Non alpha character is in iso3")
  iso3[is.na(iso3)] <- "000"
  df <- bind_cols(text = text, iso3 = iso3)
  codelist$iso3c[which(codelist$country.name.en == "Kosovo")] <- "XKX"
  df <- left_join(
    df,
    select(codelist, iso3c, country.name.en.regex),
    by = c("iso3" = "iso3c"),
    relationship = "many-to-one")
  if (remove_negative_looks) {
    df$country.name.en.regex = str_replace_all(df$country.name.en.regex, "\\(\\?\\!.*?\\)", "")
  }
  valid <- str_detect(tolower(df$text), df$country.name.en.regex)
  return(valid)
}

# FUNCTION TO READ MOST RECENT FILE IN A FOLDER
# Requires re-structuring `Indicator_dataset/` in `compoundriskdata` repository
# Also could mean saving all live-downloaded data somewhere
# I need to save the filename so I can use the date in it as the access_date
read_most_recent <- compiler::cmpfun(function(directory_path, FUN = read.csv, ..., as_of, date_format = "%Y-%m-%d", return_date = F, return_name = F, n = 1, exclude_string = NULL) {
    file_names <- list.files(directory_path)
    if (!is.null(exclude_string)) {
      file_names <- file_names[str_detect(file_names, exclude_string, negate = T)]
    }
    # Reads the date portion of a filename in the format of acaps-2021-12-13
    file_dates <- data.frame(
      file_names = file_names,
      # name_dates = sub(".*(20[[:digit:][:punct:]]+)\\..*", "\\1", file_names) %>%
      name_dates = str_extract(file_names, "20\\d{2}[:punct:]?\\d{1,2}[:punct:]?\\d{1,2}") %>%
        stringr::str_replace_all("[:punct:]", "-") %>%
        as.Date(format = date_format)) %>%
        filter(!is.na(name_dates)) %>%
        arrange(name_dates)
    if (n == "all") n <- nrow(file_dates)
    selected_files <- subset(file_dates, name_dates <= as_of) %>% tail(n)

    data <- apply(selected_files, 1, function(r) {
      most_recent_file <- r["file_names"]# file_names[which(name_dates == date)]
      data <- FUN(file.path(directory_path, most_recent_file), ...)
      # data <- FUN(paste_path(directory_path, most_recent_file), col_types = "dddddccD")
      return(data)
    })
    names(data) <- selected_files$file_names

    if (n == 1) data <- data[[1]]

    output <- list(
      data = data,
      date = if (return_date) selected_files$name_dates else NULL,
      name = if (return_name) selected_files$file_names else NULL)
    output <- output[lengths(output) != 0]
    if (length(lengths(output)) == 1) output <- output[[1]]

    return(output)
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

  if ( length(ASTs) == 0 ) return( enexpr(x) )        # Not in a pipe
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
leading_zeros <- function(data, length, filler = "0", trailing = F) {
  strings <- sapply(data, function(x) {
    if (is.na(x)) {
      x <- 0
      warning("NAs converted to 0. Likelihood of duplicate strings")
    }
    if (nchar(x) > length) {
      stop(paste("String is longer than desired length of", length, ":", x, "in", deparse(substitute(data))))
    }
    if (trailing == F) {
      string <- paste0(paste0(rep(filler, length - nchar(x)), collapse = ""), x)
    } else {
      string <- paste0(x, paste0(rep(filler, length - nchar(x)), collapse = ""))
    }
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
    rep <- as.character(eval(parse(text = stringr::str_extract(v, "[^(%%)].+[^(%%)]"))))
    if (length(rep) == 0) {rep <- "-"} else {rep}
  })
  names(reps) <- stringr::str_replace_all(names(reps), "([\\[\\]\\(\\)\\$])", "\\\\\\1")
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

duplicated2way <- duplicated_all <- function(x) {
  duplicated(x) | duplicated(x, fromLast = T)
}

paste_df <- function(df1, df2) {
    matrix(paste(
        as.matrix(df1),
        as.matrix(df2)) %>% trimws(),
    ncol = ncol(df1),
    dimnames = dimnames(df1))
}

initials <- function(v) {
  v %>% lapply(function(x) {
    if (!str_detect(x, "\\s|-")) return(x) else {
      inits <- str_extract_all(x, "(^|\\s|-)([A-Z])", simplify = T) %>% str_extract_all("[A-Z]", simplify = T) %>% paste(collapse = "")
      return(inits)
    }
  })  %>% unlist()
}

yaml_as_df <- function(yaml, print = F) {
  items <- names(yaml)
  keys <-  unique(unlist(sapply(yaml, names)))
  tib <- bind_cols(lapply(keys, function(k) tibble({{k}} := rep(list(NA), length(items)))))
  for (j in colnames(tib)) {
    for (i in seq_along(items)) {
      tib[i, j] <- list(yaml[[i]][j])
    }
    tib[[j]][sapply(tib[[j]], is.null)] <- NA
    if (all(lengths(tib[[j]])<=1)) tib[j] <- unlist(tib[j])
  }
  tib <- bind_cols(item = items, tib)
  # This method is much simpler but is 4x slower
  # tib <- bind_rows(lapply(yaml, function(item) {
  #   tidyr::pivot_wider(tibble::enframe(item), names_from = name, values_from = value)
  # }))
  if(print) print(as.data.frame(tib), right = F)
  return(tib)
}

tolatin <- function(x) stringi::stri_trans_general(x, id = "Latin-ASCII")
