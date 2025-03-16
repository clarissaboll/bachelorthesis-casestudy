filePath <- here("data")

process_crossreference_files <- function(conn) {
  crossreferenceFiles <- get_file_names(filePath, "crossreference")
  create_crossreference_table(conn)
  for (file in crossreferenceFiles) {
    process_crossreference_data(file, conn)
  }
}

process_crossreference_data <- function(file, conn) {
  full_file_path <- file.path(filePath, file)
  crossreferenceSheet <- excel_sheets(full_file_path)
  
  crossreference_data <- get_cell_infos(full_file_path, crossreferenceSheet)
  create_crossreference_table(conn)
  
  substudy_col <- get_col_start(crossreference_data, "study")
  sample_col <- get_col_start(crossreference_data, "sample")
  ec_col <- get_col_start(crossreference_data, "ec")
  cas_col <- get_col_start(crossreference_data, "cas")
  category_col <- get_col_start(crossreference_data, "category")
  
  filled_rows <- unique(crossreference_data$row)
  filled_rows <- filled_rows[filled_rows != substudy_col$row]
  
  for (row in filled_rows) {
    substudy <- filter_character_from_row_col(crossreference_data, crossreferenceSheet, row, substudy_col$col)
    sample_name <- filter_character_from_row_col(crossreference_data, crossreferenceSheet, row, sample_col$col)
    ec_no <- filter_character_from_row_col(crossreference_data, crossreferenceSheet, row, ec_col$col)
    cas_no <- filter_character_from_row_col(crossreference_data, crossreferenceSheet, row, cas_col$col)
    category <- filter_character_from_row_col(crossreference_data, crossreferenceSheet, row, category_col$col)
    
    insert_crossreference_data(conn, substudy, sample_name, ec_no, cas_no, category)
  }
  
}

get_col_start <- function(crossreference_data, name) {
  return(crossreference_data %>%
           filter(stri_detect_regex(character, paste0("\\b", name, "\\b"), case_insensitive = TRUE)) %>% 
           select(col, row) %>%
           arrange(row) %>%      
           slice_head(n = 1))
}
