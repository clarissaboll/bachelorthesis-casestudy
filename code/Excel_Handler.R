get_cell_infos <- function(file, sheetnames) {
  mydf <- xlsx_cells(file,sheets = sheetnames, include_blank_cells = FALSE)
  head(mydf)
  tail(mydf)
  dim(mydf)
  xlsx_formats(file)
  xlsx_names(file)
  return(mydf)
}

get_file_names <- function(folder_path, name) {
  if (!dir.exists(folder_path)) {
    stop("Folder not found.")
  }
  
  file_list <- list.files(folder_path, pattern = name, 
                          full.names = TRUE, recursive = TRUE, ignore.case = TRUE)
  
  if (length(file_list) > 0) {
    return(basename(file_list)) 
  } else {
    return(NULL) 
  }
}

filter_sheet_row_col <- function(cell_info, sheet_name, row_num, col_num) {
  return(cell_info %>%
           filter(sheet == sheet_name) %>%
           filter(row == row_num) %>%
           filter(col == col_num))   
}

filter_character_from_row_col <- function(cell_info, current_sheet, current_row, current_col) {
  return(cell_info %>% 
           filter(sheet == current_sheet) %>%
           filter(row == current_row) %>%
           filter(col == current_col) %>%
           pull(character))
}

convert_csv_to_excel <- function(csv_path, saveDir) {
  first_line <- readLines(csv_path, n = 1)
  
  if (grepl(";", first_line)) {
    delim <- ";"
  } else if (grepl("\t", first_line)) {
    delim <- "\t"
  } else {
    delim <- ","  # Default to comma
  }
  
  df <- read_delim(csv_path, delim = delim)
  excel_path <- file.path(saveDir, paste0(tools::file_path_sans_ext(basename(csv_path)), ".xlsx"))
  
  write_xlsx(df, excel_path)
  return(excel_path) 
}

find_col_by_keyword <- function(cell_info, sheet_name, keyword) {
  return(cell_info %>%
           filter(sheet == sheet_name) %>%
           filter(grepl(keyword, character, ignore.case = TRUE)) %>%
           pull(col))
}