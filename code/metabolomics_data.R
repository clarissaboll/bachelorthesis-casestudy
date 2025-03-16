filePath <- here("data")
process_metabolomics_files <- function(conn) {
  # Get all metabolomics file names in the specified directory
  metabolomicsFiles <- get_file_names(filePath, "metabolom")
  
  # Create the metabolomics table in the database 
  create_metabolomics_table(conn)
  
  # Loop through each file and process the metabolomics data
  for (file in metabolomicsFiles) {
    insert_metabolomics(file, conn)
  }
}

insert_metabolomics <- function(file, conn) {
  # Construct full file path and get sheet names
  full_file_path <- file.path(filePath, file)
  allsheetnames <- excel_sheets(full_file_path)
  metabolomics_cells_info <- get_cell_infos(full_file_path, allsheetnames)
  all_metabolomics <- list()
  
  for (sheet_name in allsheetnames) {
    metabolome_cells_info <- get_cell_infos(full_file_path, sheet_name)
    
    # Retrieve row positions for all headers
    animal_id_row <- get_header_position(metabolome_cells_info, sheet_name, "animal") %>% pull(row) %>% first()
    alias_id_row <- get_header_position(metabolome_cells_info, sheet_name, "alias") %>% pull(row) %>% first()
    analyte_id_row <- get_header_position(metabolome_cells_info, sheet_name, "analyte") %>% pull(row) %>% first()
    metabolite_row <- get_header_position(metabolome_cells_info, sheet_name, "metabolite") %>% pull(row) %>% first()
    substudy_row <- get_header_position(metabolome_cells_info, sheet_name, "substudy") %>% pull(row) %>% first()
    starting_row <- get_header_position(metabolome_cells_info, sheet_name, "analyte|metabolite|alias") %>% pull(row) %>% unique()
    
    # Retrieve column positions for the headers analyteid and metabolitename
    analyte_id_col <- get_header_position(metabolome_cells_info, sheet_name, "analyte") %>% pull(col)
    metabolite_col <- get_header_position(metabolome_cells_info, sheet_name, "metabolite") %>% pull(col)
    
    # Get all filled rows and columns 
    filled_rows <- get_filled_rows(metabolome_cells_info, sheet_name, starting_row + 1)
    filled_cols <- get_filled_cols(metabolome_cells_info, sheet_name, 3)
    
    # Retrieve analyteid and metabolitename values as data frame
    analyte_info <- get_analyte_info(metabolome_cells_info, sheet_name, filled_rows, analyte_id_col, metabolite_col, analyte_id_row, metabolite_row)
    
    animal_ids <- get_animal_ids(conn)
    
    # Process each column
    for (i in seq_along(filled_cols)) {
      current_col <- filled_cols[i]
      
      # Extract metadata values for the current column
      substudy <- filter_character_from_row_col(metabolome_cells_info, sheet_name, substudy_row, current_col)
      animal_name <- filter_character_from_row_col(metabolome_cells_info, sheet_name, animal_id_row, current_col)
      
      # Match animal name with an ID from the database
      animalid <- animal_ids %>%
        filter(animalname == animal_name) %>%
        pull(animalid)
      
      # Retrieve alias ID
      aliasid <- filter_sheet_row_col(metabolome_cells_info, sheet_name, alias_id_row, current_col) %>%
        pull(numeric) %>%
        as.integer()
      
      # Process each row in filled_rows within the current column
      for (current_row in filled_rows) {
        metabolitename <- get_value_from_analyte_info(analyte_info, "metabolite_name", current_row)
        analyteid <- get_value_from_analyte_info(analyte_info, "analyte_id", current_row)
        
        # Extract the numeric value from the current row 
        value <- filter_sheet_row_col(metabolome_cells_info, sheet_name, current_row, current_col) %>%
          pull(numeric)
        
        # Store extracted data in a list
        all_metabolomics <- append(all_metabolomics, list(
          list(
            animal_id = animalid,
            metabolite_name = metabolitename,
            value = value,
            analyte_id = analyteid, 
            alias_id = aliasid,
            substudy = substudy
          )
        ))
      }
    }
  }
  
  # Convert list into a dataframe and write it to the database
  metabolomics_df <- bind_rows(all_metabolomics)
  
  if (nrow(metabolomics_df) > 0) {
    dbWriteTable(conn, "metabolomics_data", metabolomics_df, append = TRUE, row.names = FALSE)
  }
}

# Function to get values from the analyte_info table dynamically
get_value_from_analyte_info <- function(analyte_info, column_name, current_row) {
  if (column_name %in% colnames(analyte_info)) {
    value <- analyte_info %>%
      filter(row == current_row) %>%
      pull(!!sym(column_name))
  } else {
    value <- NA
  }
  return(value)
}

# Function to get the row position of a given header in the sheet
get_header_position <- function(metabolome_cells_info, Metabolomesheetname, name) {
  return(metabolome_cells_info %>%
           filter(sheet == Metabolomesheetname) %>%
           filter(grepl(name, character, ignore.case = TRUE)))
}

# Function to retrieve filled rows 
get_filled_rows <- function(metabolome_cells_info, current_sheet, start) {
  return(metabolome_cells_info %>%
           filter(sheet == current_sheet) %>%
           filter(is_blank == FALSE) %>%
           filter(row >= start) %>%
           pull(row) %>%
           unique())
}

# Function to retrieve filled columns
get_filled_cols <- function(metabolome_cells_info, current_sheet, start) {
  return(metabolome_cells_info %>%
           filter(sheet == current_sheet) %>%
           filter(is_blank == FALSE) %>%
           filter(col >= start) %>%
           pull(col) %>%
           unique())
}

# Function to retrieve analyteid and metabolitename values for all rows 
get_analyte_info <- function(metabolome_cells_info, current_sheet, filled_rows, analyte_id_col, metabolite_col, analyte_id_row, metabolite_row) {
  valid_cols <- c(analyte_id_col, metabolite_col)
  
  if (length(valid_cols) > 0) {
    valid_cols <- valid_cols[!is.na(valid_cols)]  
  }
  
  analyte_info <- metabolome_cells_info %>%
    filter(sheet == current_sheet) %>%
    filter(row %in% filled_rows) %>%  
    filter(col %in% valid_cols) %>%  
    select(row, col, character) %>% 
    pivot_wider(names_from = col, values_from = character, names_prefix = "col_") 
  
  column_mappings <- c()
  
  if (!is.na(analyte_id_row)) {
    column_mappings[paste0("col_", analyte_id_col)] <- "analyte_id"
  }
  if (!is.na(metabolite_row)) {
    column_mappings[paste0("col_", metabolite_col)] <- "metabolite_name"
  }
  analyte_info <- analyte_info %>%
    rename_with(~ column_mappings[.x], names(column_mappings))
  
  return(analyte_info)
}
