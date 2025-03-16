filePath <- here("data")
process_analytical_files <- function(conn) {
  # Retrieve all file names related to analytical data
  analyticalFiles <- get_file_names(filePath, "analytical")
  
  # Create necessary tables in the database
  create_chemical_table(conn)
  create_analyticaldata_table(conn)
  create_measurementdata_table(conn)
  
  # Process each analytical file
  for (file in analyticalFiles) {
    analytical_data <- prepare_analytical_data(file)
    chemical_ids <- insert_chemicals(conn, analytical_data)
    sample_ids <- insert_analyticalData(conn, analytical_data, chemical_ids)
    insert_measurementData(conn, analytical_data, sample_ids, chemical_ids)
  }
}


prepare_analytical_data <- function(file) {
  # Get all sheet names and the file as a tidyxl tibble
  full_file_path <- file.path(filePath, file)
  allsheetnames <- excel_sheets(full_file_path)
  chemicals_cell_info <- get_cell_infos(full_file_path, allsheetnames)
  
  sheet_data_list <- list()
  
  for (sheet_name in allsheetnames) {
    # Extract column positions for all headers in the current sheet 
    cas_no_col <- find_col_by_keyword(chemicals_cell_info, sheet_name, "cas no")
    ec_no_col <- find_col_by_keyword(chemicals_cell_info, sheet_name, "ec no")
    category_col <- find_col_by_keyword(chemicals_cell_info, sheet_name, "category")
    substance_col <- find_col_by_keyword(chemicals_cell_info, sheet_name, "substance")
    experiment_col <- find_col_by_keyword(chemicals_cell_info, sheet_name, "experiment")
    sample_col <- find_col_by_keyword(chemicals_cell_info, sheet_name, "sample")
    c_no_col <- find_col_by_keyword(chemicals_cell_info, sheet_name, "^c n")
    
    # Identify rows that contain actual data (not headers)
    analytical_rows <- chemicals_cell_info %>%
      filter(is_blank == FALSE) %>%
      filter(sheet == sheet_name) %>%
      filter(row != 1) %>%
      distinct(row) %>%
      pull(row) 
    
    # Add all header columns and the data rows to the analyticaldata list for later use
    sheet_data_list[[sheet_name]] <- list(
      cas_no_col = cas_no_col,
      ec_no_col = ec_no_col,
      category_col = category_col,
      substance_col = substance_col,
      experiment_col = experiment_col,
      sample_col = sample_col,
      c_no_col = c_no_col,
      analytical_rows = analytical_rows
    )
  }
  
  return(list(
    allsheetnames = allsheetnames,
    chemicals_cell_info = chemicals_cell_info,
    sheet_data = sheet_data_list
  ))
}

insert_chemicals <- function(conn, analytical_data) {
  # Extract the relevant data from the prepared data 
  chemicals_cell_info <- analytical_data$chemicals_cell_info
  sheet_names <- analytical_data$allsheetnames
  all_chemicals <- list()
  
  for (sheet_name in sheet_names) {
    # Extract the relevant data for chemicals from the prepared data 
    analytical_rows <- analytical_data$sheet_data[[sheet_name]]$analytical_rows
    cas_no_col <- analytical_data$sheet_data[[sheet_name]]$cas_no_col
    ec_no_col <- analytical_data$sheet_data[[sheet_name]]$ec_no_col
    substance_col <- analytical_data$sheet_data[[sheet_name]]$substance_col
    category_col <- analytical_data$sheet_data[[sheet_name]]$category_col
    
    # Go through each filled row 
    for (row_num in analytical_rows) {
      # Extract chemical information from each row 
      cas_no <- filter_sheet_row_col(chemicals_cell_info, sheet_name, row_num, cas_no_col[1]) %>% pull(character)
      ec_no <- filter_sheet_row_col(chemicals_cell_info, sheet_name, row_num, ec_no_col[1]) %>% pull(character)
      substance_name <- filter_sheet_row_col(chemicals_cell_info, sheet_name, row_num, substance_col[1]) %>% pull(character)
      category_name <- filter_sheet_row_col(chemicals_cell_info, sheet_name, row_num, category_col[1]) %>% pull(character)
      
      # Check if CAS and EC Number are not null, since they have NOT NULL condition 
      if (!is.null(cas_no) && !is.null(ec_no)) {
        # Add the data for one row to the list for later batch insertion 
        all_chemicals <- append(all_chemicals, list(
          list(
            cas_no = cas_no,
            substance_name = substance_name,
            ec_no = ec_no,
            category_name = category_name
          )
        ))
      }
    }
  }
  
  # Insert chemicals into database and return their IDs
  chemical_df <- bind_rows(all_chemicals)
  chemical_ids <- insert_chemicals_batch(conn, chemical_df)
  return(chemical_ids)
}

insert_analyticalData <- function(conn, analytical_data, chemical_ids) {
  chemicals_cell_info <- analytical_data$chemicals_cell_info
  sheet_names <- analytical_data$allsheetnames
  
  analyticals_cno <- list()
  analyticals <- list()
  for (sheet_name in sheet_names) {
    analytical_rows <- analytical_data$sheet_data[[sheet_name]]$analytical_rows
    ec_no_col <- analytical_data$sheet_data[[sheet_name]]$ec_no_col
    experiment_col <- analytical_data$sheet_data[[sheet_name]]$experiment_col
    sample_col <- analytical_data$sheet_data[[sheet_name]]$sample_col
    c_no_col <- analytical_data$sheet_data[[sheet_name]]$c_no_col
 
    for (row_num in analytical_rows) {
      experiment <- filter_sheet_row_col(chemicals_cell_info, sheet_name, row_num, experiment_col)%>%
        pull(numeric)
      ec_number <- filter_sheet_row_col(chemicals_cell_info, sheet_name, row_num, ec_no_col[1])%>%
        pull(character)
      sample <- filter_sheet_row_col(chemicals_cell_info, sheet_name, row_num, sample_col[1])%>%
        pull(character)
      c_no <- get_c_number_from_col(chemicals_cell_info, sheet_name, row_num, c_no_col[1])
      
      chemical_id <- chemical_ids %>%
        filter(ec_no == ec_number) %>%
        pull(chemicalid)
    
      
      if (is.null(c_no) || length(c_no) == 0) {
        analyticals <- append(analyticals, list(
          list(
            chemical_id = chemical_id,
            sample_name = sample,
            method = sheet_name,
            experiment = experiment
          )
        ))
      } else {
      analyticals_cno <- append(analyticals_cno, list(
        list(
          chemical_id = chemical_id,
          sample_name = sample,
          method = sheet_name,
          c_number = c_no,
          experiment = experiment
        )
      ))
      }
    }
  }
  
  sample_ids <- insert_analyticalData_batch(conn, analyticals_cno, analyticals)
  return(sample_ids)
 
}

get_sample_id <- function(df, chemicalid, method_name, c_no) {
  return(df %>%
    filter(chemical_id == chemicalid,
           method == method_name,
           (is.na(c_no) & is.na(c_number)) | (!is.na(c_no) & c_number == c_no)) %>%
    pull(sample_id))
}

insert_measurementData <- function(conn, analytical_data, sample_ids, chemical_ids) {
  # Extract the relevant data from the prepared data 
  chemicals_cell_info <- analytical_data$chemicals_cell_info
  chemicalsheetnames <- analytical_data$allsheetnames
  all_measurements <- list()
  
  for (sheet_name in chemicalsheetnames) {
    # Extract the relevant data for measurementdata from the prepared data 
    ec_no_col <- analytical_data$sheet_data[[sheet_name]]$ec_no_col
    c_no_col <- analytical_data$sheet_data[[sheet_name]]$c_no_col
    
    # Retrieve all measurement units present in the current sheet 
    measurement_units <- get_measurement_units(chemicals_cell_info, sheet_name, "wt%|mg/kg")
    
    # Go trough all measurement unit columns 
    for (j in 1:nrow(measurement_units)) {
      current_col <- measurement_units$col[j]
      current_unit <- measurement_units$character[j]
      
      # Retrieve the values for all rows for the current measurement unit 
      measurement_data <- get_measurement_data(chemicals_cell_info, sheet_name, current_col)
      
      # Go through all rows of the measurement values for the current unit
      for (k in 1:nrow(measurement_data)) {
        current_row <- measurement_data$row[k]
        # Retrieve values necessary for the foreign key sample id 
        ec_number <- filter_sheet_row_col(chemicals_cell_info, sheet_name, current_row, ec_no_col[1]) %>% pull(character)
        c_no <- get_c_number_from_col(chemicals_cell_info, sheet_name, current_row, c_no_col[1])
        if (length(c_no) == 0) { c_no <- NA }
        # Get the foreign key 
        chemical_id <- chemical_ids %>% filter(ec_no == ec_number) %>% pull(chemicalid)
        sampleid <- get_sample_id(sample_ids, chemical_id, sheet_name, c_no)
        
        # Add the current row to the data list for later batch insertion 
        all_measurements <- append(all_measurements, list(
          list(
            sample_id = sampleid,
            measurement = current_unit,
            value = measurement_data$numeric[k]
          )
        ))
      }
    }
  }
  
  # Insert all measurement data into database
  measurement_df <- bind_rows(all_measurements)
  dbWriteTable(conn, "measurementData", as.data.frame(measurement_df), append = TRUE, row.names = FALSE)
}

# Retreives the c Number value and handles special cases where C Number is numeric, 
# character or doesn't exist
get_c_number_from_col <- function(chemicals_cell_info, sheet_name, row, c_no_col) {
  return(filter_sheet_row_col(chemicals_cell_info, sheet_name, row, c_no_col[1]) %>%
           mutate(
             value = case_when(
               !is.na(numeric) ~ as.character(as.integer(numeric)),  
               !is.na(character) & character != "" ~ character,     
               TRUE ~ NA_character_                                 
             )
           ) %>%
           pull(value))
}

get_measurement_data <- function(chemicals_cell_info, sheet_name, current_col) {
  return(chemicals_cell_info %>%
           filter(is_blank == FALSE) %>%
           filter(row != 1) %>%
           filter(sheet == sheet_name) %>%
           filter(col == current_col) %>%
           select(sheet,row, col, data_type, numeric, character))
}

get_measurement_units <- function(chemicals_cell_info, sheet_name, pattern) {
  return(chemicals_cell_info %>%
    filter(is_blank == FALSE) %>%
    filter(row == 1) %>%
    filter(sheet == sheet_name) %>%
    filter(stri_detect_regex(character, pattern)) %>%
    select(sheet, col, character))
}
