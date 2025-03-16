process_study_files <- function(conn) {
  studyFiles <- get_file_names(filePath, "study")
  create_study_tables(conn)
  
  # Iterate over each study file and process its data
  for (file in studyFiles) {
    # Prepare the data for the current study file
    study_data <- prepare_study_data(file)
    
    study_id <- insert_study(conn, file)
    study_data$study_id <- study_id  
    group_ids <- process_groups(conn, study_data)
    animal_ids <- process_animals(conn, study_data, group_ids)
    process_endpoints(conn, study_data, animal_ids)
  }
}

# Function to prepare the study data from a given study file
prepare_study_data <- function(studyFile) {
  full_file_path <- file.path(filePath, studyFile)
  allsheetnames <- excel_sheets(full_file_path)
  
  # Retrieve the tidyxl tibble containing all non-empty cells 
  study_cells_info <- get_cell_infos(full_file_path, allsheetnames) %>% 
    filter(is_blank == FALSE)
  
  # Identify the rows in the file where each subtable starts by searching for the pattern "Sex:"
  tablerowstarts <- study_cells_info %>% 
    filter(grepl("Sex:", character)) %>% 
    select(sheet, row, character)  %>%
    mutate( # add a sex column to tablerowstarts and assign male or female (or NA) for later retrieval 
      sex = case_when(
        grepl("female", character, ignore.case = TRUE) ~ "female",
        grepl("male", character, ignore.case = TRUE) ~ "male",
        TRUE ~ NA_character_
      )
    )
  # Identify the rows containing group information (group name, dose, unit)
  grouprowstarts <- study_cells_info %>% 
    filter(grepl("G\\d+", character, perl = TRUE)) %>% # Matches any cell starting with "G1", "G2", etc.
    select(sheet, row, character) %>%
    mutate(
      split_info = strsplit(character, " "),  # Split the group info into name, dose, and unit
      name_dose = sapply(split_info, function(x) x[1]),
      groupunit = sapply(split_info, function(x) gsub("\r\n", "", x[2])),
      split_name_dose = strsplit(name_dose, "\r\n"),
      groupname = sapply(split_name_dose, function(x) x[1]),
      dose = sapply(split_name_dose, function(x) x[2])
    )
  # Return the study data as a list of relevant information
  return(list(
    tablerowstarts = tablerowstarts,
    study_cells_info = study_cells_info, 
    all_sheetnames = allsheetnames,
    grouprowstarts = grouprowstarts
  ))
}


process_groups <- function(conn, study_data) {
  # Get the group start rows from the study data
  grouprowstarts <- study_data$grouprowstarts
  
  all_groups <- list()
  # Loop through each group, retrieve the field values and append to the list for later insertion 
  for (i in 1:nrow(grouprowstarts)) {
    all_groups <- append(all_groups, list(
      list(
        groupname = grouprowstarts$groupname[i],
        study_id = study_data$study_id,  # Foreign key the current study ID
        dose = grouprowstarts$dose[i],
        unit = grouprowstarts$groupunit[i]
      )
    ))
  }
  # Combine the group data into a data frame and insert it into the database
  group_df <- bind_rows(all_groups)  
  group_df_ids <- insert_groups_batch(conn, group_df, study_data$study_id)
  
  return(group_df_ids)
}


process_animals <- function(conn, study_data, group_ids) {
  studyid <- study_data$study_id
  tablerowstarts <- study_data$tablerowstarts
  grouprowstarts <- study_data$grouprowstarts
  study_cells_info <- study_data$study_cells_info
  all_animals <- list()

  # Loop through each group and process the animals in that group
  for (i in 1:nrow(grouprowstarts)) {
    current_sheet <- grouprowstarts$sheet[i]
    current_row <- grouprowstarts$row[i]
    group_name <- grouprowstarts$groupname[i]
  
    # Get the sex of the animals in the group
    sex <- get_animal_sex(tablerowstarts, current_sheet, current_row)
    
    # Get the group ID for the current group by groupname and studyid 
    group_id <- group_ids %>%
      filter(groupname == group_name) %>%
      filter(study_id == studyid) %>%
      pull(groupid)
    
    # Get the row where the next group starts for the current sheet or if last Inf
    next_group_start <- get_next_row(grouprowstarts, i, current_sheet)
    
    # Get all animal rows that are between the current and the next group row 
    animalRowstarts <- get_animalrowStarts(study_cells_info, current_sheet, current_row, next_group_start)
    # Loop through each animal in the group and collect their information
    for (j in 1:nrow(animalRowstarts)) {
      animal_name <- stri_trim(animalRowstarts$character[j])  # Clean up animal name
      all_animals <- append(all_animals, list(
        list(
          animalname = animal_name,
          group_id = group_id,
          study_id = studyid,
          sex = sex
        )
      ))
    }
  }
  # Combine the animal data into a data frame and insert it into the database
  animal_df <- bind_rows(all_animals)
  animal_df_ids <- insert_animals_batch(conn, animal_df, studyid)
  
  return(animal_df_ids) 
}

get_animal_sex <- function(tablerowstarts, current_sheet, current_row) {
  # Find the row in tablerowstarts corresponding to the current animal
  start_rows <- tablerowstarts %>% 
    filter(sheet == current_sheet & row < current_row) %>%
    pull(row)
  start_row <- max(start_rows) 
  
  # Return the sex of the animal from the corresponding row
  return(tablerowstarts %>% 
           filter(sheet == current_sheet) %>%
           filter(row == start_row) %>%
           pull(sex))
}

# Function to get the start rows for animals for each group 
get_animalrowStarts <- function(study_cells_info, current_sheet, current_row, next_row) {
  # Matches all cells starting with "animal" that are between the current group start and the next 
  return(study_cells_info %>%
           filter(sheet == current_sheet) %>%
           filter(row > current_row) %>%
           filter(row < next_row) %>%
           filter(grepl("animal\\s*.*\\d+", character, ignore.case = TRUE)) %>%
           select(row, character))
}

# Function to get the next group start row 
get_next_row <- function(df, i, current_sheet) {
  if (i < nrow(df) && df$sheet[i + 1] == current_sheet) {
    next_row <- df$row[i + 1]
  } else {
    next_row <- Inf  # Set to Inf if the current group is the last in the sheet 
  }
  return(next_row)
}


process_endpoints <- function(conn, study_data, animal_ids) {
  studyid <- study_data$study_id
  tablerowstarts <- study_data$tablerowstarts
  grouprowstarts <- study_data$grouprowstarts
  study_cells_info <- study_data$study_cells_info
  endpoint_data <- list()
  
  # Loop through each group and process the endpoint metadata 
  for (i in 1:nrow(grouprowstarts)) {
    current_sheet <- grouprowstarts$sheet[i]
    current_row <- grouprowstarts$row[i]
    # Get the row where the next group starts for the current sheet or if last Inf
    next_group_row <- get_next_row(grouprowstarts, i, current_sheet)
    
    # Get the rows for the endpoint metadata cells 
    endpointRowstarts <- study_cells_info %>%
      filter(sheet == current_sheet) %>%
      filter(row > current_row) %>% 
      slice_min(row, n = 1) 
    
    # Get the starting row for animals in the current group 
    animalRowstarts <- get_animalrowStarts(study_cells_info, current_sheet, current_row, next_group_row)
    
    # Loop through each endpoint metadata cell
    for (j in 1:nrow(endpointRowstarts)) {
      current_col <- endpointRowstarts$col[j]
      endpoint <- endpointRowstarts$character[j]
      
      # Extract endpoint information (name, unit, day)
      endpoint_info <- extract_endpoint_info(endpoint)
      
      # Loop through each animal in the current group 
      for (k in 1:nrow(animalRowstarts)) {
        current_row <- animalRowstarts$row[k]
        animal_name <- stri_trim(animalRowstarts$character[k])
        
        # Get the animal ID
        animal_id <- animal_ids %>%
          filter(animalname == animal_name) %>%
          filter(study_id == studyid) %>%
          pull(animalid)
        
        # Get the endpoint value for the current animal
        endpoint_value <- filter_sheet_row_col(study_cells_info, current_sheet, current_row, current_col) %>% 
          select(row, col, data_type, numeric, date, character)
        value <- get_endpoint_value(endpoint_value) 
        
        # Append the endpoint data to the list
        endpoint_data <- append(endpoint_data, list(
          list(
            animal_id = animal_id,
            endpoint = endpoint_info$endpoint_name,
            day = as.character(endpoint_info$day),
            day_number = as.numeric(endpoint_info$day_number),
            value = as.character(value),
            unit = as.character(endpoint_info$endpoint_unit)
          )
        ))
      }
    }
  }
  
  # Combine the endpoint data into a data frame and insert it into the database
  endpoint_df <- bind_rows(endpoint_data)
  if (nrow(endpoint_df) > 0) {
    dbWriteTable(conn, "endpoints", endpoint_df, append = TRUE, row.names = FALSE)
  }
}

# Function to get the value of an endpoint (numeric, date, or character)
get_endpoint_value <- function(endpoint) {
  if (nrow(endpoint) > 0) {
    if (!is.na(endpoint$numeric)) {
      return(endpoint$numeric)  # Return numeric value if numeric
    } else if (!is.na(endpoint$date)) {
      return(endpoint$date)  # Return date value if date
    } else {
      return(endpoint$character)  # Return character value if character
    }
  } else {
    return(NA)  # Return NA if no value is found
  }
}

# Function to extract endpoint information (name, unit, day)
extract_endpoint_info <- function(endpoint) {
  # Extract the endpoint name from the string by extracting the first part of the string separated by \r\n
  endpoint_name <- strsplit(endpoint, "[\r\n]")[[1]][1]
  
  # Remove the endpoint name to get the rest of the information (unit and day)
  endpoint_unit_day <- sub(paste0("^", endpoint_name), "", endpoint)
  unit_day_cleaned <- gsub("^[\r\n]+", "", endpoint_unit_day)
  
  # Extract the endpoint unit and day by separating again by \r\n
  endpoint_unit <- strsplit(unit_day_cleaned, "[\r\n]")[[1]][1]
  endpoint_day <- sub(endpoint_unit, "", unit_day_cleaned, fixed = TRUE)
  endpoint_day_cleaned <- stri_replace_all_regex(endpoint_day, "[\r\n]", "")
  
  # Check if the day is a valid number
  if (!is.na(as.numeric(endpoint_day_cleaned))) {
    day_number <- endpoint_day_cleaned
  } else {
    day_number <- NA  # Set to NA if not a valid number
  }
  
  return(list(endpoint_name = endpoint_name, 
              endpoint_unit = endpoint_unit, 
              day_number = day_number,
              day = endpoint_day_cleaned))
}
