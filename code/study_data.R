filePath <- here("data")

process_study_files <- function(conn) {
  studyFiles <- get_file_names(filePath, "study")
  create_study_tables(conn)
  for (file in studyFiles) {
    study_data <- prepare_study_data(file)
    study_id <- insert_study(conn, file)
    study_data$study_id <- study_id
    group_ids <- process_groups(conn, study_data)
    animal_ids <- process_animals(conn, study_data, group_ids)
    process_endpoints(conn, study_data, animal_ids)
  }
}

prepare_study_data <- function(studyFile) {
  full_file_path <- file.path(filePath, studyFile)
  allsheetnames <- excel_sheets(full_file_path)
  study_cells_info <- get_cell_infos(full_file_path, allsheetnames) %>% 
    filter(is_blank == FALSE)

  tablerowstarts <- study_cells_info %>% 
    filter(grepl("Sex:", character)) %>% 
    select(sheet, row, character) %>%
    mutate(
      sex = case_when(
        grepl("male", character, ignore.case = TRUE) ~ "male",
        grepl("female", character, ignore.case = TRUE) ~ "female",
        TRUE ~ NA_character_
      )
    )
  grouprowstarts <- study_cells_info %>% 
    filter(grepl("G\\s*\\d+\\s*\\d+\\s*[A-Za-z]+\\s*/\\s*[^/]+\\s*/\\s*[^/]+", character, perl = TRUE)) %>% 
    select(sheet, row, character)%>%
    mutate(
      split_info = strsplit(character, " "),
      name_dose = sapply(split_info, function(x) x[1]),
      groupunit = sapply(split_info, function(x) gsub("\r\n", "", x[2])),
      split_name_dose = strsplit(name_dose, "\r\n"),
      groupname = sapply(split_name_dose, function(x) x[1]),
      dose = sapply(split_name_dose, function(x) x[2])
    )
  
  list(
    tablerowstarts = tablerowstarts,
    study_cells_info = study_cells_info, 
    all_sheetnames = allsheetnames,
    grouprowstarts = grouprowstarts
  )
}


process_groups <- function(conn, study_data) {
  grouprowstarts <- study_data$grouprowstarts
  all_groups <- list()
  
  for (i in 1:nrow(grouprowstarts)) {
    all_groups <- append(all_groups, list(
      list(
        groupname = grouprowstarts$groupname[i],
        study_id = study_data$study_id,
        dose = grouprowstarts$dose[i],
        unit = grouprowstarts$groupunit[i]
      )
    ))
  }
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
  for (i in 1:nrow(grouprowstarts)) {
    current_sheet <- grouprowstarts$sheet[i]
    current_row <- grouprowstarts$row[i]
    group_name <- grouprowstarts$groupname[i]
    
    sex <- get_animal_sex(tablerowstarts, current_sheet, current_row)
    group_id <- group_ids %>%
      filter(groupname == group_name) %>%
      filter(study_id == studyid) %>%
      pull(groupid)

    next_row <- get_next_row(grouprowstarts, i, current_sheet)
  
    animalRowstarts <- get_animalrowStarts(study_cells_info, current_sheet, current_row, next_row)
    for (j in 1:nrow(animalRowstarts)) {
      animal_name <- stri_trim(animalRowstarts$character[j])
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
  animal_df <- bind_rows(all_animals)
  animal_df_ids <- insert_animals_batch(conn, animal_df, studyid)
  return(animal_df_ids)
}


get_animal_sex <- function(tablerowstarts, current_sheet, current_row) {
  start_rows <- tablerowstarts %>% 
    filter(sheet == current_sheet & row < current_row) %>%
    pull(row)
  start_row <- max(start_rows)
  return(tablerowstarts %>% 
    filter(sheet == current_sheet) %>%
    filter(row == start_row) %>%
    pull(sex))
}


get_animalrowStarts <- function(study_cells_info, current_sheet, current_row, next_row) {
  return(study_cells_info %>%
           filter(sheet == current_sheet) %>%
           filter(row > current_row) %>%
           filter(row < next_row) %>%
           filter(grepl("animal\\s*.*\\d+", character, ignore.case = TRUE)) %>%
           select(row, character))
}


get_next_row <- function(df, i, current_sheet) {
  if (i < nrow(df) && df$sheet[i + 1] == current_sheet) {
    next_row <- df$row[i + 1]
  } else {
    next_row <- Inf 
  }
  return(next_row)
}


process_endpoints <- function(conn, study_data, animal_ids) {
  studyid <- study_data$study_id
  tablerowstarts <- study_data$tablerowstarts
  grouprowstarts <- study_data$grouprowstarts
  study_cells_info <- study_data$study_cells_info
  endpoint_data <- list()
  for (i in 1:nrow(grouprowstarts)) {
    current_sheet <- grouprowstarts$sheet[i]
    current_row <- grouprowstarts$row[i]
    next_row <- get_next_row(grouprowstarts, i, current_sheet)
    
    endpointRowstarts <- study_cells_info %>%
      filter(sheet == current_sheet) %>%
      filter(row > current_row) %>% 
      slice_min(row, n = 1) 
    animalRowstarts <- get_animalrowStarts(study_cells_info, current_sheet, current_row, next_row)
    for (j in 1:nrow(endpointRowstarts)) {
      current_col <- endpointRowstarts$col[j]
      endpoint <- endpointRowstarts$character[j]
      endpoint_info <- extract_endpoint_info(endpoint)
      
      for (k in 1:nrow(animalRowstarts)) {
        current_row <- animalRowstarts$row[k]
        animal_name <- stri_trim(animalRowstarts$character[k])
        animal_id <- animal_ids %>%
          filter(animalname == animal_name) %>%
          filter(study_id == studyid) %>%
          pull(animalid)
        
        endpoint_value <- filter_sheet_row_col(study_cells_info, current_sheet, current_row, current_col) %>% 
          select(row, col, data_type, numeric, date, character)
      
        value <- get_endpoint_value(endpoint_value)
    
        endpoint_data <- append(endpoint_data, list(
          list(
          animal_id = animal_id,
          endpoint = endpoint_info$endpoint_name,
          day = as.character(endpoint_info$day),
          day_number = as.numeric(endpoint_info$day_number),
          value = as.character(value),
          unit = as.character(endpoint_info$endpoint_unit)
        )))
        
       
      }
    }
  }
  endpoint_df <- bind_rows(endpoint_data)
  if (nrow(endpoint_df) > 0) {
    dbWriteTable(conn, "endpoints", endpoint_df, append = TRUE, row.names = FALSE)
  }
  
}




get_endpoint_value <- function(endpoint) {
  if (nrow(endpoint) > 0) {
    if (!is.na(endpoint$numeric)) {
      return(endpoint$numeric)
    } else if (!is.na(endpoint$date)) {
      return(endpoint$date)
    } else {
      return(endpoint$character)
    }
  } else {
    return(NA)  
  }
}

extract_endpoint_info <- function(endpoint) {
  endpoint_name <- strsplit(endpoint, "[\r\n]")[[1]][1]
  
  endpoint_unit_day <- sub(paste0("^", endpoint_name), "", endpoint)
  
  unit_day_cleaned <- gsub("^[\r\n]+", "", endpoint_unit_day)
  
  endpoint_unit <- strsplit(unit_day_cleaned, "[\r\n]")[[1]][1]
  
  endpoint_day <- sub(endpoint_unit, "", unit_day_cleaned, fixed = TRUE)
  endpoint_day_cleaned <- stri_replace_all_regex(endpoint_day, "[\r\n]", "")
  
  if (!is.na(as.numeric(endpoint_day_cleaned))) {
    day_number <- endpoint_day_cleaned
  } else {
    day_number <- NA  # If it's not a valid number, set as NA
  }
  
  return(list(endpoint_name = endpoint_name, 
              endpoint_unit = endpoint_unit, 
              day_number = day_number,
              day = endpoint_day_cleaned))
}
