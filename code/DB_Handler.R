

create_study_tables <- function(conn) {
  if ("endpoints" %in% dbListTables(conn)) {
    dbExecute(conn, "DROP TABLE endpoints")
  } 
  if ("animals" %in% dbListTables(conn)) {
    dbExecute(conn, "DROP TABLE animals")
  } 
  if ("groups" %in% dbListTables(conn)) {
    dbExecute(conn, "DROP TABLE groups")
  } 
  if ("studies" %in% dbListTables(conn)) {
    dbExecute(conn, "DROP TABLE studies")
  }
  
  # Create the `endpoints` table
  create_statement <- "
  CREATE TABLE endpoints (
    endpointid INTEGER PRIMARY KEY,
    animal_id INTEGER NOT NULL,
    endpoint TEXT NOT NULL,
    day TEXT,
    day_number INTEGER,
    value TEXT,
    unit TEXT,
    day_reference TEXT,
    method TEXT,
    FOREIGN KEY(animal_id) REFERENCES animals(animalid)
  );"
  dbExecute(conn, create_statement)
  
  # Create the `animals` table
  create_statement <- "
  CREATE TABLE animals (
    animalid INTEGER PRIMARY KEY,
    animalname TEXT NOT NULL,
    group_id INTEGER NOT NULL,
    study_id INTEGER NOT NULL,
    sex TEXT NOT NULL,
    FOREIGN KEY(group_id) REFERENCES groups(groupid),
    FOREIGN KEY(study_id) REFERENCES studies(studyid),
    UNIQUE(animalname, group_id, study_id)
  );"
  dbExecute(conn, create_statement)
  
  # Create the `groups` table
  create_statement <- "
  CREATE TABLE groups (
    groupid INTEGER PRIMARY KEY,
    groupname TEXT NOT NULL,
    study_id INTEGER NOT NULL,
    dose TEXT NOT NULL,
    unit TEXT NOT NULL,
    FOREIGN KEY(study_id) REFERENCES studies(studyid),
    UNIQUE(groupname, study_id)
  );"
  dbExecute(conn, create_statement)
  
  # Create the `studies` table
  create_statement <- "
  CREATE TABLE studies (
    studyid INTEGER PRIMARY KEY,
    studyname TEXT UNIQUE,
    species TEXT,
    study_duration TEXT,
    strain TEXT
  );"
  dbExecute(conn, create_statement)
  
  # Return the list of tables to verify creation
  RSQLite::dbListTables(conn)
}


create_chemical_table <- function(conn) {
  if ("chemical" %in% dbListTables(conn)) {
    dbExecute(conn, "DROP TABLE chemical")
  } 
  create_statement <- "
  CREATE TABLE chemical (
    chemicalid INTEGER PRIMARY KEY,
    cas_no TEXT NOT NULL,
    substance_name TEXT NOT NULL,
    ec_no TEXT NOT NULL,
    category_name TEXT,
    UNIQUE (cas_no, ec_no)
  );"
  dbExecute(conn, create_statement)
}

create_analyticaldata_table <- function(conn) {
  if ("analyticalData" %in% dbListTables(conn)) {
    dbExecute(conn, "DROP TABLE analyticalData")
  } 
  create_statement <- "
  CREATE TABLE analyticalData (
    sample_id INTEGER PRIMARY KEY,
    chemical_id INTEGER NOT NULL,
    sample_name TEXT NOT NULL,
    method TEXT NOT NULL,
    c_number TEXT,
    sample_date DATE,
    experiment INTEGER,
    FOREIGN KEY(chemical_id) REFERENCES chemical(chemicalid),
    UNIQUE (chemical_id, method, c_number)
  );"
  dbExecute(conn, create_statement)
}

create_measurementdata_table <- function(conn) {
  if ("measurementData" %in% dbListTables(conn)) {
    dbExecute(conn, "DROP TABLE measurementData")
  } 
  create_statement <- "
  CREATE TABLE measurementData (
    measurement_id INTEGER PRIMARY KEY,
    sample_id INTEGER NOT NULL,
    measurement TEXT NOT NULL,
    value REAL,
    
    FOREIGN KEY(sample_id) REFERENCES analyticalData(sample_id)
  );"
  dbExecute(conn, create_statement)
}

create_crossreference_table <- function(conn) {
  if ("cross_reference" %in% dbListTables(conn)) {
    dbExecute(conn, "DROP TABLE cross_reference")
  } 
  create_statement <- "
  CREATE TABLE cross_reference (
    cross_id INTEGER PRIMARY KEY,
    substudy TEXT NOT NULL,
    sample_name TEXT NOT NULL,
    ec_no TEXT NOT NULL,
    cas_no TEXT,
    category TEXT,
    
    FOREIGN KEY(cas_no) REFERENCES chemical(cas_no)
  );"
  dbExecute(conn, create_statement)
}

create_metabolomics_table <- function(conn) {
  if ("metabolomics_data" %in% dbListTables(conn)) {
    dbExecute(conn, "DROP TABLE metabolomics_data")
  } 
  create_statement <- "
  CREATE TABLE metabolomics_data (
    metabolomics_id INTEGER PRIMARY KEY,
    animal_id INTEGER NOT NULL,
    metabolite_name TEXT NOT NULL,
    value REAL NOT NULL,
    unit TEXT,
    analyte_id TEXT,
    alias_id TEXT,
    substudy TEXT NOT NULL,
    
    FOREIGN KEY(animal_id) REFERENCES animals(animal_id),
    FOREIGN KEY(substudy) REFERENCES cross_reference(substudy)
  );"
  dbExecute(conn, create_statement)
}

insert_animals_batch <- function(conn, animal_df, study_id) {
  animal_df <- animal_df %>%
    distinct(animalname, study_id, group_id, .keep_all = TRUE)
  dbWriteTable(conn, "animals", as.data.frame(animal_df), append = TRUE, row.names = FALSE)
  animal_ids <- dbGetQuery(conn, "
    SELECT animalname, study_id, group_id, animalid
    FROM animals
    WHERE study_id = ?;
  ", params = list(study_id))
 
  animal_df_ids <- left_join(animal_df, animal_ids, by = c("animalname", "study_id", "group_id"))
  return(animal_df_ids)
}

insert_study <- function(conn, studyFile) {
  study_name <- tools::file_path_sans_ext(basename(studyFile))
  insert_statement <- "INSERT INTO studies (studyname) VALUES (?) ON CONFLICT(studyname) DO NOTHING;"
  dbExecute(conn, insert_statement, params = list(study_name))
  query <- "SELECT studyid FROM studies WHERE studyname = ?;"
  study_id <- dbGetQuery(conn, query, params = list(study_name))$studyid
  
  return(study_id)
}

insert_groups_batch <- function(conn, group_df, study_id) {
  # Insert the batch of groups into the database
  group_df <- distinct(group_df, groupname, study_id, .keep_all = TRUE)
  dbWriteTable(conn, "groups", group_df, append = TRUE, row.names = FALSE)
  # Fetch the group_ids from the database, using the study_id and groupname
  group_ids <- dbGetQuery(conn, "
    SELECT groupname, study_id, groupid 
    FROM groups
    WHERE study_id = ?;
  ", params = list(study_id))
  # Merge the group_ids back into the original group_df
  group_df_ids <- left_join(group_df, group_ids, by = c("groupname", "study_id"))
  # Return the updated group_df with the group_id
  return(group_df_ids)
}

insert_chemicals_batch <- function(conn, chemical_df) {
  chemical_df <- chemical_df %>%
    distinct(cas_no, ec_no, .keep_all = TRUE)
  dbWriteTable(conn, "chemical", as.data.frame(chemical_df), append = TRUE, row.names = FALSE)
  chemical_ids <- dbGetQuery(conn, "
    SELECT chemicalid, cas_no, ec_no
    FROM chemical;
  ")
  chemical_df_ids <- left_join(chemical_df, chemical_ids, by = c("cas_no", "ec_no"))
  return(chemical_df_ids)
}

insert_analyticalData_batch <- function(conn, analyticals_cno, analyticals) {
  analytical_cno_df <- bind_rows(analyticals_cno)
  analytical_df <- bind_rows(analyticals)
  analytical_cno_df <- analytical_cno_df %>%
    distinct(chemical_id, method, c_number, .keep_all = TRUE)
  analytical_df <- analytical_df %>%
    distinct(chemical_id, method, .keep_all = TRUE)
  dbWriteTable(conn, "analyticalData", analytical_df, append = TRUE, row.names = FALSE)
  dbWriteTable(conn, "analyticalData", analytical_cno_df, append = TRUE, row.names = FALSE)
  
  sample_ids <- dbGetQuery(conn, "
    SELECT sample_id, chemical_id, method, c_number
    FROM analyticalData;
  ")
  sample_ids_cno <- left_join(analytical_cno_df, sample_ids, 
                                     by = c("chemical_id", "method", "c_number"))
  
  sample_ids <- left_join(analytical_df, sample_ids, 
                                 by = c("chemical_id", "method"))
  
  # Ensure consistency in column structure
  sample_ids$c_number <- NA_character_  # Add missing c_number column for consistency
  
  # Combine both data frames
  final_analytical_df <- bind_rows(sample_ids, sample_ids_cno)
  
  return(final_analytical_df)
}

get_animal_ids <- function(conn) {
  return(dbGetQuery(conn, "
    SELECT a.animalid, a.animalname, s.studyname
    FROM animals a
    JOIN studies s ON a.study_id = s.studyid;
  "))
}

insert_crossreference_data <- function(conn, substudy, sample_name, ec_no, cas_no, category) {
  insert_statement <- "
      INSERT INTO cross_reference (substudy, sample_name, ec_no, cas_no, category)
      VALUES (?, ?, ?, ?, ?);
    "
  dbExecute(conn, insert_statement, params = list(substudy, sample_name, ec_no, cas_no, category))
}

