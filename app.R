library(readxl)
library(shiny)
library(shinydashboard)
library(tools)
library(DT)
library(ggplot2)
library(dplyr)
library(plotly)
library(here)
library(tidyxl)
library(tidyverse)
library(RSQLite)
library(stringi)
library(DBI)
library(readr)
library(writexl)

source("code/study_data.R")
source("code/DB_Handler.R")
source("code/Excel_Handler.R")
source("code/analytical_data.R")
source("code/crossreference_data.R")
source("code/metabolomics_data.R")

conn <- dbConnect(SQLite(), "resources/case_study.sqlite")
filePath <- here("data")

ui <- dashboardPage(
  dashboardHeader(title = "Study Dashboard"),
  dashboardSidebar(
    sidebarMenu(
      menuItem("Upload/Download Data", tabName = "upload", icon = icon("upload")),
      menuItem("Study Data", tabName = "study_data", icon = icon("table")),
      menuItem("Study Data Details", tabName = "study_data_details", icon = icon("chart-pie")),
      menuItem("Chemical Data", tabName = "chemical_data", icon = icon("table")),
      menuItem("Analytical Data", tabName = "analytical_data", icon = icon("table")),
      menuItem("Metabolomics Data", tabName = "metabolomics_data", icon = icon("table")),
      menuItem("Crossreference Data", tabName = "crossreference_data", icon = icon("table"))
    )
  ),
  dashboardBody(
    tabItems(
      # Upload Data Tab
      tabItem(
        tabName = "upload",
        fluidRow(
          box(
            title = "Upload Folder (ZIP File)",
            fileInput(
              inputId = "zipFile",
              label = "Upload a ZIP file containing Excel or CSV files",
              multiple = FALSE,
              accept = c(".zip")
            ),
            helpText("Ensure the ZIP file contains only Excel (.xlsx) or CSV files."),
            width = 6
          ),
          box(
            title = "Files in Uploaded ZIP",
            verbatimTextOutput("fileList"),
            actionButton(
              inputId = "processFiles",
              label = "Insert in Database",
              icon = icon("database"),
              style = "margin-top: 10px;"
            ),
            width = 6
          ), 
          box(
            title = "Download Database",
            downloadButton("download_db", "Download SQLite File", class = "btn btn-primary"),
            width = 6
          )
          
        )
      ),
      tabItem(
        tabName = "study_data",
        fluidRow(
          box(title = "Studies Table", DTOutput("studiesTable"), width = 12),
          box(title = "Groups Table", DTOutput("groupsTable"), width = 12),
          box(title = "Animals Table", DTOutput("animalsTable"), width = 12),
          box(title = "Endpoints Table", DTOutput("endpointsTable"), width = 12)
        )
      ), 
      tabItem(
        tabName = "study_data_details",
        fluidRow(
          box(
            title = "Animal Distribution by Sex",
            plotOutput("sexDistribution"),
            width = 6
          ),
          box(
            title = "Groups per Study",
            plotOutput("groupsPerStudy"),
            width = 6
          ),
          box(
            title = "Bubble Chart: Table Record Counts",
            plotlyOutput("bubblePlot"),
            width = 12
          )
          
        )
      ),
      tabItem(
        tabName = "chemical_data",
        fluidRow(
          box(title = "Chemicals Table", DTOutput("chemicalsTable"), width = 12)
        )
      ),
      tabItem(
        tabName = "analytical_data",
        fluidRow(
          box(title = "Analytical Data Table", DTOutput("analyticalTable"), width = 12),
          box(title = "Measurement Data Table", DTOutput("measurementTable"), width = 12)
        )
      ),
      tabItem(
        tabName = "metabolomics_data",
        fluidRow(
          box(title = "Metabolomics Table", DTOutput("metabolomicsTable"), width = 12)
        )
      ),
      tabItem(
        tabName = "crossreference_data",
        fluidRow(
          box(title = "Crossreference Table", DTOutput("crossreferenceTable"), width = 12)
        )
      )
    )
  )
)

# Server
server <- function(input, output, session) {
  # Directory to save processed files
  saveDir <- "data"
  if (!dir.exists(saveDir)) dir.create(saveDir, recursive = TRUE)
  
  extractedFiles <- reactive({
    req(input$zipFile)
    
    tempDir <- tempfile()
    dir.create(tempDir)
    unzip(input$zipFile$datapath, exdir = tempDir)
    
    allFiles <- list.files(tempDir, full.names = TRUE, recursive = TRUE)
    cleanFiles <- allFiles[!grepl("__MACOSX|.DS_Store", allFiles)]
    validFiles <- cleanFiles[file_ext(cleanFiles) %in% c("xlsx", "xls", "csv")]
    
    # Process CSV files 
    convertedFiles <- c()
    for (file in validFiles) {
      if (file_ext(file) == "csv") {
        convertedFiles <- c(convertedFiles, convert_csv_to_excel(file, saveDir))
      } else {
        file.copy(file, saveDir, overwrite = TRUE)
        convertedFiles <- c(convertedFiles, file.path(saveDir, basename(file)))
      }
    }
    
    return(convertedFiles)
  })
  
  output$fileList <- renderText({
    files <- extractedFiles()
    if (length(files) == 0) {
      "No Excel or CSV files found in the ZIP."
    } else {
      paste(basename(files), collapse = "\n")
    }
  })
  observeEvent(input$processFiles, {
    files <- extractedFiles()
    if (length(files) == 0) {
      showNotification("No files to process. Please upload a ZIP file.", type = "error")
    } else {
      showNotification("Processing files... Please wait.", type = "message")
      
      tryCatch({
        process_study_files(conn)
        process_analytical_files(conn)
        process_crossreference_files(conn)
        process_metabolomics_files(conn)
        
        showNotification("Files successfully inserted into the database!", type = "message")
      }, error = function(e) {
        showNotification(paste("Error:", e$message), type = "error")
      })
    }
  })
  
  
  animals <- reactive({
    query <- "SELECT animalid, sex FROM animals"
    dbGetQuery(conn, query)
  })
  
  tableCounts <- reactive({
    tryCatch({
      data.frame(
        Table = c("Studies", "Groups", "Animals", "Endpoints"),
        Count = c(
          dbGetQuery(conn, "SELECT COUNT(*) FROM studies")[[1]],
          dbGetQuery(conn, "SELECT COUNT(*) FROM groups")[[1]],
          dbGetQuery(conn, "SELECT COUNT(*) FROM animals")[[1]],
          dbGetQuery(conn, "SELECT COUNT(*) FROM endpoints")[[1]]
        )
      )
    }, error = function(e) {
      showNotification("Error fetching table counts!", type = "error")
      NULL
    })
  })
  
  chemical_data <- reactive({
    query <- "SELECT * FROM chemical"
    dbGetQuery(conn, query)
  })
  
  
  
  output$studiesTable <- renderDT({
    dbGetQuery(conn, "SELECT * FROM studies;")
  }, options = list(pageLength = 10, scrollX = TRUE), rownames = FALSE)
  
  output$groupsTable <- renderDT({
    dbGetQuery(conn, "SELECT * FROM groups;")
  }, options = list(pageLength = 10, scrollX = TRUE), rownames = FALSE)
  
  output$animalsTable <- renderDT({
    dbGetQuery(conn, "SELECT * FROM animals;")
  }, options = list(pageLength = 10, scrollX = TRUE), rownames = FALSE)
  
  output$endpointsTable <- renderDT({
    dbGetQuery(conn, "SELECT * FROM endpoints;")
  }, options = list(pageLength = 10, scrollX = TRUE), rownames = FALSE)
  
  output$sexDistribution <- renderPlot({
    animals() %>%
      group_by(sex) %>%
      summarise(count = n()) %>%
      ggplot(aes(x = sex, y = count, fill = sex)) +
      geom_bar(stat = "identity") +
      labs(title = "Animal Distribution by Sex", x = "Sex", y = "Count") +
      theme_minimal()
  })
  
  
  
  output$bubblePlot <- renderPlotly({
    counts <- tableCounts()
    if (is.null(counts)) return(NULL) 
    
    plot_ly(
      data = counts,
      x = ~Table,
      y = ~Count,
      type = 'scatter',
      mode = 'markers',
      size = ~Count,
      text = ~paste("Table:", Table, "<br>Records:", Count),
      marker = list(opacity = 0.7, sizemode = 'diameter', color = ~Count)
    ) %>%
      layout(
        title = "Database Table Metrics",
        xaxis = list(title = "Table"),
        yaxis = list(title = "Number of Records"),
        showlegend = FALSE
      )
  })
  
  output$chemicalsTable <- renderDT({
    datatable(
      chemical_data(), 
      options = list(
        pageLength = 5,       
        autoWidth = TRUE,     
        dom = 'Bfrtip',      
        class = "cell-border stripe" 
      ),
      rownames = FALSE,       
    )
  })
  
  output$analyticalTable <- renderDT({
    dbGetQuery(conn, "SELECT * FROM analyticalData;")
  }, options = list(pageLength = 10, scrollX = TRUE), rownames = FALSE)
  
  output$measurementTable <- renderDT({
    dbGetQuery(conn, "SELECT * FROM measurementData;")
  }, options = list(pageLength = 10, scrollX = TRUE), rownames = FALSE)
  
  output$metabolomicsTable <- renderDT({
    dbGetQuery(conn, "SELECT * FROM metabolomics_data")
  }, options = list(pageLength = 10, scrollX = TRUE), rownames = FALSE)
  
  output$crossreferenceTable <- renderDT({
    dbGetQuery(conn, "SELECT * FROM cross_reference")
  }, options = list(pageLength = 10, scrollX = TRUE), rownames = FALSE)
  
  output$download_db <- downloadHandler(
    filename = function() {
      "case_study.sqlite"
    },
    content = function(file) {
      db_path <- "resources/case_study.sqlite"
      if (file.exists(db_path)) {
        file.copy(db_path, file)
      } else {
        showNotification("Database file not found!", type = "error")
      }
    }
  )
  
  
  
  onStop(function() {
    if (dir.exists(saveDir)) {
      unlink(saveDir, recursive = TRUE) 
    }
  })
}

shinyApp(ui, server)