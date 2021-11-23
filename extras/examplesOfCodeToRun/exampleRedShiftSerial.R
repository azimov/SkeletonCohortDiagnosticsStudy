############### Note this is a custom script, a version of CodeToRun.R that may not work for everyone ##############
############### Please use CodeToRun.R as it a more generic version  ###############################################

############### This version of code will loop over each database ids in serial                      ###############

############### processes to run Cohort Diagnostics ################################################################
library(magrittr)
Sys.getenv("bulkLoad")
################################################################################
# VARIABLES - please change
################################################################################
# The folder where the study intermediate and result files will be written:
outputFolder <- "D:/studyResults/SkeletonCohortDiagnosticsStudy"
# create output directory if it does not exist
if (!dir.exists(outputFolder)) {
  dir.create(outputFolder,
             showWarnings = FALSE,
             recursive = TRUE)
}
# Optional: specify a location on your disk drive that has sufficient space.
options(andromedaTempFolder = file.path(outputFolder, "andromedaTemp"))

# lets get meta information for each of these databaseId. This includes connection information.
source("extras/examplesOfCodeToRun/dataSourceInformation.R")

############## databaseIds to run cohort diagnostics on that source  #################
databaseIds <-
  c(
    'ims_australia_lpd',
    'ims_france',
    'jmdc',
    'cprd',
    'iqvia_pharmetrics_plus',
    'truven_ccae',
    'truven_mdcd',
    'truven_mdcr',
    'optum_extended_dod',
    'optum_ehr',
    'premier')

## service name for keyring for db with cdm
keyringUserService <- 'OHDSI_USER'
keyringPasswordService <- 'OHDSI_PASSWORD'

# cdmSources <- cdmSources2
# rm("cdmSources2")

###### create a list object that contain connection and meta information for each data source
x <- list()
for (i in (1:length(databaseIds))) {
  cdmSource <- cdmSources %>%
    dplyr::filter(.data$sequence == 1) %>%
    dplyr::filter(database == databaseIds[[i]])
  
  outputFolderLong <- file.path(outputFolder, paste0(databaseIds[[i]], "_v", as.character(cdmSource$version)))
  
  x[[i]] <- list(
    cdmSource = cdmSource,
    generateCohortTableName = TRUE,
    verifyDependencies = FALSE,
    databaseId = databaseIds[[i]],
    outputFolder = outputFolderLong,
    userService = keyringUserService,
    passwordService = keyringPasswordService,
    preMergeDiagnosticsFiles = TRUE
  )
}


############ executeOnMultipleDataSources #################
# x <- x[1:2]
for (i in (1:length(x))) {
  executeOnMultipleDataSources(x[[i]])
}
