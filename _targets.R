library(targets)
library(SkeletonCohortDiagnosticsStudy)

# Imagine this is some secure way of getting db credentials
getSecureConnectionDetails <- function() {
  Eunomia::getEunomiaConnectionDetails()
}

loadStudyParameters <- function(secureConnectionDetails) {
  # Imagine serialized settings stored somewhere
  # For API purposes this should be defined as a function within the package
  dir.create("StudyOutput", showWarnings = FALSE)
  # The folder where the study intermediate and result files will be written:
  outputFolder <- normalizePath("StudyOutput")

  list(
    connectionDetails = secureConnectionDetails,
    cdmDatabaseSchema = "main",
    cohortDatabaseSchema = "main",
    cohortTable = "cohort",
    verifyDependencies = TRUE,
    outputFolder = outputFolder,
    databaseId = "Eunomia",
    databaseName = "Eunomia",
    databaseDescription = "Description of the study",
    vocabularyDatabaseSchema = "main",
    incremental = TRUE,
    incrementalFolder = file.path(outputFolder, "incremental"),
    maxCores = parallel::detectCores(),
    ## Cohort Diagnostics Execution Parameters ##
    runInclusionStatistics = TRUE,
    runIncludedSourceConcepts = TRUE,
    runOrphanConcepts = TRUE,
    runTimeDistributions = TRUE,
    runVisitContext = TRUE,
    runBreakdownIndexEvents = TRUE,
    runIncidenceRate = TRUE,
    runTimeSeries = FALSE,
    runCohortOverlap = TRUE,
    runCohortCharacterization = TRUE,
    covariateSettings = FeatureExtraction::createDefaultCovariateSettings(),
    runTemporalCohortCharacterization = TRUE,
    temporalCovariateSettings = FeatureExtraction::createTemporalCovariateSettings(
      useConditionOccurrence =
        TRUE,
      useDrugEraStart = TRUE,
      useProcedureOccurrence = TRUE,
      useMeasurement = TRUE,
      temporalStartDays = c(-365, -30, 0, 1, 31),
      temporalEndDays = c(-31, -1, 0, 30, 365)
    ),
    minCellCount = 5
  )
}

getCohortDefinitions <- function() {
  # This doesn't have to be this local definition: we could use a package like pins or web service
  dplyr::tibble(CohortGenerator::getCohortDefinitionSet(packageName = "SkeletonCohortDiagnosticsStudy",
                                                        cohortFileNameValue = "cohortId"))
}

getCohortTableNames <- function(studyParams) {
  CohortGenerator::getCohortTableNames(cohortTable = studyParams$cohortTable)
}

createCohorts <- function(studyParams, cohortDefinitionSet, cohortTableNames) {
  CohortGenerator::generateCohortSet(
    connectionDetails = studyParams$connectionDetails,
    cdmDatabaseSchema = studyParams$cdmDatabaseSchema,
    cohortDatabaseSchema = studyParams$cohortDatabaseSchema,
    cohortTableNames = cohortTableNames,
    cohortDefinitionSet = cohortDefinitionSet,
    incrementalFolder = studyParams$incrementalFolder,
    incremental = studyParams$incremental
  )
  return(T)
}

runDiagnostics <- function(studyParams, cohortDefinitionSet, cohortTableNames, cohortsCreated) {
  CohortDiagnostics::executeDiagnostics(
    cohortDefinitionSet = cohortDefinitionSet,
    exportFolder = studyParams$outputFolder,
    databaseId = studyParams$databaseId,
    connectionDetails = studyParams$connectionDetails,
    cdmDatabaseSchema = studyParams$cdmDatabaseSchema,
    tempEmulationSchema = studyParams$tempEmulationSchema,
    cohortDatabaseSchema = studyParams$cohortDatabaseSchema,
    cohortTable = studyParams$cohortTable,
    cohortTableNames = cohortTableNames,
    vocabularyDatabaseSchema = studyParams$vocabularyDatabaseSchema,
    inclusionStatisticsFolder = studyParams$outputFolder,
    cohortIds = studyParams$cohortIds,
    databaseName = studyParams$databaseName,
    databaseDescription = studyParams$databaseDescription,
    cdmVersion = 5,
    runInclusionStatistics = studyParams$runInclusionStatistics,
    runIncludedSourceConcepts = studyParams$runIncludedSourceConcepts,
    runOrphanConcepts = studyParams$runOrphanConcepts,
    runTimeDistributions = studyParams$runTimeDistributions,
    runVisitContext = studyParams$runVisitContext,
    runBreakdownIndexEvents = studyParams$runBreakdownIndexEvents,
    runIncidenceRate = studyParams$runIncidenceRate,
    runTimeSeries = studyParams$runTimeSeries,
    runCohortOverlap = studyParams$runCohortOverlap,
    runCohortCharacterization = studyParams$runCohortCharacterization,
    covariateSettings = studyParams$covariateSettings,
    runTemporalCohortCharacterization = studyParams$runTemporalCohortCharacterization,
    temporalCovariateSettings = studyParams$temporalCovariateSettings,
    minCellCount = studyParams$minCellCount,
    incremental = studyParams$incremental,
    incrementalFolder = studyParams$incrementalFolder
  )
  return(T)
}

cleanUp <- function(studyParams, diagnosticsComplete) {
  CohortGenerator::dropCohortStatsTables(
    connectionDetails = studyParams$connectionDetails,
    cohortDatabaseSchema = studyParams$cohortDatabaseSchema,
    cohortTableNames = studyParams$cohortTableNames
  )
  return(T)
}
# Set target-specific options such as packages.
tar_option_set(packages = c("CohortGenerator", "SkeletonCohortDiagnosticsStudy"))
# End this file with a list of target objects.
list(
  tar_target(secureConnectionDetails, getSecureConnectionDetails),
  tar_target(studyParams, loadStudyParameters(secureConnectionDetails)),
  tar_target(cohortTableNames,  getCohortTableNames(studyParams)),
  tar_target(cohortDefinitionSet, getCohortDefinitions()),
  tar_target(instaniteCohortsStep, createCohorts(studyParams, cohortDefinitionSet, cohortTableNames)),
  tar_target(diagnosticsStep, runDiagnostics(studyParams, cohortDefinitionSet, cohortTableNames, instaniteCohortsStep)),
  tar_target(cleanTables, cleanUp(studyParams, diagnosticsStep))
)
