CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.CaseManagementOverview`
  OPTIONS(description = 'Provides information about Case creation and resolution trends')
AS (
  SELECT
    CaseOrigin,
    CasePriority,
    CaseStatus,
    CaseType,
    CaseOwnerId,
    CaseOwnerName,
    AccountId,
    AccountName,
    AccountIndustry,
    AccountBillingCountry,
    DateOfCaseCreatedDate,
    MonthOfCaseCreatedDate,
    QuarterOfCaseCreatedDate,
    YearOfCaseCreatedDate,
    DateOfCaseClosedDate,
    MonthOfCaseClosedDate,
    QuarterOfCaseClosedDate,
    YearOfCaseClosedDate,
    IF(IsAgentAssigned, CaseOwnerId, NULL) AS CaseOwnerAgentId,
    COUNT(CaseId) AS CasesCreated,
    COUNTIF(NOT IsCaseClosed) AS OpenCases,
    COUNTIF(NOT IsCaseClosed AND CasePriority = 'High') AS OpenHighPriorityCases,
    COUNTIF(IsCaseClosed AND CasePriority = 'High') AS ClosedHighPriorityCases,
    COUNTIF(NOT IsCaseClosed AND NOT IsAgentAssigned) AS UnassignedOpenCases,
    COUNTIF(IsAgentAssigned) AS AssignedCases,
    SUM(
      IF(
        NOT IsCaseClosed, DATE_DIFF(DATE(CURRENT_TIMESTAMP()), DATE(CaseCreatedDatestamp), DAY), 0
      )) AS TotalCaseAge,
    SUM(DATE_DIFF(DATE(CaseClosedDatestamp), DATE(CaseCreatedDatestamp), DAY)) AS TotalCaseResolutionTime,
    SUM(
      IF(
        IsCaseClosed AND CasePriority = 'High',
        DATE_DIFF(DATE(CaseClosedDatestamp), DATE(CaseCreatedDatestamp), DAY),
        0)) AS TotalCaseResolutionTimeHighPriorityCases,
    COUNTIF(IsCaseClosed) AS ClosedCases,
    COUNTIF(NOT IsCaseClosed AND CaseStatus = 'Escalated') AS EscalatedOpenCases,
    COUNTIF(NOT IsCaseClosed AND CasePriority = 'Medium') AS OpenMediumPriorityCases,
    COUNTIF(NOT IsCaseClosed AND CasePriority = 'Low') AS OpenLowPriorityCases,
    COUNTIF(IsCaseClosed AND CasePriority = 'Medium') AS ClosedMediumPriorityCases,
    COUNTIF(IsCaseClosed AND CasePriority = 'Low') AS ClosedLowPriorityCases
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.CaseManagement`
  GROUP BY
    CaseOrigin,
    CasePriority,
    CaseStatus,
    CaseType,
    CaseOwnerId,
    CaseOwnerName,
    CaseOwnerAgentId,
    AccountId,
    AccountName,
    AccountIndustry,
    AccountBillingCountry,
    DateOfCaseCreatedDate,
    MonthOfCaseCreatedDate,
    QuarterOfCaseCreatedDate,
    YearOfCaseCreatedDate,
    DateOfCaseClosedDate,
    MonthOfCaseClosedDate,
    QuarterOfCaseClosedDate,
    YearOfCaseClosedDate
);