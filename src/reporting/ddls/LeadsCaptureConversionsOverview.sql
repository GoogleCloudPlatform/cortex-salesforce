CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.LeadsCaptureConversionsOverview`
  OPTIONS(description = 'Provides information about Leads creation and conversion trends')
AS (
  SELECT
    LeadCountry,
    LeadIndustry,
    LeadOwnerName,
    LeadSource,
    LeadStatus,
    LeadCreatedDatestamp,
    --## CORTEX-CUSTOMER If you prefer to use amount in AmountInTarget Currency, uncomment below and
    --## uncomment currency_conversion in LeadsCaptureConversion
    -- AmountInTargetCurrency,
    -- TargetCurrency,
    DateOfLeadCreatedDate,
    WeekOfLeadCreatedDate,
    MonthOfLeadCreatedDate,
    QuarterOfLeadCreatedDate,
    YearOfLeadCreatedDate,
  COUNT(LeadId) AS NoOfLeads,
  SUM(DATETIME_DIFF(DATETIME(LeadFirstResponeTimestamp), DATETIME(LeadCreatedDatestamp), HOUR)) AS TotalLeadResponseTime,
  COUNT(DISTINCT(IF(IsLeadConverted IS TRUE, LeadId, NULL))) AS ConvertedLead
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.LeadsCaptureConversion`
  GROUP BY
    LeadCountry,
    LeadIndustry,
    LeadOwnerName,
    LeadSource,
    LeadStatus,
    LeadCreatedDatestamp,
    -- AmountInTargetCurrency,
    -- TargetCurrency,
    DateOfLeadCreatedDate,
    WeekOfLeadCreatedDate,
    MonthOfLeadCreatedDate,
    QuarterOfLeadCreatedDate,
    YearOfLeadCreatedDate
);
