-- SELECT
-- SUM(CountOfOpportunities),
-- SUM(CountOfOpportunitiesOpen),
-- SUM(CountOfOpportunitiesWon),
-- SUM(CountOfOpportunitiesLost),
-- SUM(ValueTotalOpportunities),
-- SUM(ValueOpenOpportunities),
-- SUM(ValueWonOpportunities),
-- SUM(ValueLostOpportunities),
-- FROM
-- (
CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.OpportunityPipeline`
OPTIONS(
  description = 'Provides information about Opportunity trends and pipeline to do the forcasting'
)
AS
SELECT
  AccountName,
  OpportunityOwnerName,
  AccountBillingCountry AS Country,
  AccountIndustry AS Industry,
  OpportunityProbability,
  --## CORTEX-CUSTOMER If you prefer to use amount in AmountInTarget Currency, uncomment below and
  --## uncomment currency_conversion in Opportunity
  -- AmountInTargetCurrency,
  -- TargetCurrency,
  DateOfOpportunityCreatedDate,
  WeekOfOpportunityCreatedDate,
  MonthOfOpportunityCreatedDate,
  QuarterOfOpportunityCreatedDate,
  YearOfOpportunityCreatedDate,
  DateOfOpportunityClosedDate,
  WeekOfOpportunityClosedDate,
  MonthOfOpportunityClosedDate,
  QuarterOfOpportunityClosedDate,
  YearOfOpportunityClosedDate,

  COUNT(OpportunityId) AS CountOfOpportunities,
  COUNT(IF(((OpportunityIsClosed = FALSE)), OpportunityId, NULL)) AS CountOfOpportunitiesOpen,
  COUNT(IF(((OpportunityIsClosed = TRUE) AND (OpportunityIsWon = TRUE)), OpportunityId, NULL)) AS CountOfOpportunitiesWon,
  COUNT(IF(((OpportunityIsClosed = TRUE) AND (OpportunityIsWon = FALSE)), OpportunityId, NULL)) AS CountOfOpportunitiesLost,

  --COUNT(IF((OpportunityRecordTypeName = 'Cortex Opportunity'), OpportunityId, NULL)) AS CountOfOpportunities,
  -- COUNT(IF(((OpportunityIsClosed = FALSE) AND (OpportunityRecordTypeName = 'Cortex Opportunity')), OpportunityId, NULL)) AS CountOfOpportunitiesOpen,
  -- COUNT(IF(((OpportunityIsClosed = TRUE) AND (OpportunityIsWon = TRUE) AND (OpportunityRecordTypeName = 'Cortex Opportunity')), OpportunityId, NULL)) AS CountOfOpportunitiesWon,
  -- COUNT(IF(((OpportunityIsClosed = TRUE) AND (OpportunityIsWon = FALSE) AND (OpportunityRecordTypeName = 'Cortex Opportunity')), OpportunityId, NULL)) AS CountOfOpportunitiesLost,

  SUM(TotalSaleAmount) AS ValueTotalOpportunities,
  SUM(IF((OpportunityIsClosed = FALSE), TotalSaleAmount, NULL)) AS ValueOpenOpportunities,
  SUM(IF(((OpportunityIsClosed = TRUE) AND (OpportunityIsWon = TRUE)), TotalSaleAmount, NULL)) AS ValueWonOpportunities,
  SUM(IF(((OpportunityIsClosed = TRUE) AND (OpportunityIsWon = FALSE)), TotalSaleAmount, NULL)) AS ValueLostOpportunities



-- SUM(IF((OpportunityRecordTypeName = 'Cortex Opportunity'), TotalSaleAmount, NULL)) AS ValueTotalOpportunities,
  -- SUM(IF(((OpportunityIsClosed = FALSE) AND (OpportunityRecordTypeName = 'Cortex Opportunity')), TotalSaleAmount, NULL)) AS ValueOpenOpportunities,
  -- SUM(IF(((OpportunityIsClosed = TRUE) AND (OpportunityIsWon = TRUE) AND (OpportunityRecordTypeName = 'Cortex Opportunity')), TotalSaleAmount, NULL)) AS ValueWonOpportunities,
  -- SUM(IF(((OpportunityIsClosed = TRUE) AND (OpportunityIsWon = FALSE) AND (OpportunityRecordTypeName = 'Cortex Opportunity')), TotalSaleAmount, NULL)) AS ValueLostOpportunities
  --SUM(IF((OpportunityRecordTypeName = 'Sales Target'), TargetAmount, NULL)) AS TargetAmount

FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.OpportunityPipeline`
GROUP BY
  AccountName,
  OpportunityOwnerName,
  Country,
  Industry,
  OpportunityProbability,
  -- AmountInTargetCurrency,
  -- TargetCurrency,
  DateOfOpportunityCreatedDate,
  WeekOfOpportunityCreatedDate,
  MonthOfOpportunityCreatedDate,
  QuarterOfOpportunityCreatedDate,
  YearOfOpportunityCreatedDate,
  DateOfOpportunityClosedDate,
  WeekOfOpportunityClosedDate,
  MonthOfOpportunityClosedDate,
  QuarterOfOpportunityClosedDate,
  YearOfOpportunityClosedDate;
-- )
