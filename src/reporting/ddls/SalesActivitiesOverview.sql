CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.SalesActivitiesOverview`
OPTIONS(
  description = 'Provides information about follow up activities done by Sales representatives on leads and opportunities'
)
AS
WITH
  SalesActivities AS (
    SELECT
      *
    FROM
      `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.SalesActivities`
    WHERE
      OpportunityId IS NOT NULL
      AND ActivityStatus = 'Completed'
  ),
SELECT
  'Opportunity' AS List,
  OpportunityPipeline.OpportunityCreatedDatestamp AS CreatedDate,
  OpportunityPipeline.AccountBillingCountry AS Country,
  OpportunityPipeline.AccountIndustry AS Industry,
  OpportunityPipeline.OpportunityOwnerName AS SalesRepresentative,
  MAX(SalesActivities.OpportunityOwnerId) AS OpportunitySalesRepresentative,
  MAX(SalesActivities.LeadOwnerId) AS LeadsSalesRepresentative,
  OpportunityPipeline.DateOfOpportunityCreatedDate AS DateOfListCreatedDate,
  OpportunityPipeline.WeekOfOpportunityCreatedDate AS WeekOfListCreatedDate,
  OpportunityPipeline.MonthOfOpportunityCreatedDate AS MonthOfListCreatedDate,
  OpportunityPipeline.QuarterOfOpportunityCreatedDate AS QuarterOfListCreatedDate,
  OpportunityPipeline.YearOfOpportunityCreatedDate AS YearOfListCreatedDate,
  COUNT(DISTINCT OpportunityPipeline.OpportunityId) AS NoOfOpportunities,
  COUNT(DISTINCT SalesActivities.OpportunityId) AS NoOfOpportunitiesWithActivities,
  COUNT(SalesActivities.ActivityId) AS NoOfActivities,
  COUNT(DISTINCT
    IF(SalesActivities.OpportunityIsClosed IS FALSE, SalesActivities.ActivityId, NULL)) AS NoOfActivitiesOnOpenOpportunities,
  COUNT(DISTINCT
    IF(SalesActivities.OpportunityIsClosed IS FALSE, SalesActivities.OpportunityId, NULL)) AS NoOfOpenOpportunities,
  COUNT(DISTINCT
    IF(SalesActivities.OpportunityIsClosed IS TRUE
      AND SalesActivities.OpportunityIsWon IS TRUE, SalesActivities.ActivityId, NULL)) AS NoOfActivitiesOnWonOpportunities,
  COUNT(DISTINCT
    IF(SalesActivities.OpportunityIsClosed IS TRUE
      AND SalesActivities.OpportunityIsWon IS TRUE, SalesActivities.OpportunityId, NULL)) AS NoOfWonOpportunities,
  COUNT(DISTINCT
    IF(SalesActivities.OpportunityIsClosed IS TRUE
      AND SalesActivities.OpportunityIsWon IS FALSE, SalesActivities.ActivityId, NULL)) AS NoOfActivitiesOnLostOpportunities,
  COUNT(DISTINCT
    IF(SalesActivities.OpportunityIsClosed IS TRUE
      AND SalesActivities.OpportunityIsWon IS FALSE, SalesActivities.OpportunityId, NULL)) AS NoOfLostOpportunities,
  COUNT(DISTINCT
    IF(OpportunityPipeline.OpportunityIsClosed IS FALSE AND ((OpportunityPipeline.OpportunityLastActivityDate IS NOT NULL
      AND DATETIME_DIFF(DATETIME(CURRENT_TIMESTAMP()), DATETIME((TIMESTAMP_TRUNC(OpportunityPipeline.OpportunityLastActivityDate, DAY))), DAY) > 7 )
      OR (OpportunityPipeline.OpportunityLastActivityDate IS NULL
        AND DATETIME_DIFF(DATETIME(CURRENT_TIMESTAMP()), DATETIME((TIMESTAMP_TRUNC(OpportunityPipeline.OpportunityCreatedDatestamp, DAY))), DAY) > 7)), OpportunityPipeline.OpportunityId, NULL)) AS NoOfNeglectedOpportunities,
  COUNT(DISTINCT
    IF(OpportunityPipeline.OpportunityIsClosed IS FALSE
      AND DATETIME_DIFF(DATETIME(CURRENT_TIMESTAMP()), DATETIME((TIMESTAMP_TRUNC(OpportunityPipeline.OpportunityClosedDate, DAY))), DAY) > 0, OpportunityPipeline.OpportunityId, NULL)) AS NoOfOverdueOpportunities,
  NULL AS NoOfLeadActivities,
  NULL AS NoOfLeads,
  NULL AS NoOfLeadsConverted,
  NULL AS NoOfActivitiesForConvertedLeads,
  NULL AS NoOfLeadsDisqualified,
  NULL AS NoOfActivitiesForDisqualifiedLeads
FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.OpportunityPipeline` AS OpportunityPipeline
LEFT JOIN
  SalesActivities AS SalesActivities
  ON
    OpportunityPipeline.OpportunityId = SalesActivities.OpportunityId
/*
WHERE
OpportunityRecordTypeName='Cortex Opportunity'
*/
GROUP BY
  OpportunityPipeline.OpportunityCreatedDatestamp,
  OpportunityPipeline.AccountBillingCountry,
  OpportunityPipeline.AccountIndustry,
  OpportunityPipeline.OpportunityOwnerName,
  OpportunityPipeline.DateOfOpportunityCreatedDate,
  OpportunityPipeline.WeekOfOpportunityCreatedDate,
  OpportunityPipeline.MonthOfOpportunityCreatedDate,
  OpportunityPipeline.QuarterOfOpportunityCreatedDate,
  OpportunityPipeline.YearOfOpportunityCreatedDate
UNION ALL
SELECT
  'Lead' AS List,
  LeadCreatedDatestamp AS CreatedDate,
  LeadCountry AS Country,
  LeadIndustry AS Industry,
  LeadOwnerName AS SalesRepresentative,
  NULL AS OpportunitySalesRepresentative,
  MAX(LeadOwnerId) AS LeadsSalesRepresentative,
  DateOfLeadCreatedDate AS DateOfListCreatedDate,
  WeekOfLeadCreatedDate AS WeekOfListCreatedDate,
  MonthOfLeadCreatedDate AS MonthOfListCreatedDate,
  QuarterOfLeadCreatedDate AS QuarterOfListCreatedDate,
  YearOfLeadCreatedDate AS YearOfListCreatedDate,
  NULL AS NoOfOpportunities,
  NULL AS NoOfOpportunitiesWithActivities,
  NULL AS NoOfActivities,
  NULL AS NoOfActivitiesOnOpenOpportunities,
  NULL AS NoOfOpenOpportunities,
  NULL AS NoOfActivitiesOnWonOpportunities,
  NULL AS NoOfWonOpportunities,
  NULL AS NoOfActivitiesOnLostOpportunities,
  NULL AS NoOfLostOpportunities,
  NULL AS NoOfNeglectedOpportunities,
  NULL AS NoOfOverdueOpportunities,
  COUNT(DISTINCT ActivityId) AS NoOfLeadActivities,
  COUNT(DISTINCT LeadId) AS NoOfLeads,
  COUNT(DISTINCT
    IF(LeadIsConverted IS TRUE, LeadId, NULL)) AS NoOfLeadsConverted,
  COUNT(DISTINCT
    IF(LeadIsConverted IS TRUE, ActivityId, NULL)) AS NoOfActivitiesForConvertedLeads,
  COUNT(DISTINCT
    IF(LeadStatus = 'Disqualified', LeadId, NULL)) AS NoOfLeadsDisqualified,
  COUNT(DISTINCT
    IF(LeadStatus = 'Disqualified', ActivityId, NULL)) AS NoOfActivitiesForDisqualifiedLeads
FROM
  `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.SalesActivities`
WHERE
  LeadId IS NOT NULL
  AND OpportunityId IS NULL
  AND ActivityStatus = 'Completed'
GROUP BY
  LeadCreatedDatestamp,
  LeadCountry,
  LeadIndustry,
  LeadOwnerName,
  DateOfLeadCreatedDate,
  WeekOfLeadCreatedDate,
  MonthOfLeadCreatedDate,
  QuarterOfLeadCreatedDate,
  YearOfLeadCreatedDate;

  /* *Obtaining Insights FROM the VIEW* * Sales Activities & Engagement * Avg. Activities Per Sales Representative
ON
  Leads SUM(NoOfLeadActivities)/COUNT(DISTINCT LeadsSalesRepresentative) Follow Up Contact Rate
ON
  Leads SUM(NoOfLeadActivities)/SUM(NoOfLeads) Follow Up Contact Rate
ON
  Converted Leads SUM(NoOfActivitiesForConvertedLeads)/SUM(NoOfLeadsConverted) Follow Up Contact Rate
ON
  Disqualified Leads SUM(NoOfActivitiesForDisqualifiedLeads)/SUM(NoOfLeadsDisqualified) Avg. Activities Per Sales Representative
ON
  Opportunities SUM(NoOfActivities)/COUNT(DISTINCT OpportunitySalesRepresentative) Avg. Activities Per Open Opportunity SUM(NoOfOpportuintyActivities)/SUM(NoOfOpportunities)
  Avg. Activities Per Closed-Won Opportunity SUM(NoOfActivitiesOnWonOpportunities)/SUM(NoOfWonOpportunities) Avg. Activities Per Closed-Lost Opportunity SUM(NoOfActivitiesOnLostOpportunities)/SUM(NoOfLostOpportunities) */
