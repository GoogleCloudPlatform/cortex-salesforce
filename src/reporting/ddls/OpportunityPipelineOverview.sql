#-- Copyright 2022 Google LLC
#--
#-- Licensed under the Apache License, Version 2.0 (the "License");
#-- you may not use this file except in compliance with the License.
#-- You may obtain a copy of the License at
#--
#--     https://www.apache.org/licenses/LICENSE-2.0
#--
#-- Unless required by applicable law or agreed to in writing, software
#-- distributed under the License is distributed on an "AS IS" BASIS,
#-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#-- See the License for the specific language governing permissions and
#-- limitations under the License.

/*
* This view is intended to showcase how KPIs from OpportunityPipeline view are meant to be calculated
* without the accompanying Looker visualizations.
*
* Please note that this is view is INFORMATIONAL ONLY and may be subject to change without
* notice in upcoming Cortex Data Foundation releases.
*/

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.OpportunityPipelineOverview`
  OPTIONS(
    description = 'Provides information about Opportunity trends and pipeline to do the forcasting'
  )
AS (
  SELECT
    AccountName,
    OpportunityOwnerName,
    AccountBillingCountry AS Country,
    AccountIndustry AS Industry,
    OpportunityProbability,
    COUNT(OpportunityId) AS NumOfOpportunities,
    COUNTIF(NOT IsOpportunityClosed) AS NumOfOpenOpportunities,
    COUNTIF(IsOpportunityClosed AND IsOpportunityWon) AS NumOfWonOpportunities,
    COUNTIF(IsOpportunityClosed AND NOT IsOpportunityWon) AS NumOfLostOpportunities,
    SUM(TotalSaleAmount) AS TotalOpportunitiesValue,
    SUM(IF(NOT IsOpportunityClosed, TotalSaleAmount, 0)) AS TotalOpenOpportunitiesValue,
    SUM(IF(IsOpportunityClosed AND IsOpportunityWon, TotalSaleAmount, 0)) AS TotalWonOpportunitiesValue,
    SUM(IF(IsOpportunityClosed AND NOT IsOpportunityWon, TotalSaleAmount, 0)) AS TotalLostOpportunitiesValue
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.OpportunityPipeline`
  GROUP BY
    AccountName,
    OpportunityOwnerName,
    Country,
    Industry,
    OpportunityProbability
);
