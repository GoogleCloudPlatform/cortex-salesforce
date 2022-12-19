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

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.OpportunityPipeline`
  OPTIONS(description = 'Provides information about Opportunity trends and pipeline')
AS (
  SELECT
    Opportunities.OpportunityId,
    -- Total sale amount that a sales representative has achieved
    Opportunities.Amount AS TotalSaleAmount,
    Opportunities.CloseDate AS OpportunityCloseDate,
    Opportunities.Name AS OpportunityName,
    Opportunities.Probability AS OpportunityProbability,
    Opportunities.StageName AS OpportunityStageName,
    Opportunities.OwnerId AS OpportunityOwnerId,
    Opportunities.IsClosed AS IsOpportunityClosed,
    Opportunities.IsWon AS IsOpportunityWon,
    Opportunities.LastActivityDate AS OpportunityLastActivityDate,
    Opportunities.CreatedDatestamp AS OpportunityCreatedDatestamp,
    Opportunities.RecordTypeId AS OpportunityRecordTypeId,
    AccountsMD.AccountId,
    AccountsMD.Name AS AccountName,
    AccountsMD.OwnerId AS AccountOwnerId,
    AccountsMD.Industry AS AccountIndustry,
    AccountsMD.BillingCountry AS AccountBillingCountry,
    AccountsMD.ShippingCountry AS AccountShippingCountry,
    AccountsMD.CreatedDatestamp AS AccountCreatedDatestamp,
    UsersMD.Name AS OpportunityOwnerName,
    RecordTypesMD.RecordTypeName AS OpportunityRecordTypeName,
    Opportunities.Amount * (Opportunities.Probability / 100) AS OpportunityExpectedValue
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities` AS Opportunities
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.AccountsMD` AS AccountsMD
    ON AccountsMD.AccountId = Opportunities.AccountId
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD` AS UsersMD
    ON Opportunities.OwnerId = UsersMD.UserId
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.RecordTypesMD` AS RecordTypesMD
    ON Opportunities.RecordTypeId = RecordTypesMD.RecordTypeId
);
