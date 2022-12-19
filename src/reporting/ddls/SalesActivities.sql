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

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.SalesActivities`
  OPTIONS(
    description = 'Provides information about follow up activities done by Sales representatives on leads and opportunities'
  )
AS (
  WITH
    Activities AS (
      -- Select all Events
      SELECT
        'Event' AS ActivityType,
        Events.EventId AS ActivityId,
        Events.OwnerId,
        -- Mirroring default value of Tasks.Priority since Event does not have a concept of Priority
        'Normal' AS ActivityPriority,
        Events.EndDate AS ActivityEndDate,
        Events.CreatedDatestamp AS ActivityCreatedDatestamp,
        Events.AccountId,
        Events.Subject AS ActivitySubject,
        -- Mirroring possible values of Tasks.Status
        CASE
          -- Depending on whether IsAllDayEvent, only one of Date and Datetimestamp will be populated.
          WHEN COALESCE(Events.EndDate, DATE(Events.EndDatetimestamp)) < CURRENT_DATE()
            THEN 'Completed'
          WHEN COALESCE(Events.ActivityDate, DATE(Events.ActivityDatetimestamp)) > CURRENT_DATE()
            THEN 'Not Started'
          ELSE 'In Progress'
          END AS ActivityStatus,
        Events.WhoId,
        Events.WhatId
      FROM
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Events` AS Events
      UNION ALL
      -- Select all Tasks
      SELECT
        'Task' AS ActivityType,
        Tasks.TaskId AS ActivityId,
        Tasks.OwnerId,
        Tasks.Priority AS ActivityPriority,
        -- Tasks.ActivityDate is displayed as Due Date
        Tasks.ActivityDate AS ActivityEndDate,
        Tasks.CreatedDatestamp AS ActivityCreatedDatestamp,
        Tasks.AccountId,
        Tasks.Subject AS ActivitySubject,
        Tasks.Status AS ActivityStatus,
        Tasks.WhoId,
        Tasks.WhatId
      FROM
        `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Tasks` AS Tasks
    )
  SELECT
    Activities.*,
    Leads.Name AS LeadName,
    Leads.LeadId AS LeadId,
    Leads.CreatedDatestamp AS LeadCreatedDatestamp,
    Leads.OwnerId AS LeadOwnerId,
    Leads.Country AS LeadCountry,
    Leads.Industry AS LeadIndustry,
    Leads.IsConverted AS IsLeadConverted,
    Opportunities.OpportunityId AS OpportunityId,
    Opportunities.Name AS OpportunityName,
    Opportunities.StageName AS OpportunityStageName,
    Opportunities.CloseDate AS OpportunityCloseDate,
    Opportunities.CreatedDatestamp AS OpportunityCreatedDatestamp,
    Opportunities.Probability AS OpportunityProbability,
    Opportunities.IsWon AS IsOpportunityWon,
    Opportunities.OwnerId AS OpportunityOwnerId,
    Opportunities.AccountId AS OpportunityAccountId,
    Opportunities.LastActivityDate AS OpportunityLastActivityDate,
    Opportunities.IsClosed AS IsOpportunityClosed,
    Opportunities.Amount AS TotalSaleAmount,
    AccountsMD.BillingCountry AS AccountBillingCountry,
    AccountsMD.Name AS AccountName,
    AccountsMD.Industry AS AccountIndustry,
    ActivityOwnerUser.Name AS ActivityOwnerName,
    OpportunityOwnerUser.Name AS OpportunityOwnerName,
    LeadOwnerUser.Name AS LeadOwnerName,
    Leads.Status AS LeadStatus,
    -- Expected Opportunity value by each Sales Representative in Sales Activities charts.
    Opportunities.Amount * (Opportunities.Probability / 100) AS OpportunityExpectedValue
  FROM Activities
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities` AS Opportunities
    ON Activities.WhatId = Opportunities.OpportunityId
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Leads` AS Leads
    ON Activities.WhoId = Leads.LeadId
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD` AS ActivityOwnerUser
    ON Activities.OwnerId = ActivityOwnerUser.UserId
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD` AS OpportunityOwnerUser
    ON Opportunities.OwnerId = OpportunityOwnerUser.UserId
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD` AS LeadOwnerUser
    ON Leads.OwnerId = LeadOwnerUser.UserId
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.AccountsMD` AS AccountsMD
    ON Activities.AccountId = AccountsMD.AccountId
);
