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

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.LeadsCaptureConversions`
  OPTIONS(description = 'Provides information about Leads creation and conversion trends')
AS (
  WITH LeadsFirstResponseDates AS (
    SELECT LeadId, MIN(CreatedDatestamp) AS LeadFirstResponeDatestamp
    FROM (
        SELECT WhoId AS LeadId, MIN(CreatedDatestamp) AS CreatedDatestamp
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Events`
        GROUP BY 1
        UNION ALL
        SELECT WhoId AS LeadId, MIN(CreatedDatestamp) AS CreatedDatestamp
        FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Tasks`
        GROUP BY 1
      )
    GROUP BY 1
  )
  SELECT
    Leads.LeadId AS LeadId,
    Leads.Name AS LeadName,
    Leads.FirstName AS LeadFirstName,
    Leads.LastName AS LeadLastName,
    Leads.Leadsource AS LeadSource,
    Leads.OwnerId AS LeadOwnerId,
    Leads.Industry AS LeadIndustry,
    Leads.Country AS LeadCountry,
    Leads.ConvertedDate AS LeadConvertedDate,
    Leads.CreatedDatestamp AS LeadCreatedDatestamp,
    Leads.IsConverted AS IsLeadConverted,
    UsersMD.Name AS LeadOwnerName,
    Opportunities.OpportunityId AS OpportunityId,
    Opportunities.Amount AS TotalSaleAmount,
    Opportunities.OwnerId AS OpportunityOwnerId,
    Opportunities.CloseDate AS OpportunityCloseDate,
    Opportunities.CreatedDatestamp AS OpportunityCreatedDatestamp,
    Opportunities.Name AS OpportunityName,
    Leads.Status AS LeadStatus,
    LeadsFirstResponseDates.LeadFirstResponeDatestamp AS LeadFirstResponeDatestamp
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Leads` AS Leads
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD` AS UsersMD
    ON Leads.OwnerId = UsersMD.UserId
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities` AS Opportunities
    ON Leads.ConvertedOpportunityId = Opportunities.OpportunityId
  LEFT JOIN
    LeadsFirstResponseDates
    ON Leads.LeadId = LeadsFirstResponseDates.LeadId
);
