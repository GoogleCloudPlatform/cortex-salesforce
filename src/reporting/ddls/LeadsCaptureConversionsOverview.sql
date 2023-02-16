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
* This view is intended to showcase how KPIs from LeadCaptureConversions view are meant to be
* calculated without the accompanying Looker visualizations.
*
* Please note that this is view is INFORMATIONAL ONLY and may be subject to change without
* notice in upcoming Cortex Data Foundation releases.
*/

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.LeadsCaptureConversionsOverview`
  OPTIONS(
    description = 'Provides information about Leads creation and conversion trends'
  )
AS (
  SELECT
    LeadCountry,
    LeadIndustry,
    LeadOwnerName,
    LeadSource,
    LeadStatus,
    LeadCreatedDatestamp,
    COUNT(LeadId) AS NumOfLeads,
    COUNT(DISTINCT(IF(IsLeadConverted IS TRUE, LeadId, NULL))) AS NumOfConvertedLeads,
    SUM(
      DATETIME_DIFF(DATETIME(LeadFirstResponeDatestamp), DATETIME(LeadCreatedDatestamp), HOUR)
    ) AS TotalLeadResponseTimeHours
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.LeadsCaptureConversions`
  GROUP BY
    LeadCountry,
    LeadIndustry,
    LeadOwnerName,
    LeadSource,
    LeadStatus,
    LeadCreatedDatestamp
);
