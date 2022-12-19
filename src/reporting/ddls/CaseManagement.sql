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

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.CaseManagement`
  OPTIONS(description = 'Provides information about Case creation and resolution trends')
AS (
  SELECT
    Cases.CaseId AS CaseId,
    Cases.Origin AS CaseOrigin,
    Cases.Priority AS CasePriority,
    Cases.Status AS CaseStatus,
    Cases.OwnerId AS CaseOwnerId,
    Cases.ClosedDatestamp AS CaseClosedDatestamp,
    Cases.CaseNumber AS CaseNumber,
    Cases.CreatedDatestamp AS CaseCreatedDatestamp,
    Cases.IsClosed AS IsCaseClosed,
    Cases.Type AS CaseType,
    Cases.Subject AS CaseSubject,
    AccountsMD.AccountId AS AccountId,
    AccountsMD.Name AS AccountName,
    AccountsMD.Type AS AccountType,
    AccountsMD.Phone AS AccountPhone,
    AccountsMD.OwnerId AS AccountOwnerId,
    AccountOwner.Name AS AccountOwnerName,
    AccountsMD.Industry AS AccountIndustry,
    AccountsMD.BillingCountry AS AccountBillingCountry,
    AccountsMD.ShippingCountry AS AccountShippingCountry,
    AccountsMD.CreatedDatestamp AS AccountCreatedDatestamp,
    CaseOwner.Name AS CaseOwnerName,
    -- Agents who have been assigned to Cases
    (Cases.OwnerId NOT LIKE '00G%') AS IsAgentAssigned
  FROM
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Cases` AS Cases
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.AccountsMD` AS AccountsMD
    ON
      Cases.AccountId = AccountsMD.AccountId
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD` AS CaseOwner
    ON
      Cases.OwnerId = CaseOwner.UserId
  LEFT JOIN
    `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD` AS AccountOwner
    ON
      AccountsMD.OwnerId = AccountOwner.UserId
);
