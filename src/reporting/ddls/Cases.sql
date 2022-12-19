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

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Cases`
  OPTIONS(description = 'View for Cases Table')
AS (
  SELECT
    Cases.CaseId,
    Cases.AccountId,
    Cases.CaseNumber,
    Cases.ClosedDatestamp,
    Cases.Comments,
    Cases.ContactEmail,
    Cases.ContactFax,
    Cases.ContactId,
    Cases.ContactMobile,
    Cases.ContactPhone,
    Cases.CreatedById,
    Cases.CreatedDatestamp,
    Cases.Description,
    Cases.EntitlementId,
    Cases.Isclosed,
    Cases.IsEscalated,
    Cases.Language,
    Cases.MasterRecordId,
    Cases.Origin,
    Cases.OwnerId,
    Cases.ParentId,
    Cases.Priority,
    Cases.Reason,
    Cases.RecordTypeId,
    Cases.Status,
    Cases.Subject,
    Cases.SuppliedCompany,
    Cases.SuppliedEmail,
    Cases.SuppliedName,
    Cases.SuppliedPhone,
    Cases.Type,
    Cases.LastModifiedById,
    Cases.LastModifiedDatestamp,
    Cases.LastReferencedDatestamp,
    Cases.LastViewedDatestamp,
    Cases.SlaExitDatestamp,
    Cases.SlaStartDatestamp
  FROM
    `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.cases` AS Cases
);
