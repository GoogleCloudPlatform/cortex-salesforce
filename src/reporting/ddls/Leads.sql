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

CREATE OR REPLACE VIEW `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Leads`
  OPTIONS(description = 'View for Leads Table')
AS (
  SELECT
    Leads.LeadId,
    Leads.AnnualRevenue,
    Leads.City,
    Leads.Company,
    Leads.ConvertedAccountId,
    Leads.ConvertedContactId,
    Leads.ConvertedDate,
    Leads.ConvertedOpportunityId,
    Leads.Country,
    Leads.CreatedById,
    Leads.CreatedDatestamp,
    Leads.Description,
    Leads.Email,
    Leads.FirstName,
    Leads.GeocodeAccuracy,
    Leads.IndividualId,
    Leads.Industry,
    Leads.IsConverted,
    Leads.IsUnreadByOwner,
    Leads.LastActivityDate,
    Leads.LastModifiedById,
    Leads.LastModifiedDatestamp,
    Leads.LastName,
    Leads.Latitude,
    Leads.LeadSource,
    Leads.Longitude,
    Leads.MasterRecordId,
    Leads.Name,
    Leads.NumberOfEmployees,
    Leads.OwnerID,
    Leads.Phone,
    Leads.PostalCode,
    Leads.Rating,
    Leads.RecordTypeID,
    Leads.Salutation,
    Leads.State,
    Leads.Status,
    Leads.Street,
    Leads.Title,
    Leads.Website,
    Leads.EmailBouncedDatestamp,
    Leads.EmailBouncedReason,
    Leads.Jigsaw,
    Leads.JigsawContactId,
    Leads.LastReferencedDatestamp,
    Leads.LastViewedDatestamp,
    Leads.PhotoUrl
  FROM
    `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.leads` AS Leads
);
