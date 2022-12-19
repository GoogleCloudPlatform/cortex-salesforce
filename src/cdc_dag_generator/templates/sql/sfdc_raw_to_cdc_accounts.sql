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

MERGE INTO `${target_table}` AS T
USING (
  WITH
    S0 AS (
      SELECT * FROM `${source_table}`
      WHERE Recordstamp >= (
        SELECT IFNULL(MAX(Recordstamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `${target_table}`)
    ),
    -- To handle accidental dups from sfdc extraction
    S1 AS (
      SELECT * EXCEPT(row_num)
      FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY AccountId, Recordstamp ORDER BY Recordstamp) AS row_num
        FROM S0
      )
      WHERE row_num = 1
    ),
    T1 AS (
      SELECT AccountId, MAX(Recordstamp) AS Recordstamp
      FROM `${source_table}`
      WHERE Recordstamp >= (
        SELECT IFNULL(MAX(Recordstamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `${target_table}`)
      GROUP BY AccountId
    )
  SELECT S1.*
  FROM S1
  INNER JOIN T1
    ON S1.AccountId = T1.AccountId
      AND S1.Recordstamp = T1.Recordstamp
  ) AS S
ON T.AccountId = S.AccountId
WHEN NOT MATCHED AND IFNULL(S.OperationalFlag, 'I') != 'D' THEN
  INSERT (
    `AccountId`,
    `Name`,
    `ParentId`,
    `Phone`,
    `Fax`,
    `Website`,
    `Type`,
    `Industry`,
    `NumberOfEmployees`,
    `AnnualRevenue`,
    `Description`,
    `AccountSource`,
    `BillingStreet`,
    `BillingCity`,
    `BillingState`,
    `BillingPostalCode`,
    `BillingCountry`,
    `ShippingStreet`,
    `ShippingCity`,
    `ShippingState`,
    `ShippingPostalCode`,
    `ShippingCountry`,
    `OwnerId`,
    `CreatedById`,
    `LastModifiedById`,
    `CreatedDatestamp`,
    `LastModifiedDatestamp`,
    `BillingLatitude`,
    `BillingLongitude`,
    `LastActivityDate`,
    `LastReferencedDatestamp`,
    `LastViewedDatestamp`,
    `MasterRecordId`,
    `RecordTypeId`,
    `ShippingGeocodeAccuracy`,
    `ShippingLatitude`,
    `ShippingLongitude`,
    `Recordstamp`)
  VALUES (
    `AccountId`,
    `Name`,
    `ParentId`,
    `Phone`,
    `Fax`,
    `Website`,
    `Type`,
    `Industry`,
    `NumberOfEmployees`,
    `AnnualRevenue`,
    `Description`,
    `AccountSource`,
    `BillingStreet`,
    `BillingCity`,
    `BillingState`,
    `BillingPostalCode`,
    `BillingCountry`,
    `ShippingStreet`,
    `ShippingCity`,
    `ShippingState`,
    `ShippingPostalCode`,
    `ShippingCountry`,
    `OwnerId`,
    `CreatedById`,
    `LastModifiedById`,
    `CreatedDatestamp`,
    `LastModifiedDatestamp`,
    `BillingLatitude`,
    `BillingLongitude`,
    `LastActivityDate`,
    `LastReferencedDatestamp`,
    `LastViewedDatestamp`,
    `MasterRecordId`,
    `RecordTypeId`,
    `ShippingGeocodeAccuracy`,
    `ShippingLatitude`,
    `ShippingLongitude`,
    `Recordstamp`)
WHEN MATCHED AND S.OperationalFlag = 'D' THEN
  DELETE
WHEN MATCHED AND S.OperationalFlag = 'U' THEN
  UPDATE SET
    T.`AccountId` = S.`AccountId`,
    T.`Name` = S.`Name`,
    T.`ParentId` = S.`ParentId`,
    T.`Phone` = S.`Phone`,
    T.`Fax` = S.`Fax`,
    T.`Website` = S.`Website`,
    T.`Type` = S.`Type`,
    T.`Industry` = S.`Industry`,
    T.`NumberOfEmployees` = S.`NumberOfEmployees`,
    T.`AnnualRevenue` = S.`AnnualRevenue`,
    T.`Description` = S.`Description`,
    T.`AccountSource` = S.`AccountSource`,
    T.`BillingStreet` = S.`BillingStreet`,
    T.`BillingCity` = S.`BillingCity`,
    T.`BillingState` = S.`BillingState`,
    T.`BillingPostalCode` = S.`BillingPostalCode`,
    T.`BillingCountry` = S.`BillingCountry`,
    T.`ShippingStreet` = S.`ShippingStreet`,
    T.`ShippingCity` = S.`ShippingCity`,
    T.`ShippingState` = S.`ShippingState`,
    T.`ShippingPostalCode` = S.`ShippingPostalCode`,
    T.`ShippingCountry` = S.`ShippingCountry`,
    T.`OwnerId` = S.`OwnerId`,
    T.`CreatedById` = S.`CreatedById`,
    T.`LastModifiedById` = S.`LastModifiedById`,
    T.`CreatedDatestamp` = S.`CreatedDatestamp`,
    T.`LastModifiedDatestamp` = S.`LastModifiedDatestamp`,
    T.`BillingLatitude` = S.`BillingLatitude`,
    T.`BillingLongitude` = S.`BillingLongitude`,
    T.`LastActivityDate` = S.`LastActivityDate`,
    T.`LastReferencedDatestamp` = S.`LastReferencedDatestamp`,
    T.`LastViewedDatestamp` = S.`LastViewedDatestamp`,
    T.`MasterRecordId` = S.`MasterRecordId`,
    T.`RecordTypeId` = S.`RecordTypeId`,
    T.`ShippingGeocodeAccuracy` = S.`ShippingGeocodeAccuracy`,
    T.`ShippingLatitude` = S.`ShippingLatitude`,
    T.`ShippingLongitude` = S.`ShippingLongitude`,
    T.`Recordstamp` = S.`Recordstamp`;
