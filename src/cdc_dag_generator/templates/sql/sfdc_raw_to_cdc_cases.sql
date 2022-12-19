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
        SELECT *, ROW_NUMBER() OVER (PARTITION BY CaseId, Recordstamp ORDER BY Recordstamp) AS row_num
        FROM S0
      )
      WHERE row_num = 1
    ),
    T1 AS (
      SELECT CaseId, MAX(Recordstamp) AS Recordstamp
      FROM `${source_table}`
      WHERE Recordstamp >= (
        SELECT IFNULL(MAX(Recordstamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `${target_table}`)
      GROUP BY CaseId
    )
  SELECT S1.*
  FROM S1
  INNER JOIN T1
    ON S1.CaseId = T1.CaseId
      AND S1.Recordstamp = T1.Recordstamp
  ) AS S
ON T.CaseId = S.CaseId
WHEN NOT MATCHED AND IFNULL(S.OperationalFlag, 'I') != 'D' THEN
  INSERT (
    `CaseId`,
    `AccountId`,
    `CaseNumber`,
    `ClosedDatestamp`,
    `Comments`,
    `ContactEmail`,
    `ContactFax`,
    `ContactId`,
    `ContactMobile`,
    `ContactPhone`,
    `CreatedById`,
    `CreatedDatestamp`,
    `Description`,
    `EntitlementId`,
    `IsClosed`,
    `IsEscalated`,
    `Language`,
    `LastModifiedById`,
    `LastModifiedDatestamp`,
    `LastReferencedDatestamp`,
    `LastViewedDatestamp`,
    `MasterRecordId`,
    `Origin`,
    `OwnerId`,
    `ParentId`,
    `Priority`,
    `Reason`,
    `RecordTypeId`,
    `SlaExitDatestamp`,
    `SlaStartDatestamp`,
    `Status`,
    `Subject`,
    `SuppliedCompany`,
    `SuppliedEmail`,
    `SuppliedName`,
    `SuppliedPhone`,
    `SystemModstamp`,
    `Type`,
    `Recordstamp`)
  VALUES (
    `CaseId`,
    `AccountId`,
    `CaseNumber`,
    `ClosedDatestamp`,
    `Comments`,
    `ContactEmail`,
    `ContactFax`,
    `ContactId`,
    `ContactMobile`,
    `ContactPhone`,
    `CreatedById`,
    `CreatedDatestamp`,
    `Description`,
    `EntitlementId`,
    `IsClosed`,
    `IsEscalated`,
    `Language`,
    `LastModifiedById`,
    `LastModifiedDatestamp`,
    `LastReferencedDatestamp`,
    `LastViewedDatestamp`,
    `MasterRecordId`,
    `Origin`,
    `OwnerId`,
    `ParentId`,
    `Priority`,
    `Reason`,
    `RecordTypeId`,
    `SlaExitDatestamp`,
    `SlaStartDatestamp`,
    `Status`,
    `Subject`,
    `SuppliedCompany`,
    `SuppliedEmail`,
    `SuppliedName`,
    `SuppliedPhone`,
    `SystemModstamp`,
    `Type`,
    `Recordstamp`)
WHEN MATCHED AND S.OperationalFlag = 'D' THEN
  DELETE
WHEN MATCHED AND S.OperationalFlag = 'U' THEN
  UPDATE SET
    T.`CaseId` = S.`CaseId`,
    T.`AccountId` = S.`AccountId`,
    T.`CaseNumber` = S.`CaseNumber`,
    T.`ClosedDatestamp` = S.`ClosedDatestamp`,
    T.`Comments` = S.`Comments`,
    T.`ContactEmail` = S.`ContactEmail`,
    T.`ContactFax` = S.`ContactFax`,
    T.`ContactId` = S.`ContactId`,
    T.`ContactMobile` = S.`ContactMobile`,
    T.`ContactPhone` = S.`ContactPhone`,
    T.`CreatedById` = S.`CreatedById`,
    T.`CreatedDatestamp` = S.`CreatedDatestamp`,
    T.`Description` = S.`Description`,
    T.`EntitlementId` = S.`EntitlementId`,
    T.`IsClosed` = S.`IsClosed`,
    T.`IsEscalated` = S.`IsEscalated`,
    T.`Language` = S.`Language`,
    T.`LastModifiedById` = S.`LastModifiedById`,
    T.`LastModifiedDatestamp` = S.`LastModifiedDatestamp`,
    T.`LastReferencedDatestamp` = S.`LastReferencedDatestamp`,
    T.`LastViewedDatestamp` = S.`LastViewedDatestamp`,
    T.`MasterRecordId` = S.`MasterRecordId`,
    T.`Origin` = S.`Origin`,
    T.`OwnerId` = S.`OwnerId`,
    T.`ParentId` = S.`ParentId`,
    T.`Priority` = S.`Priority`,
    T.`Reason` = S.`Reason`,
    T.`RecordTypeId` = S.`RecordTypeId`,
    T.`SlaExitDatestamp` = S.`SlaExitDatestamp`,
    T.`SlaStartDatestamp` = S.`SlaStartDatestamp`,
    T.`Status` = S.`Status`,
    T.`Subject` = S.`Subject`,
    T.`SuppliedCompany` = S.`SuppliedCompany`,
    T.`SuppliedEmail` = S.`SuppliedEmail`,
    T.`SuppliedName` = S.`SuppliedName`,
    T.`SuppliedPhone` = S.`SuppliedPhone`,
    T.`SystemModstamp` = S.`SystemModstamp`,
    T.`Type` = S.`Type`,
    T.`Recordstamp` = S.`Recordstamp`;