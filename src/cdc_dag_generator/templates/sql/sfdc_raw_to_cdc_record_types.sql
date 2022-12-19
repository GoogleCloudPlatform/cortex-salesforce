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
        SELECT *, ROW_NUMBER() OVER (PARTITION BY RecordTypeId, Recordstamp ORDER BY Recordstamp) AS row_num
        FROM S0
      )
      WHERE row_num = 1
    ),
    T1 AS (
      SELECT RecordTypeId, MAX(Recordstamp) AS Recordstamp
      FROM `${source_table}`
      WHERE Recordstamp >= (
        SELECT IFNULL(MAX(Recordstamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `${target_table}`)
      GROUP BY RecordTypeId
    )
  SELECT S1.*
  FROM S1
  INNER JOIN T1
    ON S1.RecordTypeId = T1.RecordTypeId
      AND S1.Recordstamp = T1.Recordstamp
  ) AS S
ON T.RecordTypeId = S.RecordTypeId
WHEN NOT MATCHED AND IFNULL(S.OperationalFlag, 'I') != 'D' THEN
  INSERT (
    `RecordTypeId`,
    `Name`,
    `DeveloperName`,
    `NamespacePrefix`,
    `Description`,
    `BusinessProcessId`,
    `SobjectType`,
    `IsActive`,
    `CreatedById`,
    `CreatedDatestamp`,
    `LastModifiedById`,
    `LastModifiedDatestamp`,
    `SystemModstamp`,
    `Recordstamp`)
  VALUES (
    `RecordTypeId`,
    `Name`,
    `DeveloperName`,
    `NamespacePrefix`,
    `Description`,
    `BusinessProcessId`,
    `SobjectType`,
    `IsActive`,
    `CreatedById`,
    `CreatedDatestamp`,
    `LastModifiedById`,
    `LastModifiedDatestamp`,
    `SystemModstamp`,
    `Recordstamp`)
WHEN MATCHED AND S.OperationalFlag = 'D' THEN
  DELETE
WHEN MATCHED AND S.OperationalFlag = 'U' THEN
  UPDATE SET
    T.`RecordTypeId` = S.`RecordTypeId`,
    T.`Name` = S.`Name`,
    T.`DeveloperName` = S.`DeveloperName`,
    T.`NamespacePrefix` = S.`NamespacePrefix`,
    T.`Description` = S.`Description`,
    T.`BusinessProcessId` = S.`BusinessProcessId`,
    T.`SobjectType` = S.`SobjectType`,
    T.`IsActive` = S.`IsActive`,
    T.`CreatedById` = S.`CreatedById`,
    T.`CreatedDatestamp` = S.`CreatedDatestamp`,
    T.`LastModifiedById` = S.`LastModifiedById`,
    T.`LastModifiedDatestamp` = S.`LastModifiedDatestamp`,
    T.`SystemModstamp` = S.`SystemModstamp`,
    T.`Recordstamp` = S.`Recordstamp`;
