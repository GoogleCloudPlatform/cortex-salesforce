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
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ContactId, Recordstamp ORDER BY Recordstamp) AS row_num
        FROM S0
      )
      WHERE row_num = 1
    ),
    T1 AS (
      SELECT ContactId, MAX(Recordstamp) AS Recordstamp
      FROM `${source_table}`
      WHERE Recordstamp >= (
        SELECT IFNULL(MAX(Recordstamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `${target_table}`)
      GROUP BY ContactId
    )
  SELECT S1.*
  FROM S1
  INNER JOIN T1
    ON S1.ContactId = T1.ContactId
      AND S1.Recordstamp = T1.Recordstamp
  ) AS S
ON T.ContactId = S.ContactId
WHEN NOT MATCHED AND IFNULL(S.OperationalFlag, 'I') != 'D' THEN
  INSERT (
    `ContactId`,
    `AccountId`,
    `AssistantName`,
    `AssistantPhone`,
    `Birthdate`,
    `CreatedById`,
    `CreatedDatestamp`,
    `Department`,
    `Description`,
    `Email`,
    `EmailBouncedDatestamp`,
    `EmailBouncedReason`,
    `Fax`,
    `FirstName`,
    `HomePhone`,
    `IndividualId`,
    `IsEmailBounced`,
    `Jigsaw`,
    `JigsawContactId`,
    `LastActivityDate`,
    `LastCURequestDatestamp`,
    `LastCUUpdateDatestamp`,
    `LastModifiedById`,
    `LastModifiedDatestamp`,
    `LastName`,
    `LastReferencedDatestamp`,
    `LastViewedDatestamp`,
    `LeadSource`,
    `MailingCity`,
    `MailingCountry`,
    `MailingGeocodeAccuracy`,
    `MailingLatitude`,
    `MailingLongitude`,
    `MailingPostalCode`,
    `MailingState`,
    `MailingStreet`,
    `MasterRecordId`,
    `MobilePhone`,
    `Name`,
    `OtherCity`,
    `OtherCountry`,
    `OtherGeocodeAccuracy`,
    `OtherLatitude`,
    `OtherLongitude`,
    `OtherPhone`,
    `OtherPostalCode`,
    `OtherState`,
    `OtherStreet`,
    `OwnerId`,
    `Phone`,
    `PhotoUrl`,
    `RecordTypeId`,
    `ReportsToId`,
    `Salutation`,
    `SystemModstamp`,
    `Title`,
    `Recordstamp`)
    VALUES (
      `ContactId`,
      `AccountId`,
      `AssistantName`,
      `AssistantPhone`,
      `Birthdate`,
      `CreatedById`,
      `CreatedDatestamp`,
      `Department`,
      `Description`,
      `Email`,
      `EmailBouncedDatestamp`,
      `EmailBouncedReason`,
      `Fax`,
      `FirstName`,
      `HomePhone`,
      `IndividualId`,
      `IsEmailBounced`,
      `Jigsaw`,
      `JigsawContactId`,
      `LastActivityDate`,
      `LastCURequestDatestamp`,
      `LastCUUpdateDatestamp`,
      `LastModifiedById`,
      `LastModifiedDatestamp`,
      `LastName`,
      `LastReferencedDatestamp`,
      `LastViewedDatestamp`,
      `LeadSource`,
      `MailingCity`,
      `MailingCountry`,
      `MailingGeocodeAccuracy`,
      `MailingLatitude`,
      `MailingLongitude`,
      `MailingPostalCode`,
      `MailingState`,
      `MailingStreet`,
      `MasterRecordId`,
      `MobilePhone`,
      `Name`,
      `OtherCity`,
      `OtherCountry`,
      `OtherGeocodeAccuracy`,
      `OtherLatitude`,
      `OtherLongitude`,
      `OtherPhone`,
      `OtherPostalCode`,
      `OtherState`,
      `OtherStreet`,
      `OwnerId`,
      `Phone`,
      `PhotoUrl`,
      `RecordTypeId`,
      `ReportsToId`,
      `Salutation`,
      `SystemModstamp`,
      `Title`,
      `Recordstamp`)
WHEN MATCHED AND S.OperationalFlag='D' THEN
  DELETE
WHEN MATCHED AND S.OperationalFlag = 'U' THEN
	UPDATE SET
    T.`ContactId` = S.`ContactId`,
    T.`AccountId` = S.`AccountId`,
    T.`AssistantName` = S.`AssistantName`,
    T.`AssistantPhone` = S.`AssistantPhone`,
    T.`Birthdate` = S.`Birthdate`,
    T.`CreatedById` = S.`CreatedById`,
    T.`CreatedDatestamp` = S.`CreatedDatestamp`,
    T.`Department` = S.`Department`,
    T.`Description` = S.`Description`,
    T.`Email` = S.`Email`,
    T.`EmailBouncedDatestamp` = S.`EmailBouncedDatestamp`,
    T.`EmailBouncedReason` = S.`EmailBouncedReason`,
    T.`Fax` = S.`Fax`,
    T.`FirstName` = S.`FirstName`,
    T.`HomePhone` = S.`HomePhone`,
    T.`IndividualId` = S.`IndividualId`,
    T.`IsEmailBounced` = S.`IsEmailBounced`,
    T.`Jigsaw` = S.`Jigsaw`,
    T.`JigsawContactId` = S.`JigsawContactId`,
    T.`LastActivityDate` = S.`LastActivityDate`,
    T.`LastCURequestDatestamp` = S.`LastCURequestDatestamp`,
    T.`LastCUUpdateDatestamp` = S.`LastCUUpdateDatestamp`,
    T.`LastModifiedById` = S.`LastModifiedById`,
    T.`LastModifiedDatestamp` = S.`LastModifiedDatestamp`,
    T.`LastName` = S.`LastName`,
    T.`LastReferencedDatestamp` = S.`LastReferencedDatestamp`,
    T.`LastViewedDatestamp` = S.`LastViewedDatestamp`,
    T.`LeadSource` = S.`LeadSource`,
    T.`MailingCity` = S.`MailingCity`,
    T.`MailingCountry` = S.`MailingCountry`,
    T.`MailingGeocodeAccuracy` = S.`MailingGeocodeAccuracy`,
    T.`MailingLatitude` = S.`MailingLatitude`,
    T.`MailingLongitude` = S.`MailingLongitude`,
    T.`MailingPostalCode` = S.`MailingPostalCode`,
    T.`MailingState` = S.`MailingState`,
    T.`MailingStreet` = S.`MailingStreet`,
    T.`MasterRecordId` = S.`MasterRecordId`,
    T.`MobilePhone` = S.`MobilePhone`,
    T.`Name` = S.`Name`,
    T.`OtherCity` = S.`OtherCity`,
    T.`OtherCountry` = S.`OtherCountry`,
    T.`OtherGeocodeAccuracy` = S.`OtherGeocodeAccuracy`,
    T.`OtherLatitude` = S.`OtherLatitude`,
    T.`OtherLongitude` = S.`OtherLongitude`,
    T.`OtherPhone` = S.`OtherPhone`,
    T.`OtherPostalCode` = S.`OtherPostalCode`,
    T.`OtherState` = S.`OtherState`,
    T.`OtherStreet` = S.`OtherStreet`,
    T.`OwnerId` = S.`OwnerId`,
    T.`Phone` = S.`Phone`,
    T.`PhotoUrl` = S.`PhotoUrl`,
    T.`RecordTypeId` = S.`RecordTypeId`,
    T.`ReportsToId` = S.`ReportsToId`,
    T.`Salutation` = S.`Salutation`,
    T.`SystemModstamp` = S.`SystemModstamp`,
    T.`Title` = S.`Title`,
    T.`Recordstamp` = S.`Recordstamp`;