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
        SELECT *, ROW_NUMBER() OVER (PARTITION BY LeadId, Recordstamp ORDER BY Recordstamp) AS row_num
        FROM S0
      )
      WHERE row_num = 1
    ),
    T1 AS (
      SELECT LeadId, MAX(Recordstamp) AS Recordstamp
      FROM `${source_table}`
      WHERE Recordstamp >= (
        SELECT IFNULL(MAX(Recordstamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `${target_table}`)
      GROUP BY LeadId
    )
  SELECT S1.*
  FROM S1
  INNER JOIN T1
    ON S1.LeadId = T1.LeadId
      AND S1.Recordstamp = T1.Recordstamp
  ) AS S
ON T.LeadId = S.LeadId
WHEN NOT MATCHED AND IFNULL(S.OperationalFlag, 'I') != 'D' THEN
  INSERT (
    `LeadId`,
    `AnnualRevenue`,
    `City`,
    `Company`,
    `ConvertedAccountId`,
    `ConvertedContactId`,
    `ConvertedDate`,
    `ConvertedOpportunityId`,
    `Country`,
    `CreatedById`,
    `CreatedDatestamp`,
    `Description`,
    `Email`,
    `EmailBouncedDatestamp`,
    `EmailBouncedReason`,
    `FirstName`,
    `GeocodeAccuracy`,
    `IndividualId`,
    `Industry`,
    `IsConverted`,
    `IsUnreadByOwner`,
    `Jigsaw`,
    `JigsawContactId`,
    `LastActivityDate`,
    `LastModifiedById`,
    `LastModifiedDatestamp`,
    `LastName`,
    `LastReferencedDatestamp`,
    `LastViewedDatestamp`,
    `Latitude`,
    `LeadSource`,
    `Longitude`,
    `MasterRecordId`,
    `Name`,
    `NumberOfEmployees`,
    `OwnerId`,
    `Phone`,
    `PhotoUrl`,
    `PostalCode`,
    `Rating`,
    `RecordTypeId`,
    `Salutation`,
    `State`,
    `Status`,
    `Street`,
    `SystemModstamp`,
    `Title`,
    `Website`,
    `Recordstamp`)
  VALUES (
    `LeadId`,
    `AnnualRevenue`,
    `City`,
    `Company`,
    `ConvertedAccountId`,
    `ConvertedContactId`,
    `ConvertedDate`,
    `ConvertedOpportunityId`,
    `Country`,
    `CreatedById`,
    `CreatedDatestamp`,
    `Description`,
    `Email`,
    `EmailBouncedDatestamp`,
    `EmailBouncedReason`,
    `FirstName`,
    `GeocodeAccuracy`,
    `IndividualId`,
    `Industry`,
    `IsConverted`,
    `IsUnreadByOwner`,
    `Jigsaw`,
    `JigsawContactId`,
    `LastActivityDate`,
    `LastModifiedById`,
    `LastModifiedDatestamp`,
    `LastName`,
    `LastReferencedDatestamp`,
    `LastViewedDatestamp`,
    `Latitude`,
    `LeadSource`,
    `Longitude`,
    `MasterRecordId`,
    `Name`,
    `NumberOfEmployees`,
    `OwnerId`,
    `Phone`,
    `PhotoUrl`,
    `PostalCode`,
    `Rating`,
    `RecordTypeId`,
    `Salutation`,
    `State`,
    `Status`,
    `Street`,
    `SystemModstamp`,
    `Title`,
    `Website`,
    `Recordstamp`)
WHEN MATCHED AND S.OperationalFlag = 'D' THEN
  DELETE
WHEN MATCHED AND S.OperationalFlag = 'U' THEN
  UPDATE SET
    T.`LeadId` = S.`LeadId`,
    T.`AnnualRevenue` = S.`AnnualRevenue`,
    T.`City` = S.`City`,
    T.`Company` = S.`Company`,
    T.`ConvertedAccountId` = S.`ConvertedAccountId`,
    T.`ConvertedContactId` = S.`ConvertedContactId`,
    T.`ConvertedDate` = S.`ConvertedDate`,
    T.`ConvertedOpportunityId` = S.`ConvertedOpportunityId`,
    T.`Country` = S.`Country`,
    T.`CreatedById` = S.`CreatedById`,
    T.`CreatedDatestamp` = S.`CreatedDatestamp`,
    T.`Description` = S.`Description`,
    T.`Email` = S.`Email`,
    T.`EmailBouncedDatestamp` = S.`EmailBouncedDatestamp`,
    T.`EmailBouncedReason` = S.`EmailBouncedReason`,
    T.`FirstName` = S.`FirstName`,
    T.`GeocodeAccuracy` = S.`GeocodeAccuracy`,
    T.`IndividualId` = S.`IndividualId`,
    T.`Industry` = S.`Industry`,
    T.`IsConverted` = S.`IsConverted`,
    T.`IsUnreadByOwner` = S.`IsUnreadByOwner`,
    T.`Jigsaw` = S.`Jigsaw`,
    T.`JigsawContactId` = S.`JigsawContactId`,
    T.`LastActivityDate` = S.`LastActivityDate`,
    T.`LastModifiedById` = S.`LastModifiedById`,
    T.`LastModifiedDatestamp` = S.`LastModifiedDatestamp`,
    T.`LastName` = S.`LastName`,
    T.`LastReferencedDatestamp` = S.`LastReferencedDatestamp`,
    T.`LastViewedDatestamp` = S.`LastViewedDatestamp`,
    T.`Latitude` = S.`Latitude`,
    T.`LeadSource` = S.`LeadSource`,
    T.`Longitude` = S.`Longitude`,
    T.`MasterRecordId` = S.`MasterRecordId`,
    T.`Name` = S.`Name`,
    T.`NumberOfEmployees` = S.`NumberOfEmployees`,
    T.`OwnerId` = S.`OwnerId`,
    T.`Phone` = S.`Phone`,
    T.`PhotoUrl` = S.`PhotoUrl`,
    T.`PostalCode` = S.`PostalCode`,
    T.`Rating` = S.`Rating`,
    T.`RecordTypeId` = S.`RecordTypeId`,
    T.`Salutation` = S.`Salutation`,
    T.`State` = S.`State`,
    T.`Status` = S.`Status`,
    T.`Street` = S.`Street`,
    T.`SystemModstamp` = S.`SystemModstamp`,
    T.`Title` = S.`Title`,
    T.`Website` = S.`Website`,
    T.`Recordstamp` = S.`Recordstamp`;
