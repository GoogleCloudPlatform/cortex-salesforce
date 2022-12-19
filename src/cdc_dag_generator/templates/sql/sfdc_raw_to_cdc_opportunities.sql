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
        SELECT *, ROW_NUMBER() OVER (PARTITION BY OpportunityId, Recordstamp ORDER BY Recordstamp) AS row_num
        FROM S0
      )
      WHERE row_num = 1
    ),
    T1 AS (
      SELECT OpportunityId, MAX(Recordstamp) AS Recordstamp
      FROM `${source_table}`
      WHERE Recordstamp >= (
        SELECT IFNULL(MAX(Recordstamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `${target_table}`)
      GROUP BY OpportunityId
    )
  SELECT S1.*
  FROM S1
  INNER JOIN T1
    ON S1.OpportunityId = T1.OpportunityId
      AND S1.Recordstamp = T1.Recordstamp
  ) AS S
ON T.OpportunityId = S.OpportunityId
WHEN NOT MATCHED AND IFNULL(S.OperationalFlag, 'I') != 'D' THEN
  INSERT (
    `OpportunityId`,
    `AccountId`,
    `Amount`,
    `CampaignId`,
    `CloseDate`,
    `ContactId`,
    `CreatedById`,
    `CreatedDatestamp`,
    `Description`,
    `Fiscal`,
    `FiscalQuarter`,
    `FiscalYear`,
    `ForecastCategory`,
    `ForecastCategoryName`,
    `HasOpenActivity`,
    `HasOpportunityLineItem`,
    `HasOverdueTask`,
    `IsClosed`,
    `IsWon`,
    `LastActivityDate`,
    `LastAmountChangedHistoryId`,
    `LastCloseDateChangedHistoryId`,
    `LastModifiedById`,
    `LastModifiedDatestamp`,
    `LastReferencedDatestamp`,
    `LastStageChangeDatestamp`,
    `LastViewedDatestamp`,
    `LeadSource`,
    `Name`,
    `NextStep`,
    `OwnerId`,
    `Pricebook2Id`,
    `Probability`,
    `RecordTypeId`,
    `StageName`,
    `SystemModstamp`,
    `Type`,
    `Recordstamp`)
  VALUES (
    `OpportunityId`,
    `AccountId`,
    `Amount`,
    `CampaignId`,
    `CloseDate`,
    `ContactId`,
    `CreatedById`,
    `CreatedDatestamp`,
    `Description`,
    `Fiscal`,
    `FiscalQuarter`,
    `FiscalYear`,
    `ForecastCategory`,
    `ForecastCategoryName`,
    `HasOpenActivity`,
    `HasOpportunityLineItem`,
    `HasOverdueTask`,
    `IsClosed`,
    `IsWon`,
    `LastActivityDate`,
    `LastAmountChangedHistoryId`,
    `LastCloseDateChangedHistoryId`,
    `LastModifiedById`,
    `LastModifiedDatestamp`,
    `LastReferencedDatestamp`,
    `LastStageChangeDatestamp`,
    `LastViewedDatestamp`,
    `LeadSource`,
    `Name`,
    `NextStep`,
    `OwnerId`,
    `Pricebook2Id`,
    `Probability`,
    `RecordTypeId`,
    `StageName`,
    `SystemModstamp`,
    `Type`,
    `Recordstamp`)
  WHEN MATCHED AND S.OperationalFlag = 'D' THEN
    DELETE
  WHEN MATCHED AND S.OperationalFlag = 'U' THEN
    UPDATE SET
      T.`OpportunityId` = S.`OpportunityId`,
      T.`AccountId` = S.`AccountId`,
      T.`Amount` = S.`Amount`,
      T.`CampaignId` = S.`CampaignId`,
      T.`CloseDate` = S.`CloseDate`,
      T.`ContactId` = S.`ContactId`,
      T.`CreatedById` = S.`CreatedById`,
      T.`CreatedDatestamp` = S.`CreatedDatestamp`,
      T.`Description` = S.`Description`,
      T.`Fiscal` = S.`Fiscal`,
      T.`FiscalQuarter` = S.`FiscalQuarter`,
      T.`FiscalYear` = S.`FiscalYear`,
      T.`ForecastCategory` = S.`ForecastCategory`,
      T.`ForecastCategoryName` = S.`ForecastCategoryName`,
      T.`HasOpenActivity` = S.`HasOpenActivity`,
      T.`HasOpportunityLineItem` = S.`HasOpportunityLineItem`,
      T.`HasOverdueTask` = S.`HasOverdueTask`,
      T.`IsClosed` = S.`IsClosed`,
      T.`IsWon` = S.`IsWon`,
      T.`LastActivityDate` = S.`LastActivityDate`,
      T.`LastAmountChangedHistoryId` = S.`LastAmountChangedHistoryId`,
      T.`LastCloseDateChangedHistoryId` = S.`LastCloseDateChangedHistoryId`,
      T.`LastModifiedById` = S.`LastModifiedById`,
      T.`LastModifiedDatestamp` = S.`LastModifiedDatestamp`,
      T.`LastReferencedDatestamp` = S.`LastReferencedDatestamp`,
      T.`LastStageChangeDatestamp` = S.`LastStageChangeDatestamp`,
      T.`LastViewedDatestamp` = S.`LastViewedDatestamp`,
      T.`LeadSource` = S.`LeadSource`,
      T.`Name` = S.`Name`,
      T.`NextStep` = S.`NextStep`,
      T.`OwnerId` = S.`OwnerId`,
      T.`Pricebook2Id` = S.`Pricebook2Id`,
      T.`Probability` = S.`Probability`,
      T.`RecordTypeId` = S.`RecordTypeId`,
      T.`StageName` = S.`StageName`,
      T.`SystemModstamp` = S.`SystemModstamp`,
      T.`Type` = S.`Type`,
      T.`Recordstamp` = S.`Recordstamp`;
