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
        SELECT *, ROW_NUMBER() OVER (PARTITION BY EventId, Recordstamp ORDER BY Recordstamp) AS row_num
        FROM S0
      )
      WHERE row_num = 1
    ),
    T1 AS (
      SELECT EventId, MAX(Recordstamp) AS Recordstamp
      FROM `${source_table}`
      WHERE Recordstamp >= (
        SELECT IFNULL(MAX(Recordstamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `${target_table}`)
      GROUP BY EventId
    )
  SELECT S1.*
  FROM S1
  INNER JOIN T1
    ON S1.EventId = T1.EventId
      AND S1.Recordstamp = T1.Recordstamp
  ) AS S
ON T.EventId = S.EventId
WHEN NOT MATCHED AND IFNULL(S.OperationalFlag, 'I') != 'D' THEN
  INSERT (
    `EventId`,
    `AccountId`,
    `ActivityDate`,
    `ActivityDateTimestamp`,
    `CreatedById`,
    `CreatedDatestamp`,
    `Description`,
    `DurationInMinutes`,
    `EndDate`,
    `EndDateTimestamp`,
    `EventSubtype`,
    `GroupEventType`,
    `IsAllDayEvent`,
    `IsArchived`,
    `IsChild`,
    `IsGroupEvent`,
    `IsPrivate`,
    `IsRecurrence`,
    `IsRecurrence2`,
    `IsRecurrence2Exception`,
    `IsRecurrence2Exclusion`,
    `IsReminderSet`,
    `LastModifiedById`,
    `LastModifiedDatestamp`,
    `Location`,
    `OwnerId`,
    `Recurrence2PatternStartDatestamp`,
    `Recurrence2PatternText`,
    `Recurrence2PatternTimeZone`,
    `Recurrence2PatternVersion`,
    `RecurrenceActivityId`,
    `RecurrenceDayOfMonth`,
    `RecurrenceDayOfWeekMask`,
    `RecurrenceEndDateOnly`,
    `RecurrenceInstance`,
    `RecurrenceInterval`,
    `RecurrenceMonthOfYear`,
    `RecurrenceStartDateTimestamp`,
    `RecurrenceTimeZoneSidKey`,
    `RecurrenceType`,
    `ReminderDateTimestamp`,
    `ShowAs`,
    `StartDateTimestamp`,
    `Subject`,
    `SystemModstamp`,
    `WhatId`,
    `WhoId`,
    `Recordstamp`)
  VALUES (
    `EventId`,
    `AccountId`,
    `ActivityDate`,
    `ActivityDateTimestamp`,
    `CreatedById`,
    `CreatedDatestamp`,
    `Description`,
    `DurationInMinutes`,
    `EndDate`,
    `EndDateTimestamp`,
    `EventSubtype`,
    `GroupEventType`,
    `IsAllDayEvent`,
    `IsArchived`,
    `IsChild`,
    `IsGroupEvent`,
    `IsPrivate`,
    `IsRecurrence`,
    `IsRecurrence2`,
    `IsRecurrence2Exception`,
    `IsRecurrence2Exclusion`,
    `IsReminderSet`,
    `LastModifiedById`,
    `LastModifiedDatestamp`,
    `Location`,
    `OwnerId`,
    `Recurrence2PatternStartDatestamp`,
    `Recurrence2PatternText`,
    `Recurrence2PatternTimeZone`,
    `Recurrence2PatternVersion`,
    `RecurrenceActivityId`,
    `RecurrenceDayOfMonth`,
    `RecurrenceDayOfWeekMask`,
    `RecurrenceEndDateOnly`,
    `RecurrenceInstance`,
    `RecurrenceInterval`,
    `RecurrenceMonthOfYear`,
    `RecurrenceStartDateTimestamp`,
    `RecurrenceTimeZoneSidKey`,
    `RecurrenceType`,
    `ReminderDateTimestamp`,
    `ShowAs`,
    `StartDateTimestamp`,
    `Subject`,
    `SystemModstamp`,
    `WhatId`,
    `WhoId`,
    `Recordstamp`)
WHEN MATCHED AND S.OperationalFlag = 'D' THEN
  DELETE
WHEN MATCHED AND S.OperationalFlag = 'U' THEN
  UPDATE SET
    T.`EventId` = S.`EventId`,
    T.`AccountId` = S.`AccountId`,
    T.`ActivityDate` = S.`ActivityDate`,
    T.`ActivityDateTimestamp` = S.`ActivityDateTimestamp`,
    T.`CreatedById` = S.`CreatedById`,
    T.`CreatedDatestamp` = S.`CreatedDatestamp`,
    T.`Description` = S.`Description`,
    T.`DurationInMinutes` = S.`DurationInMinutes`,
    T.`EndDate` = S.`EndDate`,
    T.`EndDateTimestamp` = S.`EndDateTimestamp`,
    T.`EventSubtype` = S.`EventSubtype`,
    T.`GroupEventType` = S.`GroupEventType`,
    T.`IsAllDayEvent` = S.`IsAllDayEvent`,
    T.`IsArchived` = S.`IsArchived`,
    T.`IsChild` = S.`IsChild`,
    T.`IsGroupEvent` = S.`IsGroupEvent`,
    T.`IsPrivate` = S.`IsPrivate`,
    T.`IsRecurrence` = S.`IsRecurrence`,
    T.`IsRecurrence2` = S.`IsRecurrence2`,
    T.`IsRecurrence2Exception` = S.`IsRecurrence2Exception`,
    T.`IsRecurrence2Exclusion` = S.`IsRecurrence2Exclusion`,
    T.`IsReminderSet` = S.`IsReminderSet`,
    T.`LastModifiedById` = S.`LastModifiedById`,
    T.`LastModifiedDatestamp` = S.`LastModifiedDatestamp`,
    T.`Location` = S.`Location`,
    T.`OwnerId` = S.`OwnerId`,
    T.`Recurrence2PatternStartDatestamp` = S.`Recurrence2PatternStartDatestamp`,
    T.`Recurrence2PatternText` = S.`Recurrence2PatternText`,
    T.`Recurrence2PatternTimeZone` = S.`Recurrence2PatternTimeZone`,
    T.`Recurrence2PatternVersion` = S.`Recurrence2PatternVersion`,
    T.`RecurrenceActivityId` = S.`RecurrenceActivityId`,
    T.`RecurrenceDayOfMonth` = S.`RecurrenceDayOfMonth`,
    T.`RecurrenceDayOfWeekMask` = S.`RecurrenceDayOfWeekMask`,
    T.`RecurrenceEndDateOnly` = S.`RecurrenceEndDateOnly`,
    T.`RecurrenceInstance` = S.`RecurrenceInstance`,
    T.`RecurrenceInterval` = S.`RecurrenceInterval`,
    T.`RecurrenceMonthOfYear` = S.`RecurrenceMonthOfYear`,
    T.`RecurrenceStartDateTimestamp` = S.`RecurrenceStartDateTimestamp`,
    T.`RecurrenceTimeZoneSidKey` = S.`RecurrenceTimeZoneSidKey`,
    T.`RecurrenceType` = S.`RecurrenceType`,
    T.`ReminderDateTimestamp` = S.`ReminderDateTimestamp`,
    T.`ShowAs` = S.`ShowAs`,
    T.`StartDateTimestamp` = S.`StartDateTimestamp`,
    T.`Subject` = S.`Subject`,
    T.`SystemModstamp` = S.`SystemModstamp`,
    T.`WhatId` = S.`WhatId`,
    T.`WhoId` = S.`WhoId`,
    T.`Recordstamp` = S.`Recordstamp`;
