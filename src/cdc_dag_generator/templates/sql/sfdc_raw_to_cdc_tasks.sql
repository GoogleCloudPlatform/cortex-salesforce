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
        SELECT *, ROW_NUMBER() OVER (PARTITION BY TaskId, Recordstamp ORDER BY Recordstamp) AS row_num
        FROM S0
      )
      WHERE row_num = 1
    ),
    T1 AS (
      SELECT TaskId, MAX(Recordstamp) AS Recordstamp
      FROM `${source_table}`
      WHERE Recordstamp >= (
        SELECT IFNULL(MAX(Recordstamp), TIMESTAMP('1940-12-25 05:30:00+00'))
        FROM `${target_table}`)
      GROUP BY TaskId
    )
  SELECT S1.*
  FROM S1
  INNER JOIN T1
    ON S1.TaskId = T1.TaskId
      AND S1.Recordstamp = T1.Recordstamp
  ) AS S
ON T.TaskId = S.TaskId
WHEN NOT MATCHED AND IFNULL(S.OperationalFlag, 'I') != 'D' THEN
  INSERT (
    `TaskId`,
    `AccountId`,
    `ActivityDate`,
    `CallDisposition`,
    `CallDurationInSeconds`,
    `CallObject`,
    `CallType`,
    `CompletedDateTimestamp`,
    `CreatedById`,
    `CreatedDatestamp`,
    `Description`,
    `IsArchived`,
    `IsClosed`,
    `IsHighPriority`,
    `IsRecurrence`,
    `IsReminderSet`,
    `LastModifiedById`,
    `LastModifiedDatestamp`,
    `OwnerId`,
    `Priority`,
    `RecurrenceActivityId`,
    `RecurrenceDayOfMonth`,
    `RecurrenceDayOfWeekMask`,
    `RecurrenceEndDateOnly`,
    `RecurrenceInstance`,
    `RecurrenceInterval`,
    `RecurrenceMonthOfYear`,
    `RecurrenceRegeneratedType`,
    `RecurrenceStartDateOnly`,
    `RecurrenceTimeZoneSidKey`,
    `RecurrenceType`,
    `ReminderDateTimestamp`,
    `Status`,
    `Subject`,
    `SystemModstamp`,
    `TaskSubtype`,
    `WhatId`,
    `WhoId`,
    `Recordstamp`)
  VALUES (
    `TaskId`,
    `AccountId`,
    `ActivityDate`,
    `CallDisposition`,
    `CallDurationInSeconds`,
    `CallObject`,
    `CallType`,
    `CompletedDateTimestamp`,
    `CreatedById`,
    `CreatedDatestamp`,
    `Description`,
    `IsArchived`,
    `IsClosed`,
    `IsHighPriority`,
    `IsRecurrence`,
    `IsReminderSet`,
    `LastModifiedById`,
    `LastModifiedDatestamp`,
    `OwnerId`,
    `Priority`,
    `RecurrenceActivityId`,
    `RecurrenceDayOfMonth`,
    `RecurrenceDayOfWeekMask`,
    `RecurrenceEndDateOnly`,
    `RecurrenceInstance`,
    `RecurrenceInterval`,
    `RecurrenceMonthOfYear`,
    `RecurrenceRegeneratedType`,
    `RecurrenceStartDateOnly`,
    `RecurrenceTimeZoneSidKey`,
    `RecurrenceType`,
    `ReminderDateTimestamp`,
    `Status`,
    `Subject`,
    `SystemModstamp`,
    `TaskSubtype`,
    `WhatId`,
    `WhoId`,
    `Recordstamp`)
WHEN MATCHED AND S.OperationalFlag = 'D' THEN
  DELETE
WHEN MATCHED AND S.OperationalFlag = 'U' THEN
  UPDATE SET
    T.`TaskId` = S.`TaskId`,
    T.`AccountId` = S.`AccountId`,
    T.`ActivityDate` = S.`ActivityDate`,
    T.`CallDisposition` = S.`CallDisposition`,
    T.`CallDurationInSeconds` = S.`CallDurationInSeconds`,
    T.`CallObject` = S.`CallObject`,
    T.`CallType` = S.`CallType`,
    T.`CompletedDateTimestamp` = S.`CompletedDateTimestamp`,
    T.`CreatedById` = S.`CreatedById`,
    T.`CreatedDatestamp` = S.`CreatedDatestamp`,
    T.`Description` = S.`Description`,
    T.`IsArchived` = S.`IsArchived`,
    T.`IsClosed` = S.`IsClosed`,
    T.`IsHighPriority` = S.`IsHighPriority`,
    T.`IsRecurrence` = S.`IsRecurrence`,
    T.`IsReminderSet` = S.`IsReminderSet`,
    T.`LastModifiedById` = S.`LastModifiedById`,
    T.`LastModifiedDatestamp` = S.`LastModifiedDatestamp`,
    T.`OwnerId` = S.`OwnerId`,
    T.`Priority` = S.`Priority`,
    T.`RecurrenceActivityId` = S.`RecurrenceActivityId`,
    T.`RecurrenceDayOfMonth` = S.`RecurrenceDayOfMonth`,
    T.`RecurrenceDayOfWeekMask` = S.`RecurrenceDayOfWeekMask`,
    T.`RecurrenceEndDateOnly` = S.`RecurrenceEndDateOnly`,
    T.`RecurrenceInstance` = S.`RecurrenceInstance`,
    T.`RecurrenceInterval` = S.`RecurrenceInterval`,
    T.`RecurrenceMonthOfYear` = S.`RecurrenceMonthOfYear`,
    T.`RecurrenceRegeneratedType` = S.`RecurrenceRegeneratedType`,
    T.`RecurrenceStartDateOnly` = S.`RecurrenceStartDateOnly`,
    T.`RecurrenceTimeZoneSidKey` = S.`RecurrenceTimeZoneSidKey`,
    T.`RecurrenceType` = S.`RecurrenceType`,
    T.`ReminderDateTimestamp` = S.`ReminderDateTimestamp`,
    T.`Status` = S.`Status`,
    T.`Subject` = S.`Subject`,
    T.`SystemModstamp` = S.`SystemModstamp`,
    T.`TaskSubtype` = S.`TaskSubtype`,
    T.`WhatId` = S.`WhatId`,
    T.`WhoId` = S.`WhoId`,
    T.`Recordstamp` = S.`Recordstamp`;
