---description:TestCase1 for SalesActivities
ASSERT
(
  WITH Activities AS (
    SELECT Event.WhatId AS WhatId
    FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Events` AS Event
    UNION ALL
    SELECT Task.WhatId AS WhatId
    FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Tasks` AS Task
  )
  SELECT COUNT(Opportunity.OpportunityId)
  FROM Activities
  INNER JOIN
    `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities` AS Opportunity
    ON
      Activities.WhatId = Opportunity.OpportunityId
  WHERE Activities.WhatId = '0068a00001KXrc0AAD'
)
=
(
  SELECT COUNT(SalesActivities.OpportunityId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.SalesActivities` AS SalesActivities
  WHERE WhatId = '0068a00001KXrc0AAD'
);

---description:TestCase2 for SalesActivities
ASSERT
(
  WITH Activities AS (
    SELECT Event.EventId AS ActivityId,
      Event.WhatId AS WhatId
    FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Events` AS Event
    UNION ALL
    SELECT Task.TaskId AS ActivityId,
      Task.WhatId AS WhatId
    FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Tasks` AS Task
  )
  SELECT Opportunity.Name AS OpportunityName
  FROM Activities
  INNER JOIN
    `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities` AS Opportunity
    ON
      Activities.WhatId = Opportunity.OpportunityId
  WHERE Activities.ActivityId = '00T8a00007rFeCNEA0'
)
=
(
  SELECT OpportunityName
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.SalesActivities`
  WHERE ActivityId = '00T8a00007rFeCNEA0'
);

---description:TestCase3 for SalesActivities
ASSERT
(
  WITH Activities AS (
    SELECT Event.EventId AS ActivityId,
      Event.WhatId AS WhatId
    FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Events` AS Event
    UNION ALL
    SELECT Task.TaskId AS ActivityId,
      Task.WhatId AS WhatId
    FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Tasks` AS Task
  )
  SELECT SUM(Opportunity.Amount)
  FROM Activities
  INNER JOIN
    `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities` AS Opportunity
    ON
      Activities.WhatId = Opportunity.OpportunityId
  WHERE Opportunity.Name = 'Predovic-Lynch'
)
=
(
  SELECT SUM(TotalSaleAmount)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.SalesActivities`
  WHERE OpportunityName = 'Predovic-Lynch'
);

---description:TestCase4 for SalesActivities
ASSERT (
  WITH Activities AS (
    SELECT Event.EventId AS ActivityId,
      Event.OwnerId AS OwnerId
    FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Events` AS Event
    UNION ALL
    SELECT Task.TaskId AS ActivityId,
      Task.OwnerId AS OwnerId
    FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Tasks` AS Task
  )
  SELECT User.Name AS ActivityOwner
  FROM Activities
  INNER JOIN
    `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD` AS User
    ON
      Activities.OwnerId = User.UserId
  WHERE Activities.ActivityId = '00U8a00000neDnMEAU'
)
=
(
  SELECT ActivityOwnerName
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.SalesActivities`
  WHERE ActivityId = '00U8a00000neDnMEAU'
);
