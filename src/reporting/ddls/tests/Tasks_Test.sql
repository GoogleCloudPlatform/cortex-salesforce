---description:TestCase1 for Task
ASSERT
(
  SELECT Status
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Tasks`
  WHERE TaskId = '00T8a00007lTgGfEAK' AND Priority = 'High'
)
=
(
  SELECT Status
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.tasks`
  WHERE TaskId = '00T8a00007lTgGfEAK' AND Priority = 'High'
);

---description:TestCase2 for Task
ASSERT
(
  SELECT TaskId
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Tasks`
  WHERE WhoId = '00Q8a00001tMWRREA4' AND Subject = 'Send Letter' AND OwnerId = '0058a00000LvkJmAAJ'
)
=
(
  SELECT TaskId
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.tasks`
  WHERE WhoId = '00Q8a00001tMWRREA4' AND Subject = 'Send Letter' AND OwnerId = '0058a00000LvkJmAAJ'
);

---description:TestCase3 for Task
ASSERT
(
  SELECT COUNT(TaskId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Tasks`
  WHERE WhatId = '0068a00001JvFTbAAN' AND Status = 'Completed' AND Priority = 'Normal'
)
=
(
  SELECT COUNT(TaskId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.tasks`
  WHERE WhatId = '0068a00001JvFTbAAN' AND Status = 'Completed' AND Priority = 'Normal'
);

---description:TestCase4 for Task
ASSERT
(
  SELECT COUNT(TaskId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Tasks`
  WHERE Priority = 'Normal' AND Subject = 'Call'
)
=
(
  SELECT COUNT(TaskId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.tasks`
  WHERE Priority = 'Normal' AND Subject = 'Call'
);

---description:TestCase5 for Task
ASSERT
(
  SELECT AccountId
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Tasks`
  WHERE TaskId = '00T8a00007hr3QhEAI' AND Status = 'Waiting on someone else'
)
=
(
  SELECT AccountId
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.tasks`
  WHERE TaskId = '00T8a00007hr3QhEAI' AND Status = 'Waiting on someone else'
);

---description:TestCase6 for Task
ASSERT
(
  SELECT COUNT(TaskId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Tasks`
  WHERE Status = 'Not Started' AND CreatedByID = '0058a00000LiM2BAAV'
)
=
(
  SELECT COUNT(TaskId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.tasks`
  WHERE Status = 'Not Started' AND CreatedByID = '0058a00000LiM2BAAV'
);

---description:TestCase7 for Task
ASSERT
(
  SELECT COUNT(TaskId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Tasks`
  WHERE Priority = 'High' AND Status = 'Not Started'
)
=
(
  SELECT COUNT(TaskId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.tasks`
  WHERE Priority = 'High' AND Status = 'Not Started'
);
