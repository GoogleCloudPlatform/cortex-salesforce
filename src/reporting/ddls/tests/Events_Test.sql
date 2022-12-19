---description:TestCase1 for Event
ASSERT
(
  SELECT OwnerId
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Events`
  WHERE EventId = '00U8a00000neDfPEAU'
    AND ShowTimeAs = 'Busy'
)
=
(
  SELECT OwnerId
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.events`
  WHERE EventId = '00U8a00000neDfPEAU'
    AND ShowAs = 'Busy'
);

---description:TestCase2 for Event
ASSERT
(
  SELECT COUNT(EventId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Events`
  WHERE CreatedByID = '0058a00000LiM2BAAV'
)
=
(
  SELECT COUNT(EventId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.events`
  WHERE CreatedByID = '0058a00000LiM2BAAV'
);

---description:TestCase3 for Event
ASSERT
(
  SELECT COUNT(EventId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Events`
  WHERE Subject = 'Meeting'
)
=
(
  SELECT COUNT(EventId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.events`
  WHERE Subject = 'Meeting'
);

---description:TestCase4 for Event
ASSERT
(
  SELECT EventId
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Events`
  WHERE CreatedDatestamp = '2022-04-25 11:51:58 UTC'
    AND WhatId = '0068a00001I1M9eAAF'
)
=
(
  SELECT EventId
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.events`
  WHERE CreatedDatestamp = '2022-04-25 11:51:58 UTC'
    AND WhatId = '0068a00001I1M9eAAF'
);

---description:TestCase6 for Event
ASSERT
(
  SELECT COUNT(EventId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Events`
  WHERE WhoId = '00Q8a00001s4LMKEA2')
=
(
  SELECT COUNT(EventId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.events`
  WHERE WhoId = '00Q8a00001s4LMKEA2'
);

---description:TestCase7 for Event
ASSERT
(
  SELECT COUNT(EventId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Events`
  WHERE Subject = 'Email'
)
=
(
  SELECT COUNT(EventId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.events`
  WHERE Subject = 'Email'
);
