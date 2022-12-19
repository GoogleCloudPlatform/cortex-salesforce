---description:TestCase1 for Case
ASSERT
(
  SELECT AccountId
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Cases`
  WHERE CaseId = '5008a00001yAa4bAAC'
)
=
(
  SELECT AccountId
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.cases`
  WHERE CaseId = '5008a00001yAa4bAAC'
);

---description:TestCase2 for Case
ASSERT
(
  SELECT COUNT(CaseId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Cases`
  WHERE CreatedById = '0058a00000LiM2DAAV'
    AND Origin = 'Email'
)
=
(
  SELECT COUNT(CaseId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.cases`
  WHERE CreatedById = '0058a00000LiM2DAAV'
    AND Origin = 'Email'
);

---description:TestCase3 for Case
ASSERT
(
  SELECT COUNT(CaseNumber)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Cases`
  WHERE Status = 'Escalated'
)
=
(
  SELECT COUNT(CaseNumber)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.cases`
  WHERE Status = 'Escalated'
);

---description:TestCase4 for Case
ASSERT
(
  SELECT COUNT(CaseNumber)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Cases`
  WHERE Status != 'Closed'
)
=
(
  SELECT COUNT(CaseNumber)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.cases`
  WHERE Status != 'Closed'
);

---description:TestCase5 for Case
ASSERT
(
  SELECT COUNT(CaseNumber)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Cases`
  WHERE Status = 'Closed'
)
=
(
  SELECT COUNT(CaseNumber)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.cases`
  WHERE Status = 'Closed'
);

---description:TestCase6 for Case
ASSERT
(
  SELECT COUNT(CaseNumber)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Cases`
  WHERE Status = 'Closed'
    AND OwnerId = '0058a00000LvkK1AAJ'
)
=
(
  SELECT COUNT(CaseNumber)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.cases`
  WHERE Status = 'Closed'
    AND OwnerId = '0058a00000LvkK1AAJ'
);

---description:TestCase7 for Case
ASSERT
(
  SELECT COUNT(CaseNumber)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Cases`
  WHERE Status != 'Closed'
    AND Priority = 'High'
)
=
(
  SELECT COUNT(CaseNumber)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.cases`
  WHERE Status != 'Closed'
    AND Priority = 'High'
);
