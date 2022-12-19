---description:TestCase1 for CaseManagement
ASSERT
(
  SELECT COUNT(CaseId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.CaseManagement`
  WHERE EXTRACT(YEAR FROM CaseCreatedDatestamp) = 2022
)
=
(
  SELECT COUNT(CaseId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Cases`
  WHERE EXTRACT(YEAR FROM CreatedDatestamp) = 2022
);

---description:TestCase2 for CaseManagement
ASSERT
(
  SELECT
    COUNT(CaseId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.CaseManagement`
  WHERE EXTRACT(YEAR FROM CaseCreatedDatestamp) = 2022
    AND CaseIsClosed IS FALSE
    AND CasePriority = 'High'
)
=
(
  SELECT COUNT(CaseId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Cases`
  WHERE EXTRACT(YEAR FROM CreatedDatestamp) = 2022
    AND IsClosed IS FALSE
    AND Priority = 'High'
);

---description:TestCase3 for CaseManagement
ASSERT
(
  SELECT COUNT(CaseId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.CaseManagement`
  WHERE EXTRACT(YEAR FROM CaseCreatedDatestamp) = 2022
    AND CaseIsClosed IS FALSE
    AND CasePriority = 'Low'
)
=
(
  SELECT COUNT(CaseId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Cases`
  WHERE EXTRACT(YEAR FROM CreatedDatestamp) = 2022
    AND IsClosed IS FALSE
    AND Priority = 'Low'
);

---description:TestCase4 for CaseManagement
ASSERT
(
  SELECT Count(CaseId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.CaseManagement`
  WHERE CaseOrigin = 'Phone'
    AND EXTRACT(YEAR FROM CaseCreatedDatestamp) = 2022
)
=
(
  SELECT Count(CaseId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Cases`
  WHERE Origin = 'Phone'
    AND EXTRACT(YEAR FROM CreatedDatestamp) = 2022
);

---description:TestCase5 for CaseManagement
ASSERT
(
  SELECT CaseNumber
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.CaseManagement`
  WHERE EXTRACT(DATE FROM CaseCreatedDatestamp) = '2022-10-25'
    AND CaseType = 'Problem'
    AND CaseStatus = 'New'
    AND CaseIsClosed IS FALSE
    AND AccountId = '0018a00001zYsv9AAC'
)
=
(
  SELECT Cases.CaseNumber
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Cases` AS Cases
  INNER JOIN
    `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.AccountsMD` AS Account
    ON
      Cases.AccountId = Account.AccountId
  WHERE EXTRACT(DATE FROM Cases.CreatedDatestamp) = '2022-10-25'
    AND Cases.Type = 'Problem'
    AND Cases.Status = 'New'
    AND Cases.IsClosed IS FALSE
    AND Account.AccountId = '0018a00001zYsv9AAC'
);
