---description:TestCase1 for Account
ASSERT
(
  SELECT CreatedById
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.AccountsMD`
  WHERE AccountId = '0018a00001uzUljAAE'
)
=
(
  SELECT CreatedById
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.accounts`
  WHERE AccountId = '0018a00001uzUljAAE'
);

---description:TestCase2 for Account
ASSERT
(
  SELECT AccountId
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.AccountsMD`
  WHERE AccountSource = 'Advertisement'
    AND Name = 'Bluejam'
)
=
(
  SELECT AccountId
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.accounts`
  WHERE AccountSource = 'Advertisement'
    AND Name = 'Bluejam'
);

---description:TestCase3 for Account
ASSERT
(
  SELECT NumberOfEmployees
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.AccountsMD`
  WHERE AccountId = '0018a00001uzUlPAAU'
)
=
(
  SELECT Numberofemployees
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.accounts`
  WHERE AccountId = '0018a00001uzUlPAAU'
);

---description:TestCase4 for Account
ASSERT
(
  SELECT Name
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.AccountsMD`
  WHERE AccountId = '0018a00001uzUlcAAE'
)
=
(
  SELECT Name
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.accounts`
  WHERE AccountId = '0018a00001uzUlcAAE'
);

---description:TestCase5 for Account
ASSERT
(
  SELECT COUNT(AccountId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.AccountsMD`
  WHERE Industry = 'Retail'
)
=
(
  SELECT COUNT(AccountId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.accounts`
  WHERE Industry = 'Retail'
);
