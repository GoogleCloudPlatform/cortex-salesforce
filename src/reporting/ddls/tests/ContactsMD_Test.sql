---description:TestCase1 for Contact
ASSERT
(
  SELECT COUNT(ContactId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.ContactsMD`
  WHERE Leadsource = 'Web'
)
=
(
  SELECT COUNT(ContactId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.contacts`
  WHERE Leadsource = 'Web'
);

---description:TestCase2 for Contact
ASSERT
(
  SELECT COUNT(ContactID)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.ContactsMD`
  WHERE OwnerId = '0058a00000LvvtRAAR'
)
=
(
  SELECT COUNT(ContactId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.contacts`
  WHERE OwnerId = '0058a00000LvvtRAAR'
);

---description:TestCase3 for Contact
ASSERT
(
  SELECT Name
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.ContactsMD`
  WHERE ContactId = '0038a000030dV8HAAU'
)
=
(
  SELECT Name
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.contacts`
  WHERE ContactId = '0038a000030dV8HAAU'
);
