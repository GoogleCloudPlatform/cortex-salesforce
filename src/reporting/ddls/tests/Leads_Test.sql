---description:TestCase1 for Lead
ASSERT
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Leads`
  WHERE Industry = 'Retail'
)
=
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.leads` AS Lead
  WHERE Lead.Industry = 'Retail'
);

---description:TestCase2 for Lead
ASSERT
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Leads`
  WHERE Isconverted IS TRUE
)
=
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.leads`
  WHERE Isconverted IS TRUE
);

---description:TestCase3 for Lead
ASSERT
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Leads`
  WHERE Status != 'Closed'
)
=
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.leads`
  WHERE Status != 'Closed'
);

---description:TestCase4 for Lead
ASSERT
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Leads`
  WHERE Leadsource = 'Web'
)
=
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.leads`
  WHERE Leadsource = 'Web'
);

---description:TestCase5 for Lead
ASSERT
(
  SELECT OwnerId
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Leads`
  WHERE LeadId = '00Q8a00001s4LMJEA2'
)
=
(
  SELECT OwnerId
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.leads`
  WHERE LeadId = '00Q8a00001s4LMJEA2'
);
