---description:TestCase1 for Opportunity
ASSERT
(
  SELECT COUNT(OpportunityId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities`
  WHERE CreatedById = '0058a00000LiM2BAAV'
)
=
(
  SELECT COUNT(OpportunityId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.opportunities`
  WHERE CreatedById = '0058a00000LiM2BAAV'
);

---description:TestCase2 for Opportunity
ASSERT
(
  SELECT COUNT(OpportunityId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities`
  WHERE IsClosed IS FALSE)
=
(
  SELECT COUNT(OpportunityId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.opportunities`
  WHERE IsClosed IS FALSE
);

---description:TestCase3 for Opportunity
ASSERT
(
  SELECT SUM(Amount)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities`
  WHERE Probability = 10.0
    AND StageName = 'Prospecting'
)
=
(
  SELECT SUM(Amount)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.opportunities`
  WHERE Probability = 10.0
    AND StageName = 'Prospecting'
);

---description:TestCase4 for Opportunity
ASSERT
(
  SELECT SUM(Amount)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities`
  WHERE Name = 'Konklab'
)
=
(
  SELECT SUM(Amount)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.opportunities`
  WHERE Name = 'Konklab'
);

---description:TestCase5 for Opportunity
ASSERT
(
  SELECT COUNT(OpportunityId)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities`
  WHERE IsClosed IS FALSE
    AND Amount > 400000
)
=
(
  SELECT COUNT(OpportunityId)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.opportunities`
  WHERE IsClosed IS FALSE
    AND Amount > 400000
);

---description:TestCase6 for Opportunity
ASSERT
(
  SELECT OpportunityId
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities`
  WHERE CreatedById = '0058a00000LiM2BAAV'
    AND OwnerId = '0058a00000LiM2BAAV'
)
=
(
  SELECT OpportunityId
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.opportunities`
  WHERE CreatedById = '0058a00000LiM2BAAV'
    AND OwnerId = '0058a00000LiM2BAAV'
);

---description:TestCase7 for Opportunity
ASSERT
(
  SELECT SUM(Amount)
  FROM `{{ project_id_tgt }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities`
  WHERE StageName = 'Closed Lost'
    AND OwnerId = '0058a00000LvkJSAAZ'
)
=
(
  SELECT SUM(Amount)
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.opportunities`
  WHERE StageName = 'Closed Lost' AND OwnerId = '0058a00000LvkJSAAZ'
);
