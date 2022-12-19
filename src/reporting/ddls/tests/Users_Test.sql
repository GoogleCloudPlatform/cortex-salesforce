---description:TestCase1 for User
ASSERT
(
  SELECT Country
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD`
  WHERE CompanyName = 'Google Cloud Solutions Engineering'
    AND CreatedByID = '0058a00000LGxQRAA1' AND UserId = '0058a00000LGxQeAAL'
)
=
(
  SELECT Country
  FROM `{{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.users`
  WHERE Companyname = 'Google Cloud Solutions Engineering'
    AND Createdbyid = '0058a00000LGxQRAA1' AND UserId = '0058a00000LGxQeAAL'
);

---description:TestCase2 for User
ASSERT
(
  SELECT COUNT(UserId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD`
  WHERE ManagerId != 'null'
    AND CreatedByID = '0058a00000LiM2DAAV'
)
=
(
  SELECT COUNT(UserId)
  FROM {{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.users
  WHERE ManagerId != 'null'
    AND CreatedById = '0058a00000LiM2DAAV'
);

---description:TestCase3 for User
ASSERT
(
  SELECT UserId
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD`
  WHERE LanguageLocaleKey = 'en_US'
    AND ProfileId = '00e8a000001jniUAAQ'
)
=
(
  SELECT UserId
  FROM {{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.users
  WHERE LanguageLocaleKey = 'en_US'
    AND ProfileId = '00e8a000001jniUAAQ'
);

---description:TestCase4 for User
ASSERT
(
  SELECT COUNT(UserId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD`
  WHERE CreatedById = '0058a00000LiM2DAAV'
)
=
(
  SELECT COUNT(UserId)
  FROM {{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.users
  WHERE CreatedById = '0058a00000LiM2DAAV'
);

---description:TestCase5 for User
ASSERT
(
  SELECT CONCAT(FirstName, '', LastName)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD`
  WHERE UserId = '0058a00000Ks33jAAB'
    AND CreatedById = '0058a00000Ks2ukAAB'
)
=
(
  SELECT CONCAT(FirstName, '', LastName)
  FROM {{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.users
  WHERE UserId = '0058a00000Ks33jAAB'
    AND CreatedById = '0058a00000Ks2ukAAB'
);

---description:TestCase6 for User
ASSERT
(
  SELECT Name
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD`
  WHERE UserRoleId = '00E8a000001cOSTEA2'
    AND UserId = '0058a00000LvkApAAJ'
)
=
(
  SELECT Name
  FROM {{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.users
  WHERE UserRoleId = '00E8a000001cOSTEA2'
    AND UserId = '0058a00000LvkApAAJ'
);

---description:TestCase7 for User
ASSERT
(
  SELECT COUNT(UserId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD`
  WHERE IsActive IS TRUE
    AND UserType = 'Standard'
)
=
(
  SELECT COUNT(UserId)
  FROM {{ project_id_src }}.{{ dataset_cdc_processed_sfdc }}.users
  WHERE IsActive IS TRUE
    AND UserType = 'Standard'
);
