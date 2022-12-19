---description:TestCase1 for LeadCaptureConversions
ASSERT
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.LeadsCaptureConversions`
  WHERE EXTRACT(YEAR FROM LeadCreatedDatestamp) = 2022
    AND LeadIsConverted IS TRUE
)
=
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Leads`
  WHERE EXTRACT(YEAR FROM CreatedDatestamp) = 2022
    AND IsConverted IS TRUE
);

---description:TestCase2 for LeadCaptureConversions
ASSERT
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.LeadsCaptureConversions`
  WHERE EXTRACT(YEAR FROM LeadCreatedDatestamp) = 2022
    AND EXTRACT(MONTH FROM LeadCreatedDatestamp) = 1
    AND LeadStatus = 'Qualified'
)
=
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Leads`
  WHERE EXTRACT(YEAR FROM CreatedDatestamp) = 2022
    AND EXTRACT(MONTH FROM CreatedDatestamp) = 1
    AND Status = 'Qualified'
);

---description:TestCase3 for LeadCaptureConversions
ASSERT
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.LeadsCaptureConversions`
  WHERE EXTRACT(YEAR FROM LeadCreatedDatestamp) = 2022
    AND LeadSource = 'Web'
    AND EXTRACT(MONTH FROM LeadCreatedDatestamp) = 3
)
=
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Leads`
  WHERE EXTRACT(YEAR FROM CreatedDatestamp) = 2022
    AND LeadSource = 'Web'
    AND EXTRACT(MONTH FROM CreatedDatestamp) = 3
);

---description:TestCase4 for LeadCaptureConversions
ASSERT
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.LeadsCaptureConversions`
  WHERE EXTRACT(YEAR FROM LeadCreatedDatestamp) = 2022
    AND LeadIndustry = 'Agriculture'
)
=
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Leads`
  WHERE EXTRACT(YEAR FROM CreatedDatestamp) = 2022
    AND Industry = 'Agriculture'
);

---description:TestCase5 for LeadCaptureConversions
ASSERT
(
  SELECT COUNT(LeadId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.LeadsCaptureConversions`
  WHERE EXTRACT(YEAR FROM LeadCreatedDatestamp) = 2022
    AND LeadIsConverted IS TRUE
    AND LeadOwnerName = 'Michael Deeble'
)
=
(
  SELECT COUNT(Lead.LeadId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Leads` AS Lead
  INNER JOIN
    `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD` AS User
    ON
      Lead.OwnerId = User.UserId
  WHERE EXTRACT(YEAR FROM Lead.CreatedDatestamp) = 2022
    AND Lead.IsConverted IS TRUE
    AND User.Name = 'Michael Deeble'
);
