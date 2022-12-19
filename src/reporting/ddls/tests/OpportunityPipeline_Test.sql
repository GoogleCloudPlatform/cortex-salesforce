---description:TestCase1 for OpportunityPipeline
ASSERT
(
  SELECT SUM(TotalSaleAmount)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.OpportunityPipeline`
  WHERE EXTRACT(YEAR FROM OpportunityCreatedDatestamp) = 2022
    AND AccountIndustry = 'Retail'
)
=
(
  SELECT SUM(Opportunity.Amount)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities` AS Opportunity
  INNER JOIN
    `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.AccountsMD` AS Account
    ON
      Account.AccountId = Opportunity.AccountId
  WHERE EXTRACT(YEAR FROM Opportunity.CreatedDatestamp) = 2022
    AND Account.Industry = 'Retail'
);

---description:TestCase2 for OpportunityPipeline
ASSERT
(
  SELECT SUM(TotalSaleAmount)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.OpportunityPipeline`
  WHERE EXTRACT(YEAR FROM OpportunityCreatedDatestamp) = 2022
    AND OpportunityOwnerName = 'Michael Deeble'
    AND AccountBillingCountry = 'USA'
)
=
(
  SELECT SUM(Opportunity.Amount)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities` AS Opportunity
  INNER JOIN
    `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.AccountsMD` AS Account
    ON
      Account.AccountId = Opportunity.AccountId
  INNER JOIN
    `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD` AS User
    ON
      Opportunity.OwnerId = User.UserId
  WHERE EXTRACT(YEAR FROM Opportunity.CreatedDatestamp) = 2022
    AND User.Name = 'Michael Deeble'
    AND Account.BillingCountry = 'USA'
);

---description:TestCase3 for OpportunityPipeline
ASSERT
(
  SELECT SUM(TotalSaleAmount)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.OpportunityPipeline`
  WHERE EXTRACT(YEAR FROM OpportunityCreatedDatestamp) = 2022
    AND OpportunityStageName = 'Closed Won'
)
=
(
  SELECT SUM(Amount)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities` AS Opportunity
  WHERE EXTRACT(YEAR FROM CreatedDatestamp) = 2022
    AND Opportunity.StageName = 'Closed Won'
);

---description:TestCase4 for OpportunityPipeline
ASSERT
(
  SELECT SUM(TotalSaleAmount)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.OpportunityPipeline`
  WHERE EXTRACT(YEAR FROM OpportunityCreatedDatestamp) = 2022
    AND OpportunityStageName = 'Closed Lost'
)
=
(
  SELECT SUM(Amount)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities` AS Opportunity
  WHERE EXTRACT(YEAR FROM CreatedDatestamp) = 2022
    AND Opportunity.StageName = 'Closed Lost'
);

---description:TestCase5 for OpportunityPipeline
ASSERT
(
  SELECT Count(OpportunityId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.OpportunityPipeline`
  WHERE EXTRACT(YEAR FROM OpportunityCreatedDatestamp) = 2022
    AND AccountIndustry = 'Retail'
    AND AccountBillingCountry = 'USA'
    AND OpportunityOwnerName = 'Michael Deeble'
)
=
(
  SELECT Count(Opportunity.OpportunityId)
  FROM `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.Opportunities` AS Opportunity
  LEFT JOIN
    `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.AccountsMD` AS Account
    ON
      Account.AccountId = Opportunity.AccountId
  LEFT JOIN
    `{{ project_id_src }}.{{ dataset_reporting_tgt_sfdc }}.UsersMD` AS User
    ON
      Opportunity.OwnerId = User.UserId
  WHERE EXTRACT(YEAR FROM Opportunity.CreatedDatestamp) = 2022
    AND Account.Industry = 'Retail'
    AND Account.BillingCountry = 'USA'
    AND User.Name = 'Michael Deeble'
);
