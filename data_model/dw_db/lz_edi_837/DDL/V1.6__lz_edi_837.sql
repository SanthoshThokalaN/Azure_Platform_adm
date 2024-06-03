USE SCHEMA LZ_EDI_837;

CREATE transient TABLE provider_spike_raw_error(
  service_date                       VARCHAR(16777216), 
  subscriber_id                      VARCHAR(16777216), 
  providernpi                        VARCHAR(16777216), 
  npisource                          VARCHAR(16777216), 
  billprovidertaxid                  VARCHAR(16777216), 
  billproviderstates                 VARCHAR(16777216), 
  months                             VARCHAR(16777216), 
  years                              VARCHAR(16777216),
  claim_id                           VARCHAR(16777216), 
  line_item_charge_amt               VARCHAR(16777216),
  bill_line_num                      VARCHAR(16777216), 
  grp_control_no                     VARCHAR(16777216), 
  trancactset_cntl_no                VARCHAR(16777216),
  provider_hl_no                     VARCHAR(16777216), 
  subscriber_hl_no                   VARCHAR(16777216),
  payer_hl_no                        VARCHAR(16777216),
  transactset_create_date            VARCHAR(16777216)
  
  );


  CREATE transient TABLE taxonomy_mismatch_raw_error(
  ucps_clm_num               VARCHAR, 
  bill_line                  VARCHAR, 
  cpt_codes                  VARCHAR, 
  med_approved_amount        VARCHAR, 
  taxonomy_codes             VARCHAR, 
  provider_tax_id            VARCHAR, 
  provider_npi               VARCHAR, 
  adj_ben_amt                VARCHAR,
  claim_pd_date              VARCHAR);
