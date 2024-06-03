USE SCHEMA SRC_COMPAS;

DROP TABLE CLG_MSG;

CREATE TABLE IF NOT EXISTS CLG_MSG (
	MSG_DESC VARCHAR(16777216),
	INBOUND VARCHAR(16777216),
	MSG_CATEGORY_ID VARCHAR(16777216),
	MSG_TYPE_CREATED_BY VARCHAR(16777216),
	MSG_TYPE_CREATION_DATE VARCHAR(16777216),
	MSG_TYPE_LAST_MODIFIED_BY VARCHAR(16777216),
	MSG_TYPE_LAST_MODIFIED_DATE VARCHAR(16777216),
	MSG_LOG_ID VARCHAR(16777216),
	BODID VARCHAR(16777216),
	MSG_STATUS_ID VARCHAR(16777216),
	REFERENCE_ID VARCHAR(16777216),
	MSG_LOG_CREATE_DATE VARCHAR(16777216),
	MSG_LOG_CREATED_BY VARCHAR(16777216),
	MSG_LOG_CREATION_DATE VARCHAR(16777216),
	MSG_TEXT_ID VARCHAR(16777216),
	MSG_CLOB VARIANT,
	MSG_LOG_CREATION_DATE_DL VARCHAR(16777216),
	MSG_TYPE_ID VARCHAR(16777216),
	ISDC_CREATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	ISDC_UPDATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
);


CREATE OR REPLACE TABLE billing_jpmceftm(
  record_type varchar(1), 
  transaction_code varchar(2), 
  bank_routing_number VARCHAR(16777216), 
  dfi_account_number VARCHAR(16777216), 
  amount_billed float, 
  aarp_account_number VARCHAR(16777216), 
  aarp_association_code NUMBER(38,0), 
  individual_name_last_name VARCHAR(16777216), 
  individual_name_first_initial VARCHAR(16777216), 
  payment_type_code varchar(1), 
  discretionary_data varchar(1), 
  addenda_record_ind VARCHAR(16777216), 
  trace_number VARCHAR(16777216), 
  sequence_number VARCHAR(16777216), 
  process_date VARCHAR(16777216),
	ISDC_CREATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	ISDC_UPDATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
  
  );

CREATE OR REPLACE TABLE billing_jpmceftd(
  record_type varchar(1), 
  transaction_code varchar(2), 
  bank_routing_number VARCHAR(16777216), 
  dfi_account_number VARCHAR(16777216), 
  amount_billed float, 
  aarp_account_number VARCHAR(16777216), 
  aarp_association_code NUMBER(38,0), 
  individual_name_last_name VARCHAR(16777216), 
  individual_name_first_initial VARCHAR(16777216), 
  payment_type_code varchar(1), 
  discretionary_data varchar(1), 
  addenda_record_ind VARCHAR(16777216), 
  trace_number VARCHAR(16777216), 
  sequence_number VARCHAR(16777216), 
  process_date VARCHAR(16777216),
	ISDC_CREATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	ISDC_UPDATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
  
  );

CREATE OR REPLACE TABLE billing_dallas(
  batch_type VARCHAR(16777216), 
  process_julian_date VARCHAR(16777216), 
  batch_number VARCHAR(16777216), 
  sequence_number VARCHAR(16777216), 
  transaction_code NUMBER(38,0), 
  membership_number VARCHAR(16777216), 
  association_code NUMBER(38,0), 
  coupon_date VARCHAR(16777216), 
  type_indicator varchar(1), 
  check_number VARCHAR(16777216), 
  bank_account_routing_number VARCHAR(16777216), 
  employer_premium VARCHAR(16777216), 
  bank_account_number VARCHAR(16777216), 
  premium_amount FLOAT, 
  membership_dues FLOAT, 
  andrus_donation FLOAT, 
  total_dollar_amount FLOAT, 
  process_date VARCHAR(16777216),
	ISDC_CREATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	ISDC_UPDATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
  
  );

CREATE OR REPLACE TABLE billing_eftauth(
  process_date VARCHAR(16777216), 
  batch_number VARCHAR(16777216), 
  item_sequence_number VARCHAR(16777216), 
  bank_account_number VARCHAR(16777216), 
  bank_routing_number VARCHAR(16777216), 
  aarp_account_number VARCHAR(16777216), 
  aarp_association_code varchar(1), 
  bank_account_type varchar(1),
	ISDC_CREATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	ISDC_UPDATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
  
  );
  
 CREATE OR REPLACE TABLE billing_noc(
  process_date VARCHAR(16777216), 
  batch_number VARCHAR(16777216), 
  item_sequence_number VARCHAR(16777216), 
  new_bank_account_number VARCHAR(16777216), 
  new_bank_routing_number VARCHAR(16777216), 
  membership_number VARCHAR(16777216), 
  aarp_association_code VARCHAR(16777216), 
  new_bank_account_type VARCHAR(16777216), 
  noc_change_code VARCHAR(16777216), 
  original_bank_account_number VARCHAR(16777216), 
  original_bank_routing_number VARCHAR(16777216), 
  original_bank_account_type VARCHAR(16777216),
	ISDC_CREATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	ISDC_UPDATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
  
  );
  
  
  CREATE OR REPLACE TABLE billing_pitts(
  batch_type VARCHAR(16777216), 
  process_julian_date VARCHAR(16777216), 
  batch_number VARCHAR(16777216), 
  sequence_number VARCHAR(16777216), 
  transaction_code NUMBER(38,0), 
  membership_number VARCHAR(16777216), 
  association_code NUMBER(38,0), 
  coupon_date VARCHAR(16777216), 
  type_indicator varchar(1), 
  check_number VARCHAR(16777216), 
  bank_account_routing_number VARCHAR(16777216), 
  employer_premium VARCHAR(16777216), 
  bank_account_number VARCHAR(16777216), 
  premium_amount FLOAT, 
  membership_dues FLOAT, 
  andrus_donation FLOAT, 
  total_dollar_amount FLOAT,
  process_date VARCHAR(16777216),
	ISDC_CREATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	ISDC_UPDATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
  
  );
  
  CREATE OR REPLACE TABLE billing_prot(
  operator_code VARCHAR(16777216), 
  transaction_date VARCHAR(16777216), 
  time_batch VARCHAR(16777216), 
  transaction_code VARCHAR(16777216), 
  last_maintenance_date VARCHAR(16777216), 
  last_maintenance_clerk VARCHAR(16777216), 
  individual VARCHAR(16777216), 
  aarp_account_number VARCHAR(16777216), 
  aarp_association_code VARCHAR(16777216), 
  sub_trans_code VARCHAR(16777216), 
  check_date VARCHAR(16777216), 
  total_amount VARCHAR(16777216), 
  premium_paid VARCHAR(16777216), 
  reason_code VARCHAR(16777216),
	ISDC_CREATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	ISDC_UPDATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
  
  );
  
  
 CREATE OR REPLACE TABLE billing_thdparty(
  record_type_code varchar(1), 
  transaction_code varchar(2), 
  receiving_dfi_identification varchar(8), 
  check_digit NUMBER(38,0), 
  dfi_account_number VARCHAR(16777216), 
  dollar_amount float, 
  individual_identifcation_number VARCHAR(16777216), 
  individual_name VARCHAR(16777216), 
  discretionary_data VARCHAR(16777216), 
  addenda_record_indicator NUMBER(38,0), 
  trace_number VARCHAR(16777216), 
  process_date VARCHAR(16777216),
	ISDC_CREATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP(),
	ISDC_UPDATED_DT TIMESTAMP_NTZ(9) DEFAULT CURRENT_TIMESTAMP()
  
  );



