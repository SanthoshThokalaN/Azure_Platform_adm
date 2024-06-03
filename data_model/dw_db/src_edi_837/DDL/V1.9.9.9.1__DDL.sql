USE SCHEMA SRC_EDI_837;

create TABLE IF NOT EXISTS inst_claim_clmnbr (
  ucps_clm_num VARCHAR(16777216), 
  ucps_clm_dt VARCHAR(16777216), 
  clm_type VARCHAR(16777216), 
  ref_id_qlfr VARCHAR(16777216), 
  ref_id_value VARCHAR(16777216), 
  member_id VARCHAR(16777216), 
  creat_dt VARCHAR(16777216), 
  app_sender_code VARCHAR(16777216), 
  app_reciever_code VARCHAR(16777216), 
  grp_control_no NUMBER(38,0), 
  trancactset_cntl_no VARCHAR(16777216), 
  impl_convention_refer VARCHAR(16777216), 
  transactset_purpose_code VARCHAR(16777216), 
  batch_cntl_no VARCHAR(16777216), 
  transactset_create_time NUMBER(38,0), 
  transact_type_code VARCHAR(16777216), 
  claim_id VARCHAR(16777216), 
  total_claim_charge_amt float, 
  healthcareservice_location VARCHAR(16777216), 
  provider_accept_assign_code VARCHAR(16777216), 
  provider_benefit_auth_code VARCHAR(16777216), 
  provider_patinfo_release_auth_code VARCHAR(16777216), 
  date_time_qlfy VARCHAR(16777216), 
  date_time_frmt_qlfy VARCHAR(16777216), 
  statement_date VARCHAR(16777216), 
  admit_type_code VARCHAR(16777216), 
  admit_source_code VARCHAR(16777216), 
  patient_status_code VARCHAR(16777216), 
  health_care_code_info VARCHAR(16777216), 
  sv_reimbursement_rate float, 
  sv_hcpcs_payable_amt float, 
  sv_clm_payment_remark_code VARCHAR(16777216), 
  sl_seq_num NUMBER(38,0), 
  product_service_id VARCHAR(16777216), 
  product_service_id_qlfr VARCHAR(16777216), 
  line_item_charge_amt float, 
  measurement_unit VARCHAR(16777216), 
  service_unit_count float, 
  cas_adj_group_code VARCHAR(16777216), 
  cas_adj_reason_code VARCHAR(16777216), 
  cas_adj_amt float, 
  delay_reason_code VARCHAR(16777216), 
  line_item_denied_charge_amt float,  
  network_trace_number VARCHAR(16777216), 
  principal_procedure_info VARCHAR(16777216), 
  hc_condition_codes VARIANT, 
  clm_lab_facility_name VARCHAR(16777216), 
  clm_lab_facility_id VARCHAR(16777216), 
  clm_lab_facility_addr1 VARCHAR(16777216), 
  clm_lab_facility_addr2 VARCHAR(16777216), 
  clm_lab_facility_city VARCHAR(16777216), 
  clm_lab_facility_state VARCHAR(16777216), 
  clm_lab_facility_zip VARCHAR(16777216), 
  clm_lab_facility_ref_id_qlfr VARCHAR(16777216), 
  clm_lab_facility_ref_id VARCHAR(16777216), 
  medical_record_number VARCHAR(16777216), 
  clm_note_text VARCHAR(16777216), 
  clm_billing_note_text VARCHAR(16777216), 
  clm_admitting_diagnosis_cd VARCHAR(16777216), 
  patient_reason_for_visit_cd VARIANT, 
  external_cause_of_injury VARIANT, 
  diagnosis_related_grp_info VARCHAR(16777216), 
  other_diagnosis_cd_info VARIANT, 
  other_procedure_info VARIANT, 
  occurrence_span_info VARIANT, 
  occurrence_info VARIANT, 
  value_info VARIANT, 
  treatment_cd_info VARIANT, 
  other_payer_1_paid_amt float, 
  other_payer_2_paid_amt float, 
  drug_product_id_qlfr VARCHAR(16777216), 
  drug_product_id VARCHAR(16777216), 
  drug_unit_count float, 
  drug_measure_unit VARCHAR(16777216),
  transactset_create_date VARCHAR(16777216),
  ISDC_CREATED_DT	TIMESTAMP_NTZ(9),
  ISDC_UPDATED_DT	TIMESTAMP_NTZ(9));
