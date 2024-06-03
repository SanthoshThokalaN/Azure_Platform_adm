USE SCHEMA SRC_FOX;

CREATE OR REPLACE PROCEDURE "SP_DUPLICATE_CLAIMS_REPORT_MATCHING"("PIPELINE_ID" VARCHAR(16777216), "PIPELINE_NAME" VARCHAR(16777216), "DB_NAME" VARCHAR(16777216), "UTIL_SC" VARCHAR(16777216), "TGT_SC" VARCHAR(16777216), "SRC_EDI_837_SC" VARCHAR(16777216), "LZ_FOX_SC" VARCHAR(16777216), "WH" VARCHAR(16777216), "CURR_DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE

V_CURRENT_DATE   DATE := COALESCE(TO_DATE(:CURR_DATE), CURRENT_DATE());

V_SP_PROCESS_RUN_LOGS_DTL  VARCHAR := :DB_NAME||''.''||COALESCE(:UTIL_SC, ''UTIL'')||''.SP_PROCESS_RUN_LOGS_DTL'';

V_PROCESS_NAME      VARCHAR    DEFAULT ''DUPLICATE_CLAIMS'';

V_SUB_PROCESS_NAME  VARCHAR    DEFAULT ''DUPLICATE_CLAIMS_REPORT_MATCHING'';

V_STEP             VARCHAR;

V_STEP_NAME        VARCHAR;

V_START_TIME       VARCHAR;

V_END_TIME         VARCHAR;

V_ROWS_PARSED       INTEGER;

V_ROWS_LOADED       INTEGER;

V_MESSAGE          VARCHAR;

V_LAST_QUERY_ID    VARCHAR;

V_STAGE_QUERY      VARCHAR;

V_DUPLICATE_CLAIMS_REPORT_INTERMEDIATE VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_FOX_SC,''LZ_FOX'') || ''.DUPLICATE_CLAIMS_REPORT_INTERMEDIATE'';
V_DUPLICATE_CLAIMS_REPORT_MATCHING     VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_FOX_SC,''LZ_FOX'') || ''.DUPLICATE_CLAIMS_REPORT_MATCHING'';
V_DUPLICATE_CLAIMS_REPORT_TEMP         VARCHAR     := :DB_NAME || ''.'' || COALESCE(:LZ_FOX_SC,''LZ_FOX'') || ''.DUPLICATE_CLAIMS_REPORT_TEMP'';     

V_PROF_PROVIDER_ALL          VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_PROVIDER_ALL'';
V_PROF_PROVIDER              VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_PROVIDER'';
V_PROF_SUBSCRIBER           VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_SUBSCRIBER'';
V_PROF_CLAIM_PART            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.PROF_CLAIM_PART'';


V_INST_SUBSCRIBER            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_SUBSCRIBER'';
V_INST_PROVIDER              VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_PROVIDER'';
V_INST_CLAIM_PART            VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_CLAIM_PART'';
V_INST_PROVIDER_ALL              VARCHAR     := :DB_NAME || ''.'' || COALESCE(:SRC_EDI_837_SC,''SRC_EDI_837'') || ''.INST_PROVIDER_ALL'';


BEGIN

EXECUTE IMMEDIATE ''USE WAREHOUSE ''||:WH; 

ALTER SESSION SET TIMEZONE = ''America/Chicago'';

V_STEP := ''STEP0'';
 
V_STEP_NAME := ''Truncate DUPLICATE_CLAIMS_REPORT_INTERMEDIATE and DUPLICATE_CLAIMS_REPORT_MATCHING''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());

TRUNCATE TABLE IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_INTERMEDIATE);
TRUNCATE TABLE IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_MATCHING);

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


V_STEP := ''STEP1'';
 
V_STEP_NAME := ''Load Professional DUPLICATE_CLAIMS_REPORT_INTERMEDIATE - 1''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   

INSERT INTO  IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_INTERMEDIATE)  

SELECT DISTINCT
    db2.clm_num,
    db2.ben_amt,
    db2.pay_adj_amt,
    CASE WHEN db2.pl_of_srvc=''null'' then split(clm.healthcareservice_location,'':'')[0] else db2.pl_of_srvc END as pl_of_srvc,
    db2.srv_from_dt,
    db2.srv_to_dt,
    db2.cpt_cd,
    db2.proc_mod1,
    db2.proc_mod2,
    db2.proc_mod3,
    db2.rend_prv_npi,
    db2.bill_prv_npi,
    db2.chrg_amt,
    db2.partb_ded_amt,
    db2.coins_amt,
    prvall.app_sender_code,
    clm.total_claim_charge_amt,
    db2.inst_prof_ind,
    sub.subscriber_id,
    db2.medcr_clm_ctl_nbr,
    db2.clh_trk_id,
    db2.doc_ctl_nbr,
    db2.bil_ln_num
FROM
    (SELECT  DISTINCT
        clm_num,
        ben_amt,
        pay_adj_amt,
        pl_of_srvc,
        srv_from_dt,
        srv_to_dt,
        cpt_cd,
        proc_mod1,
        proc_mod2,
        proc_mod3,
        rend_prv_npi,
        bill_prv_npi,
        chrg_amt,
        partb_ded_amt,
        coins_amt,
        inst_prof_ind,
        medcr_clm_ctl_nbr,
        clh_trk_id,
        bil_ln_num,
        doc_ctl_nbr
        FROM IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_TEMP)
        WHERE inst_prof_ind = ''P''
    ) db2
JOIN
    (SELECT  DISTINCT
    	app_sender_code,grp_control_no,
        trancactset_cntl_no,
        subscriber_hl_no,
        provider_hl_no,
        transactset_create_date,
		GET(element.value,''prv_id''):: varchar AS rend_prv_npi,
        sl_seq_num,xml_md5
    FROM IDENTIFIER(:v_prof_provider_all) prof_p_all,
	LATERAL FLATTEN(to_array(prof_p_all.clm_rendering_prv_map)) element
    WHERE app_sender_code IN (''APTIX'', ''EXELA'')
	    AND prof_p_all.transactset_create_date > TO_VARCHAR((current_date()-3)::date,''YYYYMMDD'')
	    AND prof_p_all.transactset_create_date <= TO_VARCHAR(current_date()::date,''YYYYMMDD'')
	) prvall
	ON db2.REND_PRV_NPI = prvall.rend_prv_npi
   
    
JOIN
    (SELECT  DISTINCT
    	grp_control_no,
		trancactset_cntl_no,
		subscriber_hl_no,
		transactset_create_date,
		subscriber_id,xml_md5
	FROM IDENTIFIER(:v_prof_subscriber)
	WHERE app_sender_code IN (''APTIX'', ''EXELA'')
		    AND transactset_create_date > TO_VARCHAR((current_date()-3)::date,''YYYYMMDD'')
	    AND transactset_create_date <= TO_VARCHAR(current_date()::date,''YYYYMMDD'')
	) sub
    ON prvall.grp_control_no = sub.grp_control_no
    AND prvall.trancactset_cntl_no = sub.trancactset_cntl_no
    AND prvall.subscriber_hl_no = sub.subscriber_hl_no
    AND prvall.transactset_create_date = sub.transactset_create_date
	AND prvall.xml_md5=sub.xml_md5
JOIN IDENTIFIER(:v_prof_claim_part) clm
    ON clm.app_sender_code IN (''APTIX'', ''EXELA'')
	AND prvall.grp_control_no = clm.grp_control_no
	AND prvall.trancactset_cntl_no = clm.trancactset_cntl_no
	AND prvall.provider_hl_no = clm.provider_hl_no
	AND prvall.transactset_create_date = clm.transactset_create_date
	AND db2.srv_from_dt = to_varchar(TO_DATE(SUBSTRING(clm.service_date, 1, 8),''YYYYMMDD''),''YYYY-MM-DD 00:00:00.0000'')
	AND ((db2.medcr_clm_ctl_nbr = clm.payer_clm_ctrl_num  and clm.app_sender_code =''EXELA'')
	    OR (clm.network_trace_number = db2.clh_trk_id AND db2.clh_trk_id NOT LIKE ''null'' AND db2.clh_trk_id  IS NOT NULL )
        OR (db2.doc_ctl_nbr = trim(substr(split(clm.clm_billing_note_text,'' '')[1],3,12)) AND (db2.doc_ctl_nbr NOT LIKE ''null'' AND db2.doc_ctl_nbr IS NOT NULL)))
    AND db2.bil_ln_num=clm.sl_seq_num
	   AND clm.transactset_create_date > TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
	    AND clm.transactset_create_date <= TO_VARCHAR(:V_CURRENT_DATE::date,''YYYYMMDD'')
		AND prvall.xml_md5=clm.xml_md5;



V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);

V_STEP := ''STEP2'';
 
V_STEP_NAME := ''Load Professional DUPLICATE_CLAIMS_REPORT_MATCHING - 1''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   
INSERT INTO  IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_MATCHING)  

SELECT DISTINCT
    db2.clm_num,
	db2.ben_amt,
	db2.pay_adj_amt,
	db2.pl_of_srvc,
	db2.srv_from_dt,
	db2.srv_to_dt,
	db2.cpt_cd,
	db2.proc_mod1,
	db2.proc_mod2,
	db2.proc_mod3,
	db2.rend_prv_npi,
	db2.bill_prv_npi,
	sub.subscriber_id,
	db2.chrg_amt,
	db2.partb_ded_amt,
	db2.coins_amt,
	clm.payer_clm_ctrl_num,
	db2.clh_trk_id,
	clm.transactset_create_date,
	db2.bil_ln_num
FROM
    (SELECT  DISTINCT
        clm_num,
        ben_amt,
        pay_adj_amt,
        pl_of_srvc,
        srv_from_dt,
        srv_to_dt,
        cpt_cd,
        proc_mod1,
        proc_mod2,
        proc_mod3,
        rend_prv_npi,
        bill_prv_npi,
        chrg_amt,
        partb_ded_amt,
        coins_amt,
        inst_prof_ind,
        total_claim_charge_amt,
        subscriber_id,
        medcr_clm_ctl_nbr,
        app_sender_code,
        clh_trk_id,
        bil_ln_num,
        doc_ctl_nbr
      FROM IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_INTERMEDIATE) where INST_PROF_IND = ''P'') db2
JOIN
    (SELECT  DISTINCT
        grp_control_no,xml_md5,
        trancactset_cntl_no,
        provider_hl_no,
        subscriber_hl_no,
        transactset_create_date,
		GET(element.value,''prv_id''):: varchar AS rend_prv_npi,
        app_sender_code,
        sl_seq_num
    FROM IDENTIFIER(:v_prof_provider_all) prof_p_all,
	LATERAL FLATTEN(to_array(prof_p_all.clm_rendering_prv_map)) element
    WHERE transactset_create_date >= TO_VARCHAR(DATEADD(YEAR, -1, :V_CURRENT_DATE)::date,''YYYYMMDD'')
        AND transactset_create_date < TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
    ) prvall
    ON db2.rend_prv_npi = prvall.rend_prv_npi
JOIN
    (SELECT  DISTINCT
    grp_control_no, xml_md5,
    trancactset_cntl_no,
    subscriber_hl_no,
    transactset_create_date,
    subscriber_id
    FROM IDENTIFIER(:v_prof_subscriber)
    WHERE transactset_create_date >= TO_VARCHAR(DATEADD(YEAR, -1, :V_CURRENT_DATE)::date,''YYYYMMDD'')
        AND transactset_create_date < TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
    ) sub
    ON prvall.grp_control_no = sub.grp_control_no
    AND prvall.trancactset_cntl_no = sub.trancactset_cntl_no
    AND prvall.subscriber_hl_no = sub.subscriber_hl_no
    AND prvall.transactset_create_date = sub.transactset_create_date
    AND sub.subscriber_id = db2.subscriber_id
	AND prvall.xml_md5=sub.xml_md5
JOIN
    IDENTIFIER(:v_prof_claim_part) clm
    ON prvall.grp_control_no = clm.grp_control_no
    AND prvall.trancactset_cntl_no = clm.trancactset_cntl_no
    AND prvall.provider_hl_no = clm.provider_hl_no
    AND prvall.transactset_create_date = clm.transactset_create_date
    AND db2.srv_from_dt = to_varchar(TO_DATE(SUBSTRING(clm.service_date, 1, 8),''YYYYMMDD''),''YYYY-MM-DD 00:00:00.0000'')
    AND db2.total_claim_charge_amt = clm.total_claim_charge_amt
    AND (db2.medcr_clm_ctl_nbr = clm.payer_clm_ctrl_num
        OR (clm.network_trace_number = db2.clh_trk_id AND db2.clh_trk_id NOT LIKE ''null'' AND db2.clh_trk_id  IS NOT NULL )
        OR (db2.doc_ctl_nbr = trim(substr(split(clm.clm_billing_note_text,'' '')[1],3,12)) AND (db2.doc_ctl_nbr NOT LIKE ''null'' AND db2.doc_ctl_nbr IS NOT NULL)))
    AND db2.bil_ln_num=clm.sl_seq_num
    AND db2.pl_of_srvc = split(clm.healthcareservice_location,'':'')[0]
    AND clm.transactset_create_date >= TO_VARCHAR(DATEADD(YEAR, -1, :V_CURRENT_DATE)::date,''YYYYMMDD'')
        AND clm.transactset_create_date < TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
		AND prvall.xml_md5=clm.xml_md5;


V_ROWS_LOADED := SQLROWCOUNT ;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);                         


V_STEP := ''STEP3'';
 
V_STEP_NAME := ''Load Professional DUPLICATE_CLAIMS_REPORT_INTERMEDIATE - 2''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   



INSERT INTO IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_INTERMEDIATE)  

SELECT DISTINCT
    db2.clm_num,
	db2.ben_amt,
	db2.pay_adj_amt,
	CASE WHEN db2.pl_of_srvc=''null'' then split(clm.healthcareservice_location,'':'')[0] else db2.pl_of_srvc END,
	db2.srv_from_dt,
	db2.srv_to_dt,
	db2.cpt_cd,
	db2.proc_mod1,
	db2.proc_mod2,
	db2.proc_mod3,
	db2.rend_prv_npi,
	db2.bill_prv_npi,
	db2.chrg_amt,
	db2.partb_ded_amt,
	db2.coins_amt,
	prv.app_sender_code,
	clm.total_claim_charge_amt,
	db2.inst_prof_ind,
	sub.subscriber_id,
	db2.medcr_clm_ctl_nbr,
	db2.clh_trk_id,
	db2.doc_ctl_nbr,
	db2.bil_ln_num
FROM
	(SELECT  DISTINCT
    	clm_num,
		ben_amt,
		pay_adj_amt,
		pl_of_srvc,
		srv_from_dt,
		srv_to_dt,
		cpt_cd,
		proc_mod1,
		proc_mod2,
		proc_mod3,
		rend_prv_npi,
		bill_prv_npi,
		chrg_amt,
		partb_ded_amt,
		coins_amt,
		inst_prof_ind,
		medcr_clm_ctl_nbr,
		clh_trk_id,
		bil_ln_num,
		doc_ctl_nbr
	FROM IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_TEMP)
	WHERE pl_of_srvc NOT IN (''19'', ''21'', ''22'', ''23'', ''31'', ''74'')
		AND INST_PROF_IND = ''P''
	) db2

JOIN
    (SELECT DISTINCT
        grp_control_no, xml_md5,
        app_sender_code,
        trancactset_cntl_no,
        subscriber_hl_no,
        transactset_create_date,
        sl_seq_num,
        provider_hl_no,
        service_date,
        payer_clm_ctrl_num,
        line_item_charge_amt,
        network_trace_number,
        clm_billing_note_text,
        total_claim_charge_amt,
        healthcareservice_location
     FROM IDENTIFIER(:v_prof_claim_part)
     WHERE app_sender_code IN (''APTIX'',''EXELA'')
        AND transactset_create_date > TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
	    AND transactset_create_date <= TO_VARCHAR(:V_CURRENT_DATE::date,''YYYYMMDD'')
     ) clm
    ON db2.srv_from_dt = to_varchar(TO_DATE(SUBSTRING(clm.service_date, 1, 8),''YYYYMMDD''),''YYYY-MM-DD 00:00:00.0000'')
    AND ((db2.medcr_clm_ctl_nbr = clm.payer_clm_ctrl_num  and clm.app_sender_code =''EXELA'')
            OR (clm.network_trace_number = db2.clh_trk_id AND db2.clh_trk_id NOT LIKE ''null'' AND db2.clh_trk_id  IS NOT NULL )
            OR (db2.doc_ctl_nbr = trim(substr(split(clm.clm_billing_note_text,'' '')[1],3,12)) and (db2.doc_ctl_nbr NOT LIKE ''null'' AND db2.doc_ctl_nbr IS NOT NULL)))
    AND db2.bil_ln_num=clm.sl_seq_num
JOIN
    (SELECT  DISTINCT
        grp_control_no,xml_md5,
        trancactset_cntl_no,
        transactset_create_date,
        provider_id,provider_hl_no,
        app_sender_code
    FROM IDENTIFIER(:v_prof_provider)
    WHERE app_sender_code IN (''APTIX'',''EXELA'')
        AND transactset_create_date > TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
	    AND transactset_create_date <= TO_VARCHAR(:V_CURRENT_DATE::date,''YYYYMMDD'')
    ) prv
    ON prv.provider_hl_no = clm.provider_hl_no
    AND prv.grp_control_no = clm.grp_control_no
    AND db2.bill_prv_npi=prv.provider_id
    AND prv.trancactset_cntl_no = clm.trancactset_cntl_no
    AND  prv.transactset_create_date = clm.transactset_create_date
	AND prv.xml_md5=clm.xml_md5

JOIN
    (SELECT DISTINCT
        grp_control_no,xml_md5,
        trancactset_cntl_no,
        subscriber_hl_no,
        transactset_create_date,
        subscriber_id
     FROM IDENTIFIER(:v_prof_subscriber)
     WHERE app_sender_code IN (''APTIX'',''EXELA'')
         AND transactset_create_date > TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
	    AND transactset_create_date <= TO_VARCHAR(:V_CURRENT_DATE::date,''YYYYMMDD'')
     ) sub
    ON prv.grp_control_no = sub.grp_control_no
    AND sub.trancactset_cntl_no = prv.trancactset_cntl_no
    AND sub.transactset_create_date = prv.transactset_create_date
    AND sub.subscriber_hl_no = clm.subscriber_hl_no
	AND sub.xml_md5 = clm.xml_md5
    ;


V_ROWS_LOADED := SQLROWCOUNT;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);

V_STEP := ''STEP4'';
 
V_STEP_NAME := ''Load Professional DUPLICATE_CLAIMS_REPORT_MATCHING - 2''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   


INSERT INTO  IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_MATCHING) 

SELECT DISTINCT
    db2.clm_num,
	db2.ben_amt,
	db2.pay_adj_amt,
	db2.pl_of_srvc,
	db2.srv_from_dt,
	db2.srv_to_dt,
	db2.cpt_cd,
	db2.proc_mod1,
	db2.proc_mod2,
	db2.proc_mod3,
	db2.rend_prv_npi,
	db2.bill_prv_npi,
	sub.subscriber_id,
	db2.chrg_amt,
	db2.partb_ded_amt,
	db2.coins_amt,
	clm.payer_clm_ctrl_num,
	db2.clh_trk_id,
	clm.transactset_create_date,
	db2.bil_ln_num
FROM
	(SELECT  DISTINCT        clm_num,
		ben_amt,
		pay_adj_amt,
		pl_of_srvc,
		srv_from_dt,
		srv_to_dt,
		cpt_cd,
		proc_mod1,
		proc_mod2,
		proc_mod3,
		rend_prv_npi,
		bill_prv_npi,
		chrg_amt,
		PARTB_DED_AMT,
		COINS_AMT,
		INST_PROF_IND,
		total_claim_charge_amt,
		subscriber_id,
		medcr_clm_ctl_nbr,
		app_sender_code,
		clh_trk_id,
		bil_ln_num,
		doc_ctl_nbr
	FROM IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_INTERMEDIATE)
	WHERE inst_prof_ind = ''P''
		AND pl_of_srvc NOT IN (''19'', ''21'', ''22'', ''23'', ''31'', ''74'')
	) db2

JOIN
    (SELECT 
    	DISTINCT
        grp_control_no,xml_md5,
        trancactset_cntl_no,
        provider_hl_no,
        transactset_create_date,
        provider_id
     FROM IDENTIFIER(:v_prof_provider)
     WHERE transactset_create_date >= TO_VARCHAR(DATEADD(YEAR, -1, :V_CURRENT_DATE)::date,''YYYYMMDD'')
        AND transactset_create_date < TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
     ) prv
    ON db2.bill_prv_npi=prv.provider_id

JOIN
    (SELECT  DISTINCT
        grp_control_no,xml_md5,
        trancactset_cntl_no,
        subscriber_hl_no,
        transactset_create_date,
        subscriber_id
     FROM IDENTIFIER(:v_prof_subscriber)
     WHERE transactset_create_date >= TO_VARCHAR(DATEADD(YEAR, -1, :V_CURRENT_DATE)::date,''YYYYMMDD'')
        AND transactset_create_date < TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
     ) sub
    ON sub.grp_control_no = prv.grp_control_no
    AND sub.trancactset_cntl_no = prv.trancactset_cntl_no
    AND sub.transactset_create_date = prv.transactset_create_date
    AND sub.subscriber_id = db2.subscriber_id
	AND sub.xml_md5= prv.xml_md5

JOIN IDENTIFIER(:v_prof_claim_part) clm
    ON prv.grp_control_no = clm.grp_control_no
    AND prv.trancactset_cntl_no = clm.trancactset_cntl_no
    AND prv.transactset_create_date = clm.transactset_create_date
    AND db2.srv_from_dt = to_varchar(TO_DATE(SUBSTRING(clm.service_date, 1, 8),''YYYYMMDD''),''YYYY-MM-DD 00:00:00.0000'')
    AND db2.total_claim_charge_amt = clm.total_claim_charge_amt
    AND (db2.medcr_clm_ctl_nbr = clm.payer_clm_ctrl_num
        OR (clm.network_trace_number = db2.clh_trk_id AND db2.clh_trk_id NOT LIKE ''null'' AND db2.clh_trk_id  IS NOT NULL )
        OR (db2.doc_ctl_nbr = trim(substr(split(clm.clm_billing_note_text,'' '')[1],3,12)) and (db2.doc_ctl_nbr NOT LIKE ''null'' AND db2.doc_ctl_nbr IS NOT NULL)))
    AND db2.bil_ln_num=clm.sl_seq_num
    AND db2.pl_of_srvc = split(clm.healthcareservice_location,'':'')[0]
    AND clm.transactset_create_date >= TO_VARCHAR(DATEADD(YEAR, -1, :V_CURRENT_DATE)::date,''YYYYMMDD'')
        AND clm.transactset_create_date < TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
	AND prv.xml_md5=clm.xml_md5;


V_ROWS_LOADED := SQLROWCOUNT;

V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 
                                 
V_STEP := ''STEP5'';
   
 
V_STEP_NAME := ''Load Institutional DUPLICATE_CLAIMS_REPORT_INTERMEDIATE - 1''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   


INSERT INTO  IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_INTERMEDIATE)  

SELECT
	db2.clm_num,
	db2.ben_amt,
	db2.pay_adj_amt,
	CASE WHEN db2.pl_of_srvc=''null'' then split(clm.healthcareservice_location,'':'')[0] else db2.pl_of_srvc END as pl_of_srvc,
	db2.srv_from_dt,
	db2.srv_to_dt,
	db2.cpt_cd,
	db2.proc_mod1,
	db2.proc_mod2,
	db2.proc_mod3,
	db2.rend_prv_npi,
	db2.bill_prv_npi,
	db2.chrg_amt,
	db2.partb_ded_amt,
	db2.coins_amt,
	prvall.app_sender_code,
	cast(clm.total_claim_charge_amt as string) as total_claim_charge_amt,
	db2.inst_prof_ind,
	sub.subscriber_id,
	db2.medcr_clm_ctl_nbr,
	db2.clh_trk_id,
	db2.doc_ctl_nbr,
	db2.bil_ln_num
FROM
	(SELECT 
		DISTINCT
        clm_num,
		ben_amt,
		pay_adj_amt,
		pl_of_srvc,
		srv_from_dt,
		srv_to_dt,
		cpt_cd,
		proc_mod1,
		proc_mod2,
		proc_mod3,
		rend_prv_npi,
		bill_prv_npi,
		chrg_amt,
		partb_ded_amt,
		coins_amt,
		inst_prof_ind,
		medcr_clm_ctl_nbr,
		clh_trk_id,
		bil_ln_num,
		doc_ctl_nbr
	FROM IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_TEMP)
	WHERE inst_prof_ind = ''I'' ) db2

JOIN
    (SELECT  DISTINCT
        app_sender_code,xml_md5,
		grp_control_no,
		trancactset_cntl_no,
		transactset_create_date,
		GET(element.value,''renderingPrvId''):: varchar AS rend_prv_npi,
		sv_lx_number
     FROM IDENTIFIER(:v_inst_provider_all) inst_p_all,
	LATERAL FLATTEN(to_array(inst_p_all.clm_rendering_prv_map)) element
    WHERE app_sender_code IN (''APTIX'', ''EXELA'')
	    AND inst_p_all.transactset_create_date > TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
	    AND inst_p_all.transactset_create_date <= TO_VARCHAR(:V_CURRENT_DATE::date,''YYYYMMDD'')) prvall
    ON db2.REND_PRV_NPI = prvall.rend_prv_npi

JOIN
    (SELECT 
		DISTINCT
        grp_control_no,xml_md5,
        trancactset_cntl_no,
        transactset_create_date,
        subscriber_id
      FROM IDENTIFIER(:v_inst_subscriber)
      WHERE transactset_create_date > TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
	    AND transactset_create_date <= TO_VARCHAR(:V_CURRENT_DATE::date,''YYYYMMDD'')
     ) sub
    ON prvall.grp_control_no = sub.grp_control_no
    AND prvall.trancactset_cntl_no = sub.trancactset_cntl_no
    AND prvall.transactset_create_date = sub.transactset_create_date
	AND prvall.xml_md5=sub.xml_md5

JOIN
    IDENTIFIER(:v_inst_claim_part) clm
    ON prvall.grp_control_no = clm.grp_control_no
    AND prvall.trancactset_cntl_no = clm.trancactset_cntl_no
    AND prvall.transactset_create_date = clm.transactset_create_date
    AND ((db2.medcr_clm_ctl_nbr = clm.payer_clm_ctrl_num  and clm.app_sender_code =''EXELA'') 
        OR (clm.network_trace_number = db2.clh_trk_id AND db2.clh_trk_id NOT LIKE ''null'' AND db2.clh_trk_id  IS NOT NULL )
        OR (db2.doc_ctl_nbr = trim(substr(split(clm.clm_billing_note_text,'' '')[1],3,12)) and (db2.doc_ctl_nbr NOT LIKE ''null'' AND db2.doc_ctl_nbr IS NOT NULL)))
    AND db2.bil_ln_num=clm.sl_seq_num
    AND clm.transactset_create_date > TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
	    AND clm.transactset_create_date <= TO_VARCHAR(:V_CURRENT_DATE::date,''YYYYMMDD'')
		AND prvall.xml_md5=clm.xml_md5;





V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


V_STEP := ''STEP6'';
   
 
V_STEP_NAME := ''Load Institutional DUPLICATE_CLAIMS_REPORT_MATCHING - 1''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   


INSERT INTO  IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_MATCHING)  

SELECT DISTINCT
    db2.clm_num,
    db2.ben_amt,
    db2.pay_adj_amt,
    db2.pl_of_srvc,
    db2.srv_from_dt,
    db2.srv_to_dt,
    db2.cpt_cd,
    db2.proc_mod1,
    db2.proc_mod2,
    db2.proc_mod3,
    db2.rend_prv_npi,
    db2.bill_prv_npi,
    sub.subscriber_id,
    db2.chrg_amt,
    db2.partb_ded_amt,
    db2.coins_amt,
    clm.payer_clm_ctrl_num,
    db2.clh_trk_id,
    clm.transactset_create_date,
    db2.bil_ln_num
FROM
    (SELECT 
        DISTINCT
        clm_num,
        ben_amt,
        pay_adj_amt,
        pl_of_srvc,
        srv_from_dt,
        srv_to_dt,
        cpt_cd,
        proc_mod1,
        proc_mod2,
        proc_mod3,
        rend_prv_npi,
        bill_prv_npi,
        chrg_amt,
        partb_ded_amt,
        coins_amt,
        inst_prof_ind,
        total_claim_charge_amt,
        subscriber_id,
        medcr_clm_ctl_nbr,
        app_sender_code,
        clh_trk_id,
        bil_ln_num,
        doc_ctl_nbr
     FROM IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_INTERMEDIATE) WHERE inst_prof_ind = ''I''
    ) db2

JOIN
    (SELECT 
		DISTINCT
        grp_control_no,xml_md5,
		trancactset_cntl_no,
		transactset_create_date,
		GET(element.value,''renderingPrvId''):: varchar AS rend_prv_npi,
		app_sender_code,
		sv_lx_number
     FROM IDENTIFIER(:v_inst_provider_all) inst_p_all,
	LATERAL FLATTEN(to_array(inst_p_all.clm_rendering_prv_map)) element
    WHERE
	    inst_p_all.transactset_create_date >= TO_VARCHAR(DATEADD(YEAR, -1, :V_CURRENT_DATE)::date,''YYYYMMDD'')
	    AND inst_p_all.transactset_create_date < TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
	) prvall
    ON db2.rend_prv_npi = prvall.rend_prv_npi

JOIN
    (SELECT 
        DISTINCT
        grp_control_no,xml_md5,
        trancactset_cntl_no,
        transactset_create_date,
        subscriber_id
      FROM IDENTIFIER(:v_inst_subscriber)
      WHERE transactset_create_date >= TO_VARCHAR(DATEADD(YEAR, -1, :V_CURRENT_DATE)::date,''YYYYMMDD'')
	    AND transactset_create_date < TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
    ) sub
    ON prvall.grp_control_no = sub.grp_control_no
    AND prvall.trancactset_cntl_no = sub.trancactset_cntl_no
    AND prvall.transactset_create_date = sub.transactset_create_date
    AND sub.subscriber_id = db2.subscriber_id
	AND prvall.xml_md5 = sub.xml_md5

JOIN IDENTIFIER(:v_inst_claim_part) clm
    ON prvall.grp_control_no = clm.grp_control_no
    AND prvall.trancactset_cntl_no = clm.trancactset_cntl_no
    AND prvall.transactset_create_date = clm.transactset_create_date
    AND db2.total_claim_charge_amt = clm.total_claim_charge_amt
    AND (db2.medcr_clm_ctl_nbr = clm.payer_clm_ctrl_num
        OR (clm.network_trace_number = db2.clh_trk_id AND db2.clh_trk_id NOT LIKE ''null'' AND db2.clh_trk_id  IS NOT NULL )
        OR (db2.doc_ctl_nbr = trim(substr(split(clm.clm_billing_note_text,'' '')[1],3,12)) and (db2.doc_ctl_nbr NOT LIKE ''null'' AND db2.doc_ctl_nbr IS NOT NULL)))
    AND db2.bil_ln_num=clm.sl_seq_num
    AND db2.pl_of_srvc = split(clm.healthcareservice_location,'':'')[0]
    AND clm.transactset_create_date >= TO_VARCHAR(DATEADD(YEAR, -1, :V_CURRENT_DATE)::date,''YYYYMMDD'')
	    AND clm.transactset_create_date < TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
		AND clm.xml_md5 = prvall.xml_md5
		;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);
                                 


V_STEP := ''STEP7'';
   
 
V_STEP_NAME := ''Load Institutional DUPLICATE_CLAIMS_REPORT_INTERMEDIATE - 2''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   



INSERT INTO IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_INTERMEDIATE)  

SELECT
	db2.clm_num,
	db2.ben_amt,
	db2.pay_adj_amt,
	CASE WHEN db2.pl_of_srvc=''null'' then split(clm.healthcareservice_location,'':'')[0] else db2.pl_of_srvc END,
	db2.srv_from_dt,
	db2.srv_to_dt,
	db2.cpt_cd,
	db2.proc_mod1,
	db2.proc_mod2,
	db2.proc_mod3,
	db2.rend_prv_npi,
	db2.bill_prv_npi,
	db2.chrg_amt,
	db2.partb_ded_amt,
	db2.coins_amt,
	prv.app_sender_code,
	cast(clm.total_claim_charge_amt as string) as total_claim_charge_amt,
	db2.inst_prof_ind,
	sub.subscriber_id,
	db2.medcr_clm_ctl_nbr,
	db2.clh_trk_id,
	db2.doc_ctl_nbr,
	db2.bil_ln_num
FROM
    (SELECT 
		DISTINCT
        clm_num,
		ben_amt,
		pay_adj_amt,
		pl_of_srvc,
		srv_from_dt,
		srv_to_dt,
		cpt_cd,
		proc_mod1,
		proc_mod2,
		proc_mod3,
		rend_prv_npi,
		bill_prv_npi,
		chrg_amt,
		partb_ded_amt,
		coins_amt,
		inst_prof_ind,
		medcr_clm_ctl_nbr,
		clh_trk_id,
		bil_ln_num,
		doc_ctl_nbr
	FROM IDENTIFIER(:v_DUPLICATE_CLAIMS_REPORT_TEMP)
	WHERE pl_of_srvc NOT IN (''19'', ''21'', ''22'', ''23'', ''31'', ''74'')
		AND inst_prof_ind = ''I''
	) db2

JOIN
    (SELECT 
		DISTINCT
        grp_control_no,xml_md5,
        trancactset_cntl_no,
        transactset_create_date,
        provider_id,
        app_sender_code
      FROM IDENTIFIER(:v_inst_provider)
      WHERE transactset_create_date > TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
        AND transactset_create_date <=TO_VARCHAR(:V_CURRENT_DATE::date,''YYYYMMDD'')
     ) prv
    ON db2.bill_prv_npi=prv.provider_id

JOIN
    (SELECT 
		DISTINCT
        grp_control_no, xml_md5,
        trancactset_cntl_no,
        transactset_create_date,
        subscriber_id
     FROM IDENTIFIER(:v_inst_subscriber)
     WHERE transactset_create_date > TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
        AND transactset_create_date <=TO_VARCHAR(:V_CURRENT_DATE::date,''YYYYMMDD'')
     ) sub
    ON prv.grp_control_no = sub.grp_control_no
    AND prv.trancactset_cntl_no = sub.trancactset_cntl_no
    AND prv.transactset_create_date = sub.transactset_create_date
	AND prv.xml_md5=sub.xml_md5

JOIN IDENTIFIER(:v_inst_claim_part) clm
    ON prv.grp_control_no = clm.grp_control_no
    AND prv.trancactset_cntl_no = clm.trancactset_cntl_no
    AND prv.transactset_create_date = clm.transactset_create_date
    AND ((db2.medcr_clm_ctl_nbr = clm.payer_clm_ctrl_num  and clm.app_sender_code =''EXELA'')
         OR (clm.network_trace_number = db2.clh_trk_id AND db2.clh_trk_id NOT LIKE ''null'' AND db2.clh_trk_id  IS NOT NULL )
        OR (db2.doc_ctl_nbr = trim(substr(split(clm.clm_billing_note_text,'' '')[1],3,12)) and (db2.doc_ctl_nbr NOT LIKE ''null'' AND db2.doc_ctl_nbr IS NOT NULL)))
    AND db2.bil_ln_num=clm.sl_seq_num
    AND clm.transactset_create_date > TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
        AND clm.transactset_create_date <=TO_VARCHAR(:V_CURRENT_DATE::date,''YYYYMMDD'')
		AND clm.xml_md5 = prv.xml_md5
    ;





V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);


V_STEP := ''STEP8'';
   
 
V_STEP_NAME := ''Load Institutional DUPLICATE_CLAIMS_REPORT_MATCHING - 2''; 
   
V_START_TIME := CONVERT_TIMEZONE(''America/Chicago'', CURRENT_TIMESTAMP());
   


INSERT INTO  IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_MATCHING) 

SELECT
	DISTINCT
    db2.clm_num,
    db2.ben_amt,
    db2.pay_adj_amt,
    db2.pl_of_srvc,
    db2.srv_from_dt,
    db2.srv_to_dt,
    db2.cpt_cd,
    db2.proc_mod1,
    db2.proc_mod2,
    db2.proc_mod3,
    db2.rend_prv_npi,
    db2.bill_prv_npi,
    sub.subscriber_id,
    db2.chrg_amt,
    db2.partb_ded_amt,
    db2.coins_amt,
    clm.payer_clm_ctrl_num,
    db2.clh_trk_id,
    clm.transactset_create_date,
    db2.bil_ln_num
FROM
    (SELECT 
		DISTINCT
        clm_num,
        ben_amt,
        pay_adj_amt,
        pl_of_srvc,
        srv_from_dt,
        srv_to_dt,
        cpt_cd,
        proc_mod1,
        proc_mod2,
        proc_mod3,
        rend_prv_npi,
        bill_prv_npi,
        chrg_amt,
        partb_ded_amt,
        coins_amt,inst_prof_ind,
        total_claim_charge_amt,
        subscriber_id,
        medcr_clm_ctl_nbr,
        app_sender_code,
        clh_trk_id,
        bil_ln_num,
        doc_ctl_nbr
      FROM IDENTIFIER(:V_DUPLICATE_CLAIMS_REPORT_INTERMEDIATE)
      WHERE inst_prof_ind = ''I''
        AND pl_of_srvc NOT IN (''19'', ''21'', ''22'', ''23'', ''31'', ''74'')) db2

JOIN
    (SELECT 
		DISTINCT
        grp_control_no,xml_md5,
		trancactset_cntl_no,
		transactset_create_date,
		subscriber_id
    FROM IDENTIFIER(:v_inst_subscriber)
    WHERE transactset_create_date >= TO_VARCHAR(DATEADD(YEAR, -1, :V_CURRENT_DATE)::date,''YYYYMMDD'')
	    AND transactset_create_date < TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
	) sub
    ON sub.subscriber_id = db2.subscriber_id

JOIN
    (SELECT 
		DISTINCT
        grp_control_no,xml_md5,
        trancactset_cntl_no,
        transactset_create_date,
        provider_id
      FROM IDENTIFIER(:v_inst_provider)
      WHERE transactset_create_date >= TO_VARCHAR(DATEADD(YEAR, -1, :V_CURRENT_DATE)::date,''YYYYMMDD'')
	    AND transactset_create_date < TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
     ) prv
    ON sub.grp_control_no = prv.grp_control_no
    AND sub.trancactset_cntl_no = prv.trancactset_cntl_no
    AND sub.transactset_create_date = prv.transactset_create_date
    AND db2.bill_prv_npi=prv.provider_id
	AND sub.xml_md5=prv.xml_md5

JOIN IDENTIFIER(:v_inst_claim_part) clm
    ON sub.grp_control_no = clm.grp_control_no
    AND sub.trancactset_cntl_no = clm.trancactset_cntl_no
    AND sub.transactset_create_date = clm.transactset_create_date
    AND db2.total_claim_charge_amt = clm.total_claim_charge_amt
    AND (db2.medcr_clm_ctl_nbr = clm.payer_clm_ctrl_num
        OR (clm.network_trace_number = db2.clh_trk_id AND db2.clh_trk_id NOT LIKE ''null'' AND db2.clh_trk_id  IS NOT NULL )
        OR (db2.doc_ctl_nbr = trim(substr(split(clm.clm_billing_note_text,'' '')[1],3,12)) and (db2.doc_ctl_nbr NOT LIKE ''null'' AND db2.doc_ctl_nbr IS NOT NULL)))
    AND db2.bil_ln_num=clm.sl_seq_num
    AND db2.pl_of_srvc = split(clm.healthcareservice_location,'':'')[0]
    AND clm.transactset_create_date >= TO_VARCHAR(DATEADD(YEAR, -1, :V_CURRENT_DATE)::date,''YYYYMMDD'')
	    AND clm.transactset_create_date < TO_VARCHAR((:V_CURRENT_DATE-3)::date,''YYYYMMDD'')
		AND sub.xml_md5=clm.xml_md5;


V_LAST_QUERY_ID := (SELECT LAST_QUERY_ID(-1)) ;
   
  
CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME, :V_PROCESS_NAME, :V_SUB_PROCESS_NAME, 
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''SUCCESS'', :V_LAST_QUERY_ID, :V_ROWS_PARSED, :V_ROWS_LOADED, NULL, NULL, NULL);                              










EXCEPTION

WHEN OTHER THEN

CALL IDENTIFIER(:V_SP_PROCESS_RUN_LOGS_DTL) (:DB_NAME, :UTIL_SC, ''PRE_CLM'', :PIPELINE_ID, :PIPELINE_NAME,  :V_PROCESS_NAME, :V_SUB_PROCESS_NAME,
                                 :V_STEP, :V_STEP_NAME, :V_START_TIME, CURRENT_TIMESTAMP(), ''FAILED'', :V_LAST_QUERY_ID, NULL, NULL, :SQLERRM, :SQLCODE, :SQLSTATE);


RAISE;

END;

';
