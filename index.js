import shell from 'shelljs'
import fs from 'fs'
import path from 'path'
import { parseFile } from 'fast-csv'
import postgres from 'postgres'
import { format } from 'date-fns'
import { importWorkers } from './tables/worker.js'

// local instance
// const postgreSQL = postgres({
//   host: 'localhost',
//   port: '5432',
//   user: 'supershazwi',
//   password: 'noobies',
//   db: 'camans',
// });

// test instance
// const postgreSQL = postgres({
//   host: '209.58.180.167',
//   port: '5432',
//   user: 'supershazwi',
//   password: 'noobies',
//   db: 'camans',
// });

// production instance
const postgreSQL = postgres({
  host: '209.58.160.203',
  port: '5432',
  user: 'twc2',
  password: 'dAKh2hBI95rKcm3',
  db: 'camans',
});


// query Camans v1 mysql and populate exports folder
shell.exec(`sshpass -p 'jMAvu8SHzrq' ssh -t -t root@case.twc2.org.sg -p 56988 << EOF
  ./migration.sh
  exit
EOF`);

// download exports folder from VPS to local machine
shell.exec(`sshpass -p 'jMAvu8SHzrq' sftp -oPort=56988 root@case.twc2.org.sg << EOF
  get -R exports .
  exit
EOF`);

// download attachments folder from VPS to local machine
shell.exec(`sshpass -p 'jMAvu8SHzrq' sftp -oPort=56988 root@case.twc2.org.sg << EOF
  cd /var/lib/tomcat7/webapps/ROOT
  get -R workers .
  exit
EOF`);

const v1_v2_column_maps = {
  'tbl_accomodation': {
    'Entry_date': 'date_record_created',
    'Accomodation_provided': 'accommodation_provided',
    'Accomodation_provided_more': 'accommodation_provided_more',
    'Accomodation_type': 'accommodation_type',
    'Accomodation_type_more': 'accommodation_type_more',
    'Accomodation_location': 'accommodation_location',
    'Accomodation_condition': 'accommodation_condition',
    'Accomodation_charged': 'accommodation_charged',
    'Accomodation_self_paid': 'accommodation_self_paid',
    'Accomodation_meals': 'accommodation_meals',
    'Accomodation_start': 'when_accommodation_start',
    'Accomodation_end': 'when_accommodation_end',
    'Accomodation_remarks': 'accommodation_remarks',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_agent': {
    'Entry_date': 'date_record_created',
    'Agent_company': 'agent_company',
    'Agent_person_name': 'agent_person_name',
    'Agent_location': 'agent_location',
    'Agent_location_more': 'agent_location_more',
    'Agent_address': 'agent_address',
    'Agent_contact': 'agent_contact',
    'Agent_amt_paid': 'agent_amt_paid',
    'Agent_amt_owed': 'agent_amt_owed',
    'Agent_fee_shared': 'agent_fee_shared',
    'Agent_fee_training': 'agent_fee_training',
    'Agent_fee_airfare': 'agent_fee_airfare',
    'Agent_fee_where': 'agent_fee_where',
    'Agent_fee_when': 'agent_fee_when',
    'Agent_fee_repay': 'agent_fee_repay',
    'Agent_employer': 'agent_employer',
    'Agent_remarks': 'agent_remarks',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_aggravatingIssue': {
    'Entry_date': 'date_record_created',
    'Aggra_issue': 'aggra_issue',
    'Aggra_issue_more': 'aggra_issue_more',
    'Aggra_loss': 'aggra_loss',
    'Aggra_remarks': 'aggra_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_auxiliaryCaseWorker': {
    'Entry_date': 'date_record_created',
    'Aux_name': 'auxiliary_case_worker',
    'Aux_start': 'date_aux_start',
    'Aux_end': 'date_aux_end',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_bankAccount': {
    'Entry_date': 'date_record_created',
    'Bank_account_name': 'bank_account_name',
    'Bank_account_number': 'bank_account_number',
    'Bank_name': 'bank_name',
    'Bank_branch_name': 'bank_branch_name',
    'Bank_branch_code': 'bank_branch_code',
    'Bank_branch_address': 'bank_branch_address',
    'Bank_swift_code': 'bank_swift_code',
    'Bank_account_remarks': 'bank_account_remarks',
    'Bank_obsolete_date': 'bank_obsolete_date',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_benefit': {
    'Entry_date': 'date_record_created',
    'Bene_type': 'bene_item',
    'Bene_type_more': 'bene_item_more',
    'Bene_date': 'date_bene_issued',
    'Bene_giver': 'bene_giver',
    'Bene_serial': 'bene_serial',
    'Bene_purpose': 'bene_purpose',
    'Bene_value': 'bene_value',
    'Bene_rem': 'bene_remarks',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_caseDiscussion': {
    'Entry_date': 'date_record_created',
    'Discuss_date': 'date_of_discussion',
    'Discuss_time': 'time_of_discussion',
    'Discuss_where': 'discuss_where',
    'Discuss_where_more': 'discuss_where_more',
    'Discuss_worker_present': 'discuss_worker_present',
    'Discuss_TWC2_pers1': 'discuss_twc2_pers1',
    'Discuss_TWC2_pers2': 'discuss_twc2_pers2',
    'Discuss_oth_pers': 'discuss_oth_pers',
    'Discuss_translator': 'discuss_translator',
    'Discuss_topic': 'discuss_topic',
    'Discuss_gist': 'discuss_gist',
    'Discuss_assist': 'discuss_assist',
    'Discuss_calculate': 'discuss_calculate',
    'Discuss_action': 'discuss_action',
    'Discuss_rem': 'discuss_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_criminalCaseMilestone': {
    'Entry_date': 'date_record_created',
    'Miles_cr_date': 'date_cr_milestone_reached',
    'Miles_cr_reached': 'cr_milestone_reached',
    'Miles_cr_reached_more': 'cr_milestone_reached_more',
    'Miles_cr_charges': 'cr_milestone_charges',
    'Miles_cr_sentence': 'cr_milestone_sentence',
    'Miles_cr_rem': 'cr_milestone_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_currentPass': {
    'Entry_date': 'date_record_created',
    'Pass_type': 'current_pass_type',
    'Pass_type_more': 'current_pass_type_more',
    'Pass_number': 'current_pass_number',
    'Pass_application_date': 'date_current_pass_issued',
    'Pass_issue_date': 'date_current_pass_application',
    'Pass_expiry_date': 'date_current_pass_expires',
    'Pass_issuer': 'current_pass_issuer',
    'Pass_obsolete_date': 'date_current_pass_obsolete',
    'Pass_remarks': 'current_pass_remarks',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_digitalContact': {
    'Entry_date': 'date_record_created',
    'Digital_contact_type': 'digital_contact_type',
    'Digital_more': 'digital_contact_type_more',
    'Email_or_QQ_address': 'digital_address',
    'Owner_of_electronic_contact': 'owner_of_electronic_contact',
    'Digital_remarks': 'digital_remarks',
    'Obsolete_date': 'date_digital_contact_obsolete',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_employer': {
    'Entry_date': 'date_record_created',
    'Employer_official_name': 'employer_official_name',
    'Employer_ID': 'employer_id',
    'Employer_address': 'employer_address',
    'Employer_contacts': 'employer_contacts',
    'Employer_persons': 'employer_persons',
    'Employer_remarks': 'employer_remarks',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_employmentContract': {
    'Entry_date': 'date_record_created',
    'Contract_date': 'date_of_contract',
    'Contract_where': 'contract_where',
    'Contract_language': 'contract_language',
    'Contract_opposite_name': 'contract_opposite_name',
    'Contract_opposite_relationship': 'contract_opposite_relationship',
    'Contract_occupation': 'contract_occupation',
    'Contract_basic_salary': 'contract_basic_salary',
    'Contract_allowances': 'contract_allowances',
    'Contract_deduction_details': 'contract_deductions',
    'Contract_duration': 'contract_duration',
    'Contract_duress': 'contract_duress',
    'Contract_remarks': 'contract_remarks',
    'Job_key': 'job_id',
    'Worker_FIN_Number': 'worker_id'
  },
  'tbl_familyMember': {
    'Entry_date': 'date_record_created',
    'Name_of_family_member': 'name_of_family_member',
    'Family_member_relationship': 'family_member_relationship',
    'Family_member_where': 'family_member_where',
    'Family_member_phone_number': 'family_member_phone_number',
    'Family_member_digital': 'family_member_digital',
    'Family_member_remarks': 'family_member_remarks',
    'Family_member_obsolete': 'date_family_member_obsolete',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_homeCountryAddress': {
    'Entry_date': 'date_record_created',
    'Home_country_address': 'home_country_address',
    'Home_country_obsolete': 'date_home_address_obsolete',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_homeCountryPhoneNumber': {
    'Entry_date': 'date_record_created',
    'Home_country_telephone_number': 'home_country_phone_number',
    'Owner_of_number': 'owner_of_number',
    'Home_phone_obsolete': 'date_home_phone_obsolete',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_hospital': {
    'Entry_date': 'date_record_created',
    'Hosp_update': 'date_hosp_info_received',
    'Hosp_name': 'hosp_name',
    'Hosp_name_more': 'hosp_name_more',
    'Hosp_doctor': 'hosp_doctor',
    'Hosp_remark': 'hosp_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_illness': {
    'Entry_date': 'date_record_created',
    'Illness_start_when': 'when_illness_began',
    'Illness_diag_when': 'when_illness_diagnosed',
    'Illness_diag_who': 'illness_diag_who',
    'Illness_nature': 'illness_nature',
    'Illness_work_related': 'illness_work_related',
    'Illness_why': 'illness_why',
    'Illness_rem': 'illness_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_injury': {
    'Entry_date': 'date_record_created',
    'Injury_date': 'date_of_accident',
    'Injury_time': 'injury_time',
    'Injury_location': 'injury_location',
    'Injury_death': 'injury_death',
    'Injury_body_part': 'injury_body_part',
    'Injury_how': 'injury_how',
    'Injury_ambulance': 'injury_ambulance',
    'Injury_initial_treatment': 'injury_initial_treatment',
    'Injury_work_related': 'injury_work_related',
    'Injury_remarks': 'injury_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_ipaDetails': {
    'Entry_date': 'date_record_created',
    'IPA_pass_type': 'ipa_pass_type',
    'IPA_pass_type_more': 'ipa_pass_type_more',
    'IPA_application_date': 'date_ipa_application',
    'IPA_employer_name': 'ipa_employer_name',
    'IPA_agent_name': 'ipa_agent_name',
    'IPA_industry': 'ipa_industry',
    'IPA_occupation': 'ipa_occupation',
    'IPA_period_years': 'ipa_period_years',
    'IPA_basic_salary': 'ipa_basic_salary',
    'IPA_allowances': 'ipa_allowances',
    'IPA_allowances_details': 'ipa_allowances_details',
    'IPA_deduction': 'ipa_deduction',
    'IPA_deduction_details': 'ipa_deduction_details',
    'Housing_provided': 'housing_provided',
    'IPA_remarks': 'ipa_remarks',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_job': {
    'Entry_date': 'date_record_created',
    'Employer_name': 'employer_name',
    'Workpass_type': 'workpass_type',
    'Workpass_more': 'workpass_more',
    'Job_sector': 'job_sector',
    'Job_sector_more': 'job_sector_more',
    'Job_start_date': 'when_job_started',
    'Job_end_date': 'when_job_ended',
    'Worker_FIN_number': 'worker_id',
    'Job_whether_TJS': 'job_whether_tjs',
    'Job_remarks': 'job_remarks'
  },
  'tbl_language': {
    'Entry_date': 'date_record_created',
    'Worker_main_language': 'worker_main_language',
    'Worker_main_language_more': 'worker_main_language_more',
    'Spoken_english_standard': 'spoken_english_standard',
    'Language_remarks': 'language_remarks',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_lawyer': {
    'Entry_date': 'date_record_created',
    'Lawyer_update': 'date_lawyer_info_received',
    'Lawyer_firm': 'lawyer_firm',
    'Lawyer_firm_more': 'lawyer_firm_more',
    'Lawyer_name': 'lawyer_name',
    'Lawyer_remarks': 'lawyer_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_leadCaseWorker': {
    'Entry_date': 'date_record_created',
    'Lead_case_worker': 'lead_case_worker',
    'Lead_start': 'date_lead_start',
    'Lead_end': 'date_lead_end',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_mcStatus': {
    'Entry_date': 'date_record_created',
    'MC_update': 'date_mc_info_received',
    'MC_status': 'mc_status',
    'MC_status_more': 'mc_status_more',
    'MC_exp_date': 'date_mc_expires',
    'MC_days_cumul': 'mc_days_cumul',
    'MC_rem': 'mc_status_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_nextOfKin': {
    'Entry_date': 'date_record_created',
    'Kin_name': 'kin_name',
    'Kin_relationship': 'kin_relationship',
    'Kin_Id_doc': 'kin_id_doc',
    'Kin_phone': 'kin_phone',
    'Kin_digital': 'kin_digital',
    'Kin_address': 'kin_address',
    'Kin_proof': 'kin_proof',
    'Kin_remarks': 'kin_remarks',
    'Kin_obsolete_date': 'date_kin_obsolete',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_nickname': {
    'Entry_date': 'date_record_created',
    'Nickname': 'nickname',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_otherComplaint': {
    'Entry_date': 'date_record_created',
    'Other_plaint_date': 'date_other_plaint_lodged',
    'Other_plaint_agency': 'other_plaint_agency',
    'Other_plaint_who': 'other_plaint_who',
    'Other_plaint_who_more': 'other_plaint_who_more',
    'Other_plaint_mode': 'other_plaint_mode',
    'Other_plaint_mode_more': 'other_plaint_mode_more',
    'Other_plaint_details': 'other_plaint_details',
    'Other_plaint_rem': 'other_plaint_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_otherProblem': {
    'Entry_date': 'date_record_created',
    'Oth_problem_desc': 'oth_problem_desc',
    'Oth_problem_loss': 'oth_problem_loss',
    'Oth_problem_rem': 'oth_problem_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_passport': {
    'Entry_date': 'date_record_created',
    'Passport_number': 'passport_number',
    'Passport_country': 'passport_country',
    'Passport_issue_date': 'date_passport_issued',
    'Passport_expiry_date': 'date_passport_expires',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_policeReport': {
    'Entry_date': 'date_record_created',
    'Police_rpt_date': 'police_rpt_date',
    'Police_rpt_station': 'police_rpt_station',
    'Police_rpt_person': 'police_rpt_twc2_person',
    'Police_rpt_ref_nbr': 'police_rpt_ref_nbr',
    'Police_rpt_details': 'police_rpt_details',
    'Police_rpt_rem': 'police_rpt_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_problem': {
    'Entry_date': 'date_record_created',
    'Chief_problem_remarks': 'chief_problem_remarks',
    'Chief_problem_more': 'chief_problem_more',
    'Chief_problem': 'chief_problem',
    'Chief_problem_date': 'date_problem_registered',
  },
  'tbl_salaryHistory': {
    'Entry_date': 'date_record_created',
    'Sal_mode': 'sal_mode',
    'Sal_mode_more': 'sal_mode_more',
    'Sal_loss_total': 'sal_loss_total',
    'Sal_loss_1_year': 'sal_loss_1_year',
    'Sal_hist_basic': 'sal_hist_basic',
    'Sal_hist_ot': 'sal_hist_ot',
    'Sal_hist_allowances': 'sal_hist_allowances',
    'Sal_hist_deductions': 'sal_hist_deductions',
    'Sal_hist_kickbacks': 'sal_hist_kickbacks',
    'Sal_hist_other': 'sal_hist_other',
    'Sal_his_remarks': 'sal_his_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_sgAddress': {
    'Entry_date': 'date_record_created',
    'Singapore_address': 'sg_address',
    'Singapore_address_obsolete': 'date_sg_address_obsolete',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_sgFriend': {
    'Entry_date': 'date_record_created',
    'friend_sg_name': 'friend_sg_name',
    'friend_sg_phone': 'friend_sg_phone',
    'friend_sg_rel': 'friend_sg_rel',
    'friend_sg_rem': 'friend_sg_remarks',
    'friend_sg_obsolete': 'date_friend_sg_obsolete',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_sgPhoneNumber': {
    'Entry_date': 'date_record_created',
    'Sg_phone_num': 'sg_phone_number',
    'Sg_phone_obsolete': 'date_sg_phone_obsolete',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_worker': {
    'Entry_date': 'date_record_created',
    'Name_of_worker': 'name_of_worker',
    'FIN_number': 'fin_number',
    'Worker_registration_date': 'date_worker_registered',
    'Create_for': 'created_for',
    'Gender': 'gender',
    'Nationality': 'nationality',
    'Nationality_more': 'nationality_more',
    'Date_of_birth': 'date_of_birth'
  },
  'tbl_transferRepat': {
    'Entry_date': 'date_record_created',
    'Ttr_update': 'date_info_received',
    'Ttr_status': 'transfer_repat_status',
    'Ttr_status_more': 'transfer_repat_more',
    'Departure_date': 'when_departing',
    'Name_new_employer': 'name_new_employer',
    'New_job': 'new_job',
    'Ttr_rem': 'transfer_repat_remarks',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_user': {
    'Entry_date': 'date_record_created',
    'NRIC_number': 'user_nric',
    'Full_name': 'user_fullname',
    'Alias': 'user_altname',
    'Username': 'user_username',
    'Email_address': 'user_email_address',
    'Phone_number': 'user_phone_number',
    'Gender': 'user_gender',
    'Role': 'user_role',
    'Status': 'user_status'
  },
  'tbl_verbalAssurances': {
    'Entry_date': 'date_record_created',
    'Verbal_name': 'verbal_name',
    'Verbal_relationship': 'verbal_relationship',
    'Verbal_when': 'verbal_when',
    'Verbal_where': 'verbal_where',
    'Verbal_content': 'verbal_content',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_wicaClaim': {
    'Entry_date': 'date_record_created',
    'Wica_claim_date': 'date_wica_claim_lodged',
    'Wica_ref_nbr': 'wica_ref_nbr',
    'Wica_claim_rem': 'wica_claim_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_wicaStatus': {
    'Entry_date': 'date_record_created',
    'Wicamon_update': 'date_wica_status_checked',
    'wicamon_status': 'wica_status_class',
    'Wicamon_status_more': 'wica_status_details',
    'Wicamon_points': 'wica_status_points',
    'Wicamon_dollars': 'wica_status_dollars',
    'Wicamon_remarks': 'wica_status_remarks',
    'Prob_key': 'problem_id',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_workHistory': {
    'Entry_date': 'date_record_created',
    'Work_hist_date': 'work_hist_when_start',
    'Work_hist_how': 'work_hist_how',
    'Work_hist_how_more': 'work_hist_how_more',
    'Work_hist_first': 'work_hist_job_ordinal',
    'Work_hist_year_arrive': 'work_hist_year_first_arrive',
    'Work_hist_previous': 'work_hist_previous',
    'Work_hist_previous_problems': 'work_hist_previous_problems',
    'Work_hist_remarks': 'work_hist_remarks',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
  'tbl_workplace': {
    'Entry_date': 'date_record_created',
    'Workplace_type': 'workplace_type',
    'Workplace_type_more': 'workplace_type_more',
    'Workplace_whose': 'workplace_whose',
    'Workplace_persons': 'workplace_location',
    'Workplace_employer_relationship': 'workplace_employer_relationship',
    'Workplace_direct': 'workplace_direct',
    'Workplace_direct_more': 'workplace_direct_more',
    'Workplace_start': 'workplace_start',
    'Workplace_end': 'workplace_end',
    'Workplace_condition': 'workplace_condition',
    'Workplace_safety': 'workplace_safety',
    'Workplace_remarks': 'workplace_remarks',
    'Job_key': 'job_id',
    'Worker_FIN_number': 'worker_id'
  },
}

const today = format(new Date(), 'yyyy-MMM-dd HH:mm');

// go through each table and transfrom from v1 to v2 tables
// user
const users = [];
const usersMap = {};
parseFile('./exports/tbl_user.csv', {headers: true})
  .on('error', error => console.error(error))
  .on('data', row => {
    const user = {};
    Object.keys(row).forEach(key => {
      if (v1_v2_column_maps['tbl_user'][key]) user[v1_v2_column_maps['tbl_user'][key]] = row[key];
    });
    user.date_last_updated = today;
    user.user_password = '123!@#';
    user.created_by = 0;

    if (!usersMap[user.user_email_address] && user.user_email_address !== 'alex.au@twc2.org.sg') {
      usersMap[user.user_email_address] = true;
      users.push(user);
    }
  })
  .on('end', async (rowCount) => {
    // insert migration user
    // const user = await postgreSQL`INSERT into public.users (id, user_fullname, user_username, user_password, user_email_address, user_phone_number, user_role, user_status, date_record_created, date_last_updated) VALUES (0, 'migration user', 'migration_user', '123!@#', 'migration_user@twc2.org.sg', '62477001', 'Admin', 'Active', ${today}, ${today})`;

    // insert all other users
    // const columns = Object.keys(users[0]);
    
    // const user2 = await postgreSQL`INSERT INTO public.users ${postgreSQL(users, columns)}`;
    // console.log(`=== Inserted ${rowCount} users ===`);

    importWorkers();
  });

export {
  postgreSQL,
  v1_v2_column_maps,
  today
};
