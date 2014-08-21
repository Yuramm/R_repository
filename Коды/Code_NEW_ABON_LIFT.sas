%let mr=09 01 03 04 06 07 08 09;
%let dt_camp=201307;

%let YYMM0=201307;
%let YYMM1=201306;
%let YYMM2=201305;
%let YYMM3=201304;

%let mvABNTCount=0;

%macro MOB;
%do ii=1 %to 1;
%let mrh=%scan(&mr,&ii,' ');

    data CBM_NEW_MR&mrh. (keep=regid app_n acc_n activation_date reporting_date phone_num age source)         ;
  set A_APP.ABNT_&dt_camp._MR&mrh. (in=_inAPP keep=regid app_n acc_n activation_date phone_num birth_dt _date_) ;

length age 3 reporting_date 8 ;
      if ( Today() - datepart(activation_date)) >= 30 and (Today() - datepart(activation_date))<= 90 ; 
      age = year(_date_) - year(datepart(birth_dt));
      reporting_date = Today();
      source = "NEW";
    run;

  data CLCD_MR&mrh. (keep=app_n regid all_clc sms_clc mob_inet_clc);
      set A_SRV.CLCD_&YYMM0._MR&mrh. 
          A_SRV.CLCD_&YYMM1._MR&mrh. 
          A_SRV.CLCD_&YYMM2._MR&mrh. 
          A_SRV.CLCD_&YYMM3._MR&mrh.;
    run;

    proc sort data=CLCD_MR&mrh. out=CLCD_MR&mrh.;
      by regid app_n;
    run;

    data CLCD_MR&mrh. (keep=app_n regid all_clc_s sms_clc_s mob_inet_s);
      set CLCD_MR&mrh.;
      by regid app_n;
      retain all_clc_s sms_clc_s mob_inet_s 0;
      if last.app_n then do;
        output;
        all_clc_s=0; sms_clc_s=0; mob_inet_s=0;
      end;
      all_clc_s=sum(all_clc_s,all_clc); 
      sms_clc_s=sum(sms_clc_s,sms_clc); 
      mob_inet_s=sum(mob_inet_s,mob_inet_clc); 
    run;

    
    data ROAM_MR&mrh. (keep=app_n regid roam_all_clc roam_mts_sms_out_clc roam_nat_sms_out_clc roam_cntry_sms_out_clc roam_gprs_clc);
      set A_SRV.ROAM_&YYMM0._MR&mrh. 
          A_SRV.ROAM_&YYMM1._MR&mrh. 
          A_SRV.ROAM_&YYMM2._MR&mrh. 
          A_SRV.ROAM_&YYMM3._MR&mrh.;
    run;

    proc sort data=ROAM_MR&mrh. out=ROAM_MR&mrh.;
      by regid app_n;
    run;

    data ROAM_MR&mrh. (keep=regid app_n roam_all_clc_s roam_mts_sms_out_clc_s roam_nat_sms_out_clc_s roam_cntry_sms_out_clc_s roam_gprs_clc_s);
      set ROAM_MR&mrh. ;
      by regid app_n;
      retain roam_all_clc_s roam_mts_sms_out_clc_s roam_nat_sms_out_clc_s roam_cntry_sms_out_clc_s roam_gprs_clc_s 0;
      if last.app_n then do;
        output;
        roam_all_clc_s=0; 
		 end;
roam_mts_sms_out_clc_s=0; roam_nat_sms_out_clc_s=0; roam_cntry_sms_out_clc_s=0; roam_gprs_clc_s=0;
    
      roam_all_clc_s=sum(roam_all_clc_s,roam_all_clc); 
      roam_mts_sms_out_clc_s=sum(roam_mts_sms_out_clc_s,roam_mts_sms_out_clc); 
      roam_nat_sms_out_clc_s=sum(roam_nat_sms_out_clc_s,roam_mts_sms_out_clc); 
      roam_cntry_sms_out_clc_s=sum(roam_cntry_sms_out_clc_s,roam_cntry_sms_out_clc); 
      roam_gprs_clc_s=sum(roam_gprs_clc_s,roam_gprs_clc);
    run;



 proc sort data=CBM_NEW_MR&mrh. ;
      by acc_n;
    run;
%end;
%mend;
%MOB

%macro MOB1;
%do pp=1 %to 1;
%let mrh=%scan(&mr,&pp,' ');


    %do i=0 %to 3;
      data PMNT_&&YYMM&i.._MR&mrh.(keep=regid app_n pay_sum pay_num pay_max);
        merge 
CBM_NEW_MR&mrh.(in=_inCBM)
A_ACC.PMNT_&&YYMM&i.._MR&mrh.;
        by acc_n;
        if _inCBM;
      run;
    %end;

    data PMNT_MR&mrh. (keep=regid app_n pay_sum pay_num pay_max);
      set PMNT_&YYMM0._MR&mrh. PMNT_&YYMM1._MR&mrh. PMNT_&YYMM2._MR&mrh. PMNT_&YYMM3._MR&mrh.;
    run;

    proc sort data=PMNT_MR&mrh. out=PMNT_MR&mrh.;
      by regid app_n;
    run;

    data PMNT_MR&mrh. (keep=regid app_n pay_sum_s pay_num_s pay_max_s);
      set PMNT_MR&mrh.;
      by regid app_n;
      retain pay_sum_s pay_num_s pay_max_s 0;
      if last.app_n then do;
        output;
        pay_sum_s=0; pay_num_s=0; pay_max_s=0;
      end;
      pay_sum_s=sum(pay_sum_s,pay_sum); 
      pay_num_s=sum(pay_num_s,pay_num); 
      pay_max_s=sum(pay_max_s,pay_max); 
    run;
%end;
%mend;
%MOB1



%macro MOB2;
%do kk=1 %to 1;
%let mrh=%scan(&mr,&kk,' ');

proc sort data=WORK.CBM_NEW_MR&mrh.;
      by regid app_n;
    run;
data CBM_NEW_MR&mrh. (rename=(pay_sum_s=TU_sum pay_num_s=TU_count pay_max_s=TU_max));

      merge 
CBM_NEW_MR&mrh.(in=_inCBM ) 
CLCD_MR&mrh. 
ROAM_MR&mrh. 
PMNT_MR&mrh. ;
      by regid app_n;
      if _inCBM;
      length ARPU ARPU_T ARPU_sms_T TU_sum ARPU_data_T TU_sum_T TU_count_T 8 zero_segment_flag 3;

      ARPU=coalesce(all_clc_s,0)+coalesce(roam_all_clc_s,0);

      ARPU_T=(ARPU)/(Today()-datepart(activation_date));
      ARPU_sms_T=(coalesce(sms_clc_s,0)+coalesce(roam_mts_sms_out_clc_s,0)+coalesce(roam_nat_sms_out_clc_s,0)+
                  coalesce(roam_cntry_sms_out_clc_s,0))/(Today()-datepart(activation_date));
      ARPU_data_T=(coalesce(mob_inet_s,0)+coalesce(roam_gprs_clc_s,0))/(Today()-datepart(activation_date));
      TU_sum_T=coalesce(pay_sum_s,0)/(Today()-datepart(activation_date));
      TU_count_T=coalesce(pay_num_s,0)/(Today()-datepart(activation_date));
      
      if (pay_num_s =. or pay_num_s<=0) and (ARPU =. or ARPU<=0) then zero_segment_flag=1; 
	else zero_segment_flag=0; 
    run;

data CBM_NEW_MR&mrh. ;
      set CBM_NEW_MR&mrh.;
      length k_age 3;
      if (age <= 0 or age = .) then k_age = 0;
      else if (age <= 25) then k_age = 1;
      else if (age <= 35) then k_age = 2;
      else if (age <= 45) then k_age = 3;
      else k_age = 4;
    run; 


    data _null_;
      set CBM_NEW_MR&mrh. nobs=nobs;
        call symput('mvABNTCount', nobs);
      stop;
    run;

   
    proc sort data=CBM_NEW_MR&mrh.;
      by descending ARPU;
    run;

    data CBM_NEW_ID_PP_ARPU_MR&mrh.;
      set CBM_NEW_MR&mrh.;
      if _N_<=floor(&mvABNTCount/100) then output; 
else stop;
    run;

    proc sort data=CBM_NEW_MR&mrh.;
      by descending TU_sum;
    run;
 
    data CBM_NEW_ID_PP_TU_MR&mrh.;
      set CBM_NEW_MR&mrh.;
      if _N_<=floor(&mvABNTCount/100) then output; 
else stop;
    run;

    proc sort data=CBM_NEW_ID_PP_ARPU_MR&mrh.;by regid app_n; run;  
    proc sort data=CBM_NEW_ID_PP_TU_MR&mrh.;by regid app_n;run;  
	proc sort data=CBM_NEW_MR&mrh.;by regid app_n;run;  


    data CBM_NEW_FOR_STRAT_MR&mrh.;
      merge CBM_NEW_MR&mrh.  (in=q)
            CBM_NEW_ID_PP_TU_MR&mrh.(in=_inTU keep=regid app_n) 
            CBM_NEW_ID_PP_ARPU_MR&mrh.(in=_inARPU keep=regid app_n)   ;
      by regid app_n;
	  if q;
	   length top_segment_flag 3;
      if(_inARPU or _inTU)=1 then top_segment_flag=1; else top_segment_flag=0;
    run;



data CBM_NEW_FOR_STRAT_MR&mrh.;
set CBM_NEW_FOR_STRAT_MR&mrh.;
if zero_segment_flag ^= 1 and top_segment_flag ^= 1;
run; 


%end;
%mend;
%MOB2


%let var1=ARPU_T;
%let GROUPS1=4;

%let var2=ARPU_sms_T;
%let GROUPS2=2;

%let var3=ARPU_data_T ;
%let GROUPS3=2;

%let var4=TU_sum_T;
%let GROUPS4=3;

%let var5=TU_count_T;
%let GROUPS5=3;

%let var6=TU_max ;
%let GROUPS6=3;



proc sort data =WORK.CBM_NEW_FOR_STRAT_MR09 out=Q1; 
by descending  &var1.;
run;

%_eg_conditional_dropds(WORK.SORTTempTableSorted,
		WORK.RANKRANKEDQUERY_FOR_CBM_NEW_0001);

PROC SQL;
	CREATE VIEW WORK.SORTTempTableSorted AS
		SELECT *	FROM Q1
;
QUIT;
PROC RANK DATA = WORK.SORTTempTableSorted
	GROUPS=&GROUPS1.
	TIES=MEAN
	OUT=WORK.R1;
	VAR &var1.;
RANKS ARPU_k ;
RUN; QUIT;
%_eg_conditional_dropds(WORK.SORTTempTableSorted);
TITLE; FOOTNOTE;



proc sort data =WORK.R1 out=Q2; 
by descending &var2.  ;
run;
%_eg_conditional_dropds(WORK.SORTTempTableSorted,WORK.RANKRANKEDQUERY_FOR_CBM_NEW_0001);
PROC SQL;
	CREATE VIEW WORK.SORTTempTableSorted AS
		SELECT *
	FROM Q2;
QUIT;
PROC RANK DATA = WORK.SORTTempTableSorted
	GROUPS=&GROUPS2.
	TIES=MEAN
	OUT=WORK.R2;
	VAR &var2. ;
RANKS ARPU_sms_k ;
RUN; QUIT;
%_eg_conditional_dropds(WORK.SORTTempTableSorted);
TITLE; FOOTNOTE;



proc sort data =WORK.R2 out=Q3; 
by descending &var3. ;
run;

%_eg_conditional_dropds(WORK.SORTTempTableSorted,
		WORK.RANKRANKEDQUERY_FOR_CBM_NEW_0001);

PROC SQL;
	CREATE VIEW WORK.SORTTempTableSorted AS
		SELECT *
	FROM Q3
;
QUIT;
PROC RANK DATA = WORK.SORTTempTableSorted
	GROUPS=&GROUPS3.
	TIES=MEAN
	OUT=WORK.R3;
	VAR &var3. ;
RANKS ARPU_data_k ;
RUN; QUIT;
%_eg_conditional_dropds(WORK.SORTTempTableSorted);
TITLE; FOOTNOTE;


proc sort data =WORK.R3 out=Q4; 
by descending TU_sum_T  ;
run;

%_eg_conditional_dropds(WORK.SORTTempTableSorted,
		WORK.RANKRANKEDQUERY_FOR_CBM_NEW_0001);

PROC SQL;
	CREATE VIEW WORK.SORTTempTableSorted AS
		SELECT *
	FROM Q4
;
QUIT;
PROC RANK DATA = WORK.SORTTempTableSorted
	GROUPS=&GROUPS4.
	TIES=MEAN
	OUT=WORK.R4;
	VAR TU_sum_T ;
RANKS TU_sum_k ;
RUN; QUIT;
%_eg_conditional_dropds(WORK.SORTTempTableSorted);
TITLE; FOOTNOTE;


proc sort data =WORK.R4 out=Q5; 
by descending TU_count_T  ;
run;

%_eg_conditional_dropds(WORK.SORTTempTableSorted,
		WORK.RANKRANKEDQUERY_FOR_CBM_NEW_0001);

PROC SQL;
	CREATE VIEW WORK.SORTTempTableSorted AS
		SELECT *
	FROM Q5
;
QUIT;
PROC RANK DATA = WORK.SORTTempTableSorted
	GROUPS=&GROUPS5.
	TIES=MEAN
	OUT=WORK.R5;
	VAR TU_count_T ;
RANKS TU_count_k ;
RUN; QUIT;
%_eg_conditional_dropds(WORK.SORTTempTableSorted);
TITLE; FOOTNOTE;





proc sort data =WORK.R5 out=Q6; 
by descending &var6.  ;
run;

%_eg_conditional_dropds(WORK.SORTTempTableSorted,
		WORK.RANKRANKEDQUERY_FOR_CBM_NEW_0001);

PROC SQL;
	CREATE VIEW WORK.SORTTempTableSorted AS
		SELECT *
	FROM Q6
;
QUIT;
PROC RANK DATA = WORK.SORTTempTableSorted
	GROUPS=&GROUPS6.
	TIES=MEAN
	OUT=WORK.R6;
	VAR &var6.;
RANKS TU_max_k ;
RUN; QUIT;
%_eg_conditional_dropds(WORK.SORTTempTableSorted);
TITLE; FOOTNOTE;



PROC SQL;
   CREATE TABLE WORK.r7 AS 
   SELECT t1.*,
            (put(t1.k_age,z1.) || 
            put(t1.ARPU_k,z1.) || 
            put(t1.ARPU_sms_k,z1.) || 
            put(t1.ARPU_data_k,z1.) || 
            put(t1.TU_sum_k,z1.) || 
            put(t1.TU_count_k,z1.) || 
            put(t1.TU_max_k,z1.) ) AS CAT 
      FROM WORK.R6 t1;
QUIT;


PROC SQL;
   CREATE TABLE WORK.r8 AS 
   SELECT t1.CAT, 
         (COUNT(t1.app_n)) AS COUNT_of_app_n
      FROM r7 t1
      GROUP BY t1.CAT
      ORDER BY COUNT_of_app_n DESC;
QUIT;



PROC SQL;
   CREATE TABLE WORK.cat1 AS 
   SELECT t1.CAT, 
          t1.COUNT_of_app_n
      FROM r8 t1
      WHERE t1.COUNT_of_app_n >= 10;
QUIT;


proc sort data =WORK.CAT1; 
by CAT;
run;


proc sort data =WORK.r7; 
by CAT;
run;

data test;
merge 
WORK.r7 (in=a)
WORK.CAT1 (in=b);
by cat;
if a=b=1;
run;



%_eg_conditional_dropds(WORK.SORT, WORK.RAND);
PROC SORT
	DATA=WORK.TEST()
	OUT=WORK.SORT;
	BY CAT;
RUN;
PROC SURVEYSELECT DATA=WORK.SORT
	OUT=WORK.RAND
	METHOD=SRS
	RATE=%SYSEVALF(10/100);
	STRATA CAT / ALLOC=PROP;
RUN;
QUIT;
%_eg_conditional_dropds(WORK.SORT);



data CG;set WORK.RAND;target=0;run;
proc sort data =CG; by regid app_n;run;
proc sort data =TEST; by regid app_n;run;

data TG;
merge
WORK.TEST (in=all) CG (in=_CG);
by regid app_n;
if _CG=0;
target=1;
run;

data ITOG;set TG CG;run;