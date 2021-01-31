***Data Library***;
libname data '/home/u49641306/WeiZhang/data';

**Prepare Data: 
EDYG: Domestic Equity Growth Funds
EDYB: Domestic Equity Growth & Income Funds
EDYI: Domestic Equity Income Funds;

data fundsum;
set data.fundsummary;
if CRSP_OBJ_CD in ("EDYG","EDYB","EDYI");
year=year(CALDT);
proc sort; by crsp_fundno year;
run;

data fundno;
set fundsum;
keep crsp_fundno;
proc sort; by crsp_fundno;
run;

data monthret;
set data.monthlyreturn;
year=year(caldt);
mret=log(1+mret);
if mret ne .;
proc sort; by crsp_fundno year;
run;


proc means data=monthret noprint;
by crsp_fundno year;
var mret;
output out=annret(drop=_type_) sum=annret;
run;

data annret;
set annret;
if _freq_=12;
annret=exp(annret)-1;
run;

data fundsum;
merge fundsum(in=in1) annret(in=in2);
by crsp_fundno year;
if in1 and in2;
run;

**calculate total fund amount;
proc sql;
   create table fund_count as 
     select count(distinct(CRSP_FUNDNO)) as fundno from fundsum;
quit;

**calculate average fund amount, average TNA and average Expense Ratio;
proc sql;
   create table summary as 
     select year,count(distinct(CRSP_FUNDNO)) as fundno , mean(TNA_LATEST) as tna_mean, mean(EXP_RATIO) as exp_mean from fundsum group by year;
quit;

proc means data=summary noprint;
var fundno tna_mean exp_mean;
output out=avg_count_tna_exp(drop=_type_) mean=avg_count avg_tna avg_exp;
run;

**calculate flow&Mturn;
data fundsum1;
set fundsum;
year=year+1;
ptna=TNA_LATEST;
keep year CRSP_FUNDNO ptna annret;
proc sort; by CRSP_FUNDNO year;
run;


data for_flow;
merge fundsum(in=in1) fundsum1(in=in2);
by CRSP_FUNDNO year;
if in1 and in2;
keep CRSP_FUNDNO year TNA_LATEST ptna annret TURN_RATIO;
run;

data for_flow;
set for_flow;
flow=(TNA_LATEST-ptna*(1+annret))/ptna;
Mturn=TURN_RATIO+0.5*flow;
run;

proc sql;
   create table flow_cros_avg as 
     select year,mean(flow) as flow_mean, mean(Mturn) as Mturn_mean from for_flow group by year;
quit;

proc means data=flow_cros_avg noprint;
var flow_mean Mturn_mean;
output out=avg_flow_Mturn(drop=_type_) mean=avg_flow avg_Mturn;
run;


**load calculation;
data rearload;
set data.rearload;
if rear_load^=-99;
year=year(BEGDT);
if rear_load=. then rear_load=0;
keep crsp_fundno year rear_load;
proc sort; by crsp_fundno year;
run;

data frontload;
set data.frontload;
if front_load^=-99;
year=year(BEGDT);
if front_load=. then front_load=0;
keep crsp_fundno year front_load;
proc sort; by crsp_fundno year;
run;

proc means data=rearload noprint;
by crsp_fundno year;
var rear_load;
output out=rear sum=rear_ann;
run;

proc means data=frontload noprint;
by crsp_fundno year;
var front_load;
output out=front sum=front_ann;
run;

data load;
merge rear front;
by crsp_fundno year;
if rear_ann=. then rear_ann=0;
if front_ann=. then front_ann=0;
load=rear_ann+front_ann;
run;

data load;
merge fundno(in=in1) load;
by crsp_fundno;
if in1;
run;

proc means data=load noprint;
by crsp_fundno;
var load;
output out=totalload sum=load_sum;
run;

data withload;
set totalload;
if load_sum^=0;
run;

proc sql;
    create table load_count as 
     select count(CRSP_FUNDNO) as load_count from withload;
    create table load_pct as
     select fundno,load_count from fund_count, load_count;
quit;

data load_pct;
set load_pct;
load_pct=load_count/fundno;
run;

data load;
set load;
if load ne 0;
proc sort; by year;
run;

proc means data=load noprint;
by year;
var load;
output out=cros_load mean=load_mean;
run;

proc means data=cros_load noprint;
var load_mean;
output out=avg_load mean=avg_load;
run;

**calculate age;
data age;
set fundsum;
if first_offer_dt^=.;
by crsp_fundno;
if last.crsp_fundno=1;
ldate=caldt;
format ldate Date9.;
fdate = Input( Put( first_offer_dt, yymmdd8.), yymmdd8.);
format fdate Date9.;
age=intck('day',fdate,ldate)/365;
keep crsp_fundno fdate ldate age;
run;

proc means data=age noprint;
var age;
output out=avg_age mean=avg_age;
run;

**replicate table 1;
proc sql;
   create table table1 as 
     select  fund_count.fundno,avg_count,avg_tna,avg_flow,
     avg_exp,avg_Mturn,load_pct,avg_load,avg_age
     from fund_count,avg_count_tna_exp,avg_flow_mturn,load_pct,avg_load,avg_age;
quit;


**start replicating table3;
data ff;
set data.factors_monthly;
keep year month mktrf smb hml umd rf;
run;

proc sort data=annret;
by year;
run;

**Calculate rank according to previous year's return;
proc rank data=annret out=retrank groups=10;
by year;
var annret;
ranks aretrank;
run;

data retrank;
set retrank;
year=year+1;
prank=aretrank;
keep crsp_fundno year annret prank;
proc sort;by crsp_fundno year;
run;

data top;
set retrank;
if prank=9;
proc sort;by year;
run;

data bot;
set retrank;
if prank=0;
proc sort;by year;
run;

proc rank data=top out=toprank groups=3;
by year;
var annret;
ranks trank;
run;

proc sort data=toprank;
by crsp_fundno year;
run;

proc rank data=bot out=botrank groups=3;
by year;
var annret;
ranks brank;
run;

proc sort data=botrank;
by crsp_fundno year;
run;


**Calculate monthly portfolio return;
data mret1;
merge monthret(in=in1) retrank(in=in2);
by crsp_fundno year;
if in1 and in2;
month=month(caldt);
keep crsp_fundno year month mret prank;
proc sort; by year month prank;
run;

data mrettop;
merge monthret(in=in1) toprank(in=in2);
by crsp_fundno year;
if in1 and in2;
month=month(caldt);
keep crsp_fundno year month mret trank;
proc sort; by year month trank;
run;

data mretbot;
merge monthret(in=in1) botrank(in=in2);
by crsp_fundno year;
if in1 and in2;
month=month(caldt);
keep crsp_fundno year month mret brank;
proc sort; by year month brank;
run;

proc means data=mret1 noprint;
by year month prank;
var mret;
output out=portret1(drop=_type_) mean=portret;
run;

proc means data=mrettop noprint;
by year month trank;
var mret;
output out=portrettop(drop=_type_) mean=portret;
run;

proc means data=mretbot noprint;
by year month brank;
var mret;
output out=portretbot(drop=_type_) mean=portret;
run;

**CAPM market return;
data vw;
set data.crspvw;
year=year(date);
month=month(date);
keep year month vwretx;
run;

**Prepare dataset for regression;
data reg_10;
merge portret1(in=in1) vw(in=in2) ff(in=in3);
by year month;
if in1 and in2 and in3;
vwrf=vwretx-rf;
proc sort; by prank;
run;

proc means data=reg_10 noprint;
by prank;
var portret;
output out=stat10 mean=portret_avg std=portret_std;
run;

data reg_top;
merge portrettop(in=in1) vw(in=in2) ff(in=in3);
by year month;
if in1 and in2 and in3;
vwrf=vwretx-rf;
proc sort; by trank;
run;

proc means data=reg_top noprint;
by trank;
var portret;
output out=stattop mean=portret_avg std=portret_std;
run;

data reg_bot;
merge portretbot(in=in1) vw(in=in2) ff(in=in3);
by year month;
if in1 and in2 and in3;
vwrf=vwretx-rf;
proc sort; by brank;
run;

proc means data=reg_bot noprint;
by brank;
var portret;
output out=statbot mean=portret_avg std=portret_std;
run;

**Perform Regressions;
proc reg data=reg_10 outest=capm tableout noprint;
by prank;
model portret=vwrf /adjrsq;
run;

proc reg data=reg_10 outest=ff4 tableout noprint;
by prank;
model portret=mktrf smb hml umd /adjrsq;
run;

proc reg data=reg_top outest=capm_top tableout noprint;
by trank;
model portret=vwrf /adjrsq;
run;

proc reg data=reg_top outest=ff4_top tableout noprint;
by trank;
model portret=mktrf smb hml umd /adjrsq;
run;

proc reg data=reg_bot outest=capm_bot tableout noprint;
by brank;
model portret=vwrf /adjrsq;
run;

proc reg data=reg_bot outest=ff4_bot tableout noprint;
by brank;
model portret=mktrf smb hml umd /adjrsq;
run;

**1-10 spread;
proc means data=mrettop noprint;
by year month;
var mret;
output out=topret(drop=_type_) mean=topret;
run;

proc means data=mretbot noprint;
by year month;
var mret;
output out=botret(drop=_type_) mean=botret;
run;

data tbdiff;
merge topret(in=in1) botret(in=in2);
by year month;
if in1 and in2;
tbdiff=topret-botret;
run;

data reg_tbdiff;
merge tbdiff(in=in1) vw(in=in2) ff(in=in3);
by year month;
if in1 and in2 and in3;
vwrf=vwretx-rf;
run;

proc means data=reg_tbdiff noprint;
var tbdiff;
output out=stattbdiff mean=tbdiff_avg std=tbdiff_std;
run;

proc reg data=reg_tbdiff outest=capm_tbdiff tableout noprint;
model tbdiff=vwrf /adjrsq;
run;

proc reg data=reg_tbdiff outest=ff4_tbdiff tableout noprint;
model tbdiff=mktrf smb hml umd /adjrsq;
run;


**1A-10C spread;
data topa;
set portrettop;
if trank=2;
toparet=portret;
keep year month toparet;
run;

data botc;
set portretbot;
if brank=0;
botcret=portret;
keep year month botcret;
run;

data reg_tabcdiff;
merge topa(in=in1) botc(in=in2) vw(in=in3) ff(in=in4);
by year month;
if in1 and in2 and in3 and in4;
vwrf=vwretx-rf;
tabcdiff=toparet-botcret;
run;

proc means data=reg_tabcdiff noprint;
var tabcdiff;
output out=stattabcdiff mean=tabcdiff_avg std=tabcdiff_std;
run;

proc reg data=reg_tabcdiff outest=capm_tabcdiff tableout noprint;
model tabcdiff=vwrf /adjrsq;
run;

proc reg data=reg_tabcdiff outest=ff4_tabcdiff tableout noprint;
model tabcdiff=mktrf smb hml umd /adjrsq;
run;


**9-10 spread;
data sbot;
set portret1;
if prank=1;
sbotret=portret;
keep year month sbotret;
run;

data reg_bots;
merge sbot(in=in1) botret(in=in2) vw(in=in3) ff(in=in4);
by year month;
if in1 and in2 and in3 and in4;
vwrf=vwretx-rf;
botsdiff=sbotret-botret;
run;

proc means data=reg_bots noprint;
var botsdiff;
output out=statbotsdiff mean=botsdiff_avg std=botsdiff_std;
run;

proc reg data=reg_bots outest=capm_botsdiff tableout noprint;
model botsdiff=vwrf /adjrsq;
run;

proc reg data=reg_bots outest=ff4_botsdiff tableout noprint;
model botsdiff=mktrf smb hml umd /adjrsq;
run;

**end;















