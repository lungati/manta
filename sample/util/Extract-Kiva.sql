.mode csv
.header off

create table Client (clientid,name,groupid,officerid,balance,phone_number,nationalid);
.import Client.csv Client

create table "Group" (groupid,name,officerid,officeid);
.import Group.csv Group

create table Officer (officerid,name,officeid);
.import Officer.csv Officer

create table "Transaction" (entryid,clientid,officerid,posting_date,documentid,description,amount,transaction_type);
.import Transaction.csv Transaction

create table Loan (loanid,application_date,clientid,officerid,status,issued_date,installments,disbursement_date,amount,payment_due,balance,principal_arrears_30,principal_arrears_90,principal_arrears_180,principal_arrears_over180,arrears,grace_period,grace_period_pays_interest,interest_rate,interest_method,funds_source);
.import Loan.csv Loan

update Client set phone_number = "07" || (abs(random()) % 100000000);
update Client set nationalid = (abs(random()) % 10000000);

create table Loan_Kiva as select distinct Loan.* from Loan where funds_source = "KIVA";
create table Client_Kiva as select distinct Client.* from Client join Loan_Kiva using (clientid);
create table Transaction_Kiva as select distinct "Transaction".* from "Transaction" join Client_Kiva using (clientid);
create table Group_Kiva as select distinct "Group".* from "Group" join Client_Kiva using (groupid);

.output Kiva/Client.csv
select * from Client_Kiva;
.output stdout

.output Kiva/Group.csv
select * from Group_Kiva;
.output stdout

.output Kiva/Officer.csv
select * from Officer;
.output stdout

.output Kiva/Transaction.csv
select * from Transaction_Kiva;
.output stdout

.output Kiva/Loan.csv
select * from Loan_Kiva;
.output stdout

