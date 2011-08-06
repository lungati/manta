.mode csv
.header on

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
