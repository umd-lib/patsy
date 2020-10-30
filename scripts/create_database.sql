--  Simple database creation for local development
create user patsy;

drop database if exists patsy;
create database patsy;
grant all privileges on database patsy to patsy;
