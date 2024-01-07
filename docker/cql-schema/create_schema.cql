CREATE KEYSPACE IF NOT EXISTS srds
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

USE srds;

CREATE TABLE IF NOT EXISTS UprawnieniObywatele
(
    okreg       int,
    idObywatela uuid,
    glosDoSenatu boolean,
    glosDoSejmu boolean,
    PRIMARY KEY (okreg, idObywatela)
);

CREATE TABLE IF NOT EXISTS Frequency
(
    voteTimeStamp timestamp,
    frequency int,
    PRIMARY KEY (voteTimeStamp, frequency)
);

CREATE TABLE IF NOT EXISTS SenatWyniki
(
    okreg int,
    idKandydata uuid,
    imie varchar,
    nazwisko varchar,
    votes counter,
    PRIMARY KEY ( okreg, idKandydata, imie, nazwisko )
);

CREATE TABLE IF NOT EXISTS SejmWyniki
(
    okreg int,
    idKandydata uuid,
    imie varchar,
    nazwisko varchar,
    votes counter,
    PRIMARY KEY ( okreg, idKandydata, imie, nazwisko )
);
