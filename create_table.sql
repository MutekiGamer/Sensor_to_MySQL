create table measurement 
(
M_ID INT,
primary key(M_ID),
Measurement VARCHAR(255),
Unit VARCHAR(255),
Description VARCHAR(255) 
)
ENGINE=INNODB;

SELECT * FROM localdata.measurement;
SHOW VARIABLES LIKE "secure_file_priv";
#TRUNCATE TABLE measurement; 
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/measurement.csv'
INTO TABLE measurement
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;




CREATE TABLE sensor_data
(
	Time_EST datetime,
	Yr INT,
	Sensor_reading decimal(10,2),
    M_ID INT,
	foreign key (M_ID) references measurement(M_ID)
)
ENGINE=INNODB;