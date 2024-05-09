
/********** OLAP DATABASE *******************
DROP DATABASE IF EXISTS chinook_olap;
CREATE DATABASE chinook_olap;
*******************************************/

/**************** CUSTOMER_DIMENSION *************************
drop table if exists chinook_olap.customer_dim;
create table chinook_olap.customer_dim as (
select * from chinook_oltp.customer
    );

ALTER TABLE chinook_olap.customer_dim
	-- DECLARING PRIMARY KEY
	ADD PRIMARY KEY(CustomerId);
    
ALTER TABLE chinook_olap.customer_dim
-- FOR CHANGE DATA CAPTURE LATER
ADD COLUMN `Created_at` DATETIME NULL DEFAULT '2000-01-01 00:00:00',
ADD COLUMN `Updated_at` DATETIME NULL DEFAULT '2000-01-01 00:00:00';

select * from chinook_olap.customer_dim;

*******************************************************************/

/***************** TRACK_DIMENSION ***********************************
drop table if exists chinook_olap.track_dim;
	create table chinook_olap.track_dim as (
		select * from chinook_oltp.track
		left join chinook_oltp.album using (albumid)
    );

 ALTER TABLE chinook_olap.track_dim
    
    -- DECLARING PRIMARY KEY
	ADD PRIMARY KEY(TrackId),
	
    -- FOR CHANGE DATA CAPTURE LATER
    ADD COLUMN `Created_at` DATETIME NULL DEFAULT '2000-01-01 00:00:00',
	ADD COLUMN `Updated_at` DATETIME NULL DEFAULT '2000-01-01 00:00:00';

select * from chinook_olap.track_dim;    
************************************************************************/
/*********************** DATE_DIMENSION_PROCEDURE **********************************
DELIMITER //
DROP PROCEDURE IF EXISTS chinook_olap.create_dim_date //
CREATE PROCEDURE chinook_olap.create_dim_date(IN start_date DATE, IN end_date DATE)
BEGIN

	-- Credit to http://www.dwhworld.com/2010/08/date-dimension-sql-scripts-mysql/
	-- Small-numbers table

	DROP TABLE IF EXISTS chinook_olap.numbers_small;
	CREATE TABLE chinook_olap.numbers_small (number INT);
	INSERT INTO chinook_olap.numbers_small VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9);

	-- Main-numbers table
	DROP TABLE IF EXISTS chinook_olap.numbers;
	CREATE TABLE chinook_olap.numbers (number BIGINT);
	INSERT INTO chinook_olap.numbers
	SELECT thousands.number * 1000 + hundreds.number * 100 + tens.number * 10 + ones.number
	FROM chinook_olap.numbers_small thousands, chinook_olap.numbers_small hundreds, chinook_olap.numbers_small tens, chinook_olap.numbers_small ones
	LIMIT 1000000;

	-- Create Date Dimension table
	DROP TABLE IF EXISTS chinook_olap.date_dim;
	CREATE TABLE chinook_olap.date_dim (
	date_id          BIGINT PRIMARY KEY,
	date             DATE NOT NULL,
	year             INT,
	month            CHAR(10),
	month_of_year    CHAR(2),
	day_of_month     INT,
	day              CHAR(10),
	day_of_week      INT,
	weekend          CHAR(10) NOT NULL DEFAULT "Weekday",
	day_of_year      INT,
	week_of_year     CHAR(2),
	quarter  INT,
	previous_day     date ,
	next_day         date ,
	UNIQUE KEY `date` (`date`));

	-- First populate with ids and Date
	-- Change year start and end to match your needs. The above sql creates records for year 2010.
	INSERT INTO chinook_olap.date_dim (date_id, date)
	SELECT year(DATE_ADD( start_date, INTERVAL number DAY ))*10000+
	month(DATE_ADD( start_date, INTERVAL number DAY ))*100+
	day(DATE_ADD( start_date, INTERVAL number DAY ))date_id
	, 

	DATE_ADD( start_date, INTERVAL number DAY )
	FROM chinook_olap.numbers
	WHERE DATE_ADD( start_date, INTERVAL number DAY ) BETWEEN start_date AND end_date
	ORDER BY number;
	SET SQL_SAFE_UPDATES = 0;
	-- Update other columns based on the date.
	UPDATE chinook_olap.date_dim SET
	year            = DATE_FORMAT( date, "%Y" ),
	month           = DATE_FORMAT( date, "%M"),
	month_of_year   = DATE_FORMAT( date, "%m"),
	day_of_month    = DATE_FORMAT( date, "%d" ),
	day             = DATE_FORMAT( date, "%W" ),
	day_of_week     = DAYOFWEEK(date),
	weekend         = IF( DATE_FORMAT( date, "%W" ) IN ('Saturday','Sunday'), 'Weekend', 'Weekday'),
	day_of_year     = DATE_FORMAT( date, "%j" ),
	week_of_year    = DATE_FORMAT( date, "%V" ),
	quarter         = QUARTER(date),
	previous_day    = DATE_ADD(date, INTERVAL -1 DAY),
	next_day        = DATE_ADD(date, INTERVAL 1 DAY);

	drop table if exists chinook_olap.numbers;
	drop table if exists chinook_olap.numbers_small;
    
END //
DELIMITER ;

-- TO CHECK MIN & MAX DATE FOR RANGE
select min(InvoiceDate), max(InvoiceDate) from invoice;

call chinook_olap.create_dim_date('2008-01-01 00:00:00', '2014-12-22 00:00:00');

select min(date),max(date) from chinook_olap.date_dim;
************************************************************************************************/
/********************************************************************************
use chinook_olap;

-- SET foreign_key_checks = 0;

-- ALTER TABLE `classicmodels_olap`.`order_fact` DROP FOREIGN KEY `PROD_ID_FK`;

ALTER TABLE `chinook_oltp`.`invoice` 
CHANGE COLUMN `InvoiceDate` `InvoiceDate` DATETIME NOT NULL ;



ALTER TABLE chinook_olap.customer_dim
-- MAKING THEM NULLABLE FOR TESTING PURPOSE
CHANGE COLUMN `FirstName` `FirstName` VARCHAR(40) NOT NULL ,
CHANGE COLUMN `LastName` `LastName` VARCHAR(20) NOT NULL,
CHANGE COLUMN `Company` `Company` VARCHAR(80) NULL ,
CHANGE COLUMN `Address` `Address` VARCHAR(70) NULL ,
CHANGE COLUMN `City` `City` VARCHAR(40) NULL,
CHANGE COLUMN `State` `State` VARCHAR(40) NULL,
CHANGE COLUMN `Country` `Country` VARCHAR(40) NULL,
CHANGE COLUMN `PostalCode` `PostalCode` VARCHAR(10) NULL,
CHANGE COLUMN `Phone` `Phone` VARCHAR(24) NULL,
CHANGE COLUMN `Fax` `Fax` VARCHAR(24) NULL ,
CHANGE COLUMN `Email` `Email` VARCHAR(60) NULL ,
CHANGE COLUMN `SupportRepId` `SupportRepId` INT NULL;

-- WORKING OUT SCD 1 IMPLEMENTATION

select * from customer_dim;

drop table if exists customer_dim_scd1;
create table customer_dim_scd1 as select * from customer_dim;

select * from customer_dim_scd1;

ALTER TABLE `chinook_olap`.`customer_dim_scd1` 
ADD PRIMARY KEY (`CustomerId`); -- BECAUSE OF SCD1 THERE IS NO SURROGATE KEY

select * from customer_dim_scd1;

select * from customer_dim_scd1 where CustomerId = '1';
select * from customer_dim where CustomerId = '1';

update customer_dim set PostalCode='12238-000' , updated_at=now() where CustomerId='1' ;

update customer_dim_scd1 cdscd1
left join customer_dim cd on cdscd1.CustomerId=cd.CustomerId
set cdscd1.updated_at=cd.updated_at, cdscd1.PostalCode=cd.PostalCode
where 
cd.updated_at>cd.created_at -- THOSE RECORDS WHICH WERE UPDATED POST CREATION #CHANGE_DIMENSION
and (cd.PostalCode!=cdscd1.PostalCode ) -- THESE ATTRIBUTES WILL BE TRACKED WHEN UPDATING THE SCD AGAINST PRODUCTCODE
;

*****************************************************************************************************************/
-- FOR ABOVE UPDATE WE CAN CREATE A PROCEDURE AND CALL IT INSIDE A AFTER UPDATE TRIGGER IN MYSQL
/************************* FACT_TABLE ******************************************************
DELIMITER //

DROP PROCEDURE IF EXISTS chinook_olap.create_fact_Invoice //
CREATE PROCEDURE chinook_olap.create_fact_Invoice()
BEGIN
DROP TABLE IF EXISTS chinook_olap.Invoice_fact_scd1;
    
create table chinook_olap.Invoice_fact_scd1 as (
select Il.InvoiceLineId
			,year(I.InvoiceDate)*10000+month(I.InvoiceDate)*100+day(I.InvoiceDate) as SaleDate -- FORMATTED DATE WILL BE USED TO LINK DATE DIMENSION
		,I.InvoiceId,C.CustomerId,T.TrackId -- OUR DIMENSION REPRESENTATIVES
        ,Il.Quantity Total_Order_Quantity,Il.Unitprice*Il.quantity Total_Sale_Amount -- OUR FACT MEMBERS
        from chinook_oltp.invoiceline IL
		left join chinook_oltp.invoice I using (InvoiceId) -- TO CREATE SINGLE FACT
		left join chinook_olap.customer_dim C using (CustomerId) -- TO LINK PRODUCT DIMENSION
		left join chinook_olap.track_dim T using (TrackId) -- TO LINK Track DIMENSION
        );
-- CREATING RELATIONSHIPS FOR DIMENSIONAL MODEL
    ALTER TABLE chinook_olap.Invoice_fact_scd1 
	
    MODIFY COLUMN SaleDate BIGINT(20), -- TO ALIGN WITH DATE_DIM DATE_ID KEY DATA TYPE

    ADD PRIMARY KEY(InvoiceId,TrackId),
    ADD CONSTRAINT `CUST_ID_FK_SCD1` foreign key(`customerId`) references chinook_olap.customer_dim_scd1(CustomerId) ON DELETE NO ACTION,
    ADD CONSTRAINT `TRACK_ID_FK_SCD1` foreign key(`TrackId`) references chinook_olap.track_dim(TrackId) ,
    ADD CONSTRAINT `SALEDATE_ID_FK_SCD1` foreign key(`SaleDate`) references chinook_olap.date_dim(date_id) ON DELETE NO ACTION
    ;
    
END //

DELIMITER ;

call chinook_olap.create_fact_Invoice();

select * from chinook_olap.Invoice_fact_scd1
left join chinook_olap.customer_dim_scd1  using (Customerid)
 where Customerid = '1';

*****************************************************************************************/
/***************** CREATE VIEW ****************************
DROP VIEW IF EXISTS Chinook_Datamart;
CREATE OR REPLACE VIEW Chinook_Datamart AS
SELECT
    IFNULL(F.InvoiceId, F.TrackId) AS PrimaryKey, -- Use a common primary key
    F.InvoiceLineId,
    D.date AS SaleDate,
    F.InvoiceId,
    CD.CustomerId, -- Corrected alias to CD
    TD.TrackId,
    F.Total_Order_Quantity,
    F.Total_Sale_Amount,
    CD.FirstName,
    CD.LastName,
    CD.Company,
    CD.Address,
    CD.City,
    CD.State,
    CD.Country,
    CD.PostalCode,
    CD.Phone,
    CD.Fax,
    CD.Email,
    CD.SupportRepId,
    TD.ArtistId,
    TD.Updated_at,
    TD.AlbumId,
    TD.MediaTypeId,
    TD.GenreId,
    D.year,
    D.month,
    D.month_of_year,
    D.day_of_month,
    D.day,
    D.day_of_week,
    D.weekend,
    D.day_of_year,
    D.week_of_year,
    D.quarter,
    D.previous_day,
    D.next_day
FROM
    chinook_olap.Invoice_fact_scd1 AS F
LEFT JOIN
    chinook_olap.customer_dim_scd1 AS CD ON F.CustomerId = CD.CustomerId
LEFT JOIN
    chinook_olap.track_dim AS TD ON F.TrackId = TD.TrackId
LEFT JOIN
    chinook_olap.date_dim AS D ON F.SaleDate = D.date_id;

-- Testing of view 
SELECT *
FROM Chinook_Datamart;

*******************************************************************/












