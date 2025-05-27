/*
======================================================
Create database and schemas
======================================================

This script creates the database 'DataWarehouse' and the schemas 'bronze', 'silver' and 'gold'. 
The script checks if there is any database with that name in order to drop it and delete everything in it before creating 
everythig else
*/
USE master;
GO

-- Drop and recreate the DataWarehouse database

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse
END;
GO 

-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
