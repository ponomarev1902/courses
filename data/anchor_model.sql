-- DATABASE INITIALIZATION -----------------------------------------------------
--
-- The following code performs the initial setup of the PostgreSQL database with
-- required objects for the anchor database.
--
--------------------------------------------------------------------------------
-- create schema
CREATE SCHEMA IF NOT EXISTS dbo;
-- set schema search path
SET search_path = dbo;
-- drop universal function that generates checksum values
-- DROP FUNCTION IF EXISTS dbo.generateChecksum(text);
-- create universal function that generates checksum values
CREATE OR REPLACE FUNCTION dbo.generateChecksum(
    value text
) RETURNS bytea AS '
    BEGIN
        return cast(
            substring(
                MD5(value) for 16
            ) as bytea
        );
    END;
' LANGUAGE plpgsql;
-- KNOTS --------------------------------------------------------------------------------------------------------------
--
-- Knots are used to store finite sets of values, normally used to describe states
-- of entities (through knotted attributes) or relationships (through knotted ties).
-- Knots have their own surrogate identities and are therefore immutable.
-- Values can be added to the set over time though.
-- Knots should have values that are mutually exclusive and exhaustive.
-- Knots are unfolded when using equivalence.
--
-- Knot table ---------------------------------------------------------------------------------------------------------
-- AUT_IsAutoCourseEnrole table
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._AUT_IsAutoCourseEnrole;
CREATE TABLE IF NOT EXISTS dbo._AUT_IsAutoCourseEnrole (
    AUT_ID int not null,
    AUT_IsAutoCourseEnrole bool not null,
    Metadata_AUT int not null,
    constraint pkAUT_IsAutoCourseEnrole primary key (
        AUT_ID
    ),
    constraint uqAUT_IsAutoCourseEnrole unique (
        AUT_IsAutoCourseEnrole
    )
);
ALTER TABLE IF EXISTS ONLY dbo._AUT_IsAutoCourseEnrole CLUSTER ON pkAUT_IsAutoCourseEnrole;
-- DROP VIEW IF EXISTS dbo.AUT_IsAutoCourseEnrole;
CREATE OR REPLACE VIEW dbo.AUT_IsAutoCourseEnrole AS SELECT
    AUT_ID,
    AUT_IsAutoCourseEnrole,
    Metadata_AUT
FROM dbo._AUT_IsAutoCourseEnrole;
-- Knot table ---------------------------------------------------------------------------------------------------------
-- DEM_IsDemoEnrole table
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._DEM_IsDemoEnrole;
CREATE TABLE IF NOT EXISTS dbo._DEM_IsDemoEnrole (
    DEM_ID int not null,
    DEM_IsDemoEnrole bool not null,
    Metadata_DEM int not null,
    constraint pkDEM_IsDemoEnrole primary key (
        DEM_ID
    ),
    constraint uqDEM_IsDemoEnrole unique (
        DEM_IsDemoEnrole
    )
);
ALTER TABLE IF EXISTS ONLY dbo._DEM_IsDemoEnrole CLUSTER ON pkDEM_IsDemoEnrole;
-- DROP VIEW IF EXISTS dbo.DEM_IsDemoEnrole;
CREATE OR REPLACE VIEW dbo.DEM_IsDemoEnrole AS SELECT
    DEM_ID,
    DEM_IsDemoEnrole,
    Metadata_DEM
FROM dbo._DEM_IsDemoEnrole;
-- Knot table ---------------------------------------------------------------------------------------------------------
-- OPE_IsOpen table
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._OPE_IsOpen;
CREATE TABLE IF NOT EXISTS dbo._OPE_IsOpen (
    OPE_ID int not null,
    OPE_IsOpen bool not null,
    Metadata_OPE int not null,
    constraint pkOPE_IsOpen primary key (
        OPE_ID
    ),
    constraint uqOPE_IsOpen unique (
        OPE_IsOpen
    )
);
ALTER TABLE IF EXISTS ONLY dbo._OPE_IsOpen CLUSTER ON pkOPE_IsOpen;
-- DROP VIEW IF EXISTS dbo.OPE_IsOpen;
CREATE OR REPLACE VIEW dbo.OPE_IsOpen AS SELECT
    OPE_ID,
    OPE_IsOpen,
    Metadata_OPE
FROM dbo._OPE_IsOpen;
-- KNOT TRIGGERS ---------------------------------------------------------------------------------------------------
--
-- The following triggers enable calculation and storing checksum values.
--
-- ANCHORS AND ATTRIBUTES ---------------------------------------------------------------------------------------------
--
-- Anchors are used to store the identities of entities.
-- Anchors are immutable.
-- Attributes are used to store values for properties of entities.
-- Attributes are mutable, their values may change over one or more types of time.
-- Attributes have four flavors: static, historized, knotted static, and knotted historized.
-- Anchors may have zero or more adjoined attributes.
--
-- Anchor table -------------------------------------------------------------------------------------------------------
-- CO_Course table (with 7 attributes)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._CO_Course;
CREATE TABLE IF NOT EXISTS dbo._CO_Course (
    CO_ID serial not null, 
    Metadata_CO int not null, 
    constraint pkCO_Course primary key (
        CO_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._CO_Course CLUSTER ON pkCO_Course;
-- DROP VIEW IF EXISTS dbo.CO_Course;
CREATE OR REPLACE VIEW dbo.CO_Course AS SELECT 
    CO_ID,
    Metadata_CO 
FROM dbo._CO_Course;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- CO_TIT_Course_Title table (on CO_Course)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._CO_TIT_Course_Title;
CREATE TABLE IF NOT EXISTS dbo._CO_TIT_Course_Title (
    CO_TIT_CO_ID int not null,
    CO_TIT_Course_Title varchar(255) not null,
    CO_TIT_ChangedAt timestamp not null,
    Metadata_CO_TIT int not null,
    constraint fkCO_TIT_Course_Title foreign key (
        CO_TIT_CO_ID
    ) references dbo._CO_Course(CO_ID),
    constraint pkCO_TIT_Course_Title primary key (
        CO_TIT_CO_ID,
        CO_TIT_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._CO_TIT_Course_Title CLUSTER ON pkCO_TIT_Course_Title;
-- DROP VIEW IF EXISTS dbo.CO_TIT_Course_Title;
CREATE OR REPLACE VIEW dbo.CO_TIT_Course_Title AS SELECT
    CO_TIT_CO_ID,
    CO_TIT_Course_Title,
    CO_TIT_ChangedAt,
    Metadata_CO_TIT
FROM dbo._CO_TIT_Course_Title;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- CO_ICO_Course_IconUrl table (on CO_Course)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._CO_ICO_Course_IconUrl;
CREATE TABLE IF NOT EXISTS dbo._CO_ICO_Course_IconUrl (
    CO_ICO_CO_ID int not null,
    CO_ICO_Course_IconUrl varchar(255) not null,
    CO_ICO_ChangedAt timestamp not null,
    Metadata_CO_ICO int not null,
    constraint fkCO_ICO_Course_IconUrl foreign key (
        CO_ICO_CO_ID
    ) references dbo._CO_Course(CO_ID),
    constraint pkCO_ICO_Course_IconUrl primary key (
        CO_ICO_CO_ID,
        CO_ICO_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._CO_ICO_Course_IconUrl CLUSTER ON pkCO_ICO_Course_IconUrl;
-- DROP VIEW IF EXISTS dbo.CO_ICO_Course_IconUrl;
CREATE OR REPLACE VIEW dbo.CO_ICO_Course_IconUrl AS SELECT
    CO_ICO_CO_ID,
    CO_ICO_Course_IconUrl,
    CO_ICO_ChangedAt,
    Metadata_CO_ICO
FROM dbo._CO_ICO_Course_IconUrl;
-- Static attribute table ---------------------------------------------------------------------------------------------
-- CO_DEL_Course_Deleted table (on CO_Course)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._CO_DEL_Course_Deleted;
CREATE TABLE IF NOT EXISTS dbo._CO_DEL_Course_Deleted (
    CO_DEL_CO_ID int not null,
    CO_DEL_Course_Deleted timestamp(0) not null,
    Metadata_CO_DEL int not null,
    constraint fkCO_DEL_Course_Deleted foreign key (
        CO_DEL_CO_ID
    ) references dbo._CO_Course(CO_ID),
    constraint pkCO_DEL_Course_Deleted primary key (
        CO_DEL_CO_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._CO_DEL_Course_Deleted CLUSTER ON pkCO_DEL_Course_Deleted;
-- DROP VIEW IF EXISTS dbo.CO_DEL_Course_Deleted;
CREATE OR REPLACE VIEW dbo.CO_DEL_Course_Deleted AS SELECT
    CO_DEL_CO_ID,
    CO_DEL_Course_Deleted,
    Metadata_CO_DEL
FROM dbo._CO_DEL_Course_Deleted;
-- Static attribute table ---------------------------------------------------------------------------------------------
-- CO_CRE_Course_Created table (on CO_Course)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._CO_CRE_Course_Created;
CREATE TABLE IF NOT EXISTS dbo._CO_CRE_Course_Created (
    CO_CRE_CO_ID int not null,
    CO_CRE_Course_Created timestamp not null,
    Metadata_CO_CRE int not null,
    constraint fkCO_CRE_Course_Created foreign key (
        CO_CRE_CO_ID
    ) references dbo._CO_Course(CO_ID),
    constraint pkCO_CRE_Course_Created primary key (
        CO_CRE_CO_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._CO_CRE_Course_Created CLUSTER ON pkCO_CRE_Course_Created;
-- DROP VIEW IF EXISTS dbo.CO_CRE_Course_Created;
CREATE OR REPLACE VIEW dbo.CO_CRE_Course_Created AS SELECT
    CO_CRE_CO_ID,
    CO_CRE_Course_Created,
    Metadata_CO_CRE
FROM dbo._CO_CRE_Course_Created;
-- Knotted static attribute table -------------------------------------------------------------------------------------
-- CO_AUT_Course_IsAutoCourseEnrole table (on CO_Course)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._CO_AUT_Course_IsAutoCourseEnrole;
CREATE TABLE IF NOT EXISTS dbo._CO_AUT_Course_IsAutoCourseEnrole (
    CO_AUT_CO_ID int not null,
    CO_AUT_AUT_ID int not null,
    Metadata_CO_AUT int not null,
    constraint fk_A_CO_AUT_Course_IsAutoCourseEnrole foreign key (
        CO_AUT_CO_ID
    ) references dbo._CO_Course(CO_ID),
    constraint fk_K_CO_AUT_Course_IsAutoCourseEnrole foreign key (
        CO_AUT_AUT_ID
    ) references dbo._AUT_IsAutoCourseEnrole(AUT_ID),
    constraint pkCO_AUT_Course_IsAutoCourseEnrole primary key (
        CO_AUT_CO_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._CO_AUT_Course_IsAutoCourseEnrole CLUSTER ON pkCO_AUT_Course_IsAutoCourseEnrole;
-- DROP VIEW IF EXISTS dbo.CO_AUT_Course_IsAutoCourseEnrole;
CREATE OR REPLACE VIEW dbo.CO_AUT_Course_IsAutoCourseEnrole AS SELECT
    CO_AUT_CO_ID,
    CO_AUT_AUT_ID,
    Metadata_CO_AUT
FROM dbo._CO_AUT_Course_IsAutoCourseEnrole;
-- Knotted static attribute table -------------------------------------------------------------------------------------
-- CO_DEM_Course_IsDemoEnrole table (on CO_Course)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._CO_DEM_Course_IsDemoEnrole;
CREATE TABLE IF NOT EXISTS dbo._CO_DEM_Course_IsDemoEnrole (
    CO_DEM_CO_ID int not null,
    CO_DEM_DEM_ID int not null,
    Metadata_CO_DEM int not null,
    constraint fk_A_CO_DEM_Course_IsDemoEnrole foreign key (
        CO_DEM_CO_ID
    ) references dbo._CO_Course(CO_ID),
    constraint fk_K_CO_DEM_Course_IsDemoEnrole foreign key (
        CO_DEM_DEM_ID
    ) references dbo._DEM_IsDemoEnrole(DEM_ID),
    constraint pkCO_DEM_Course_IsDemoEnrole primary key (
        CO_DEM_CO_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._CO_DEM_Course_IsDemoEnrole CLUSTER ON pkCO_DEM_Course_IsDemoEnrole;
-- DROP VIEW IF EXISTS dbo.CO_DEM_Course_IsDemoEnrole;
CREATE OR REPLACE VIEW dbo.CO_DEM_Course_IsDemoEnrole AS SELECT
    CO_DEM_CO_ID,
    CO_DEM_DEM_ID,
    Metadata_CO_DEM
FROM dbo._CO_DEM_Course_IsDemoEnrole;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- CO_UPD_Course_UpdatedAt table (on CO_Course)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._CO_UPD_Course_UpdatedAt;
CREATE TABLE IF NOT EXISTS dbo._CO_UPD_Course_UpdatedAt (
    CO_UPD_CO_ID int not null,
    CO_UPD_Course_UpdatedAt timestamp not null,
    CO_UPD_ChangedAt timestamp not null,
    Metadata_CO_UPD int not null,
    constraint fkCO_UPD_Course_UpdatedAt foreign key (
        CO_UPD_CO_ID
    ) references dbo._CO_Course(CO_ID),
    constraint pkCO_UPD_Course_UpdatedAt primary key (
        CO_UPD_CO_ID,
        CO_UPD_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._CO_UPD_Course_UpdatedAt CLUSTER ON pkCO_UPD_Course_UpdatedAt;
-- DROP VIEW IF EXISTS dbo.CO_UPD_Course_UpdatedAt;
CREATE OR REPLACE VIEW dbo.CO_UPD_Course_UpdatedAt AS SELECT
    CO_UPD_CO_ID,
    CO_UPD_Course_UpdatedAt,
    CO_UPD_ChangedAt,
    Metadata_CO_UPD
FROM dbo._CO_UPD_Course_UpdatedAt;
-- Anchor table -------------------------------------------------------------------------------------------------------
-- ST_Stream table (with 8 attributes)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._ST_Stream;
CREATE TABLE IF NOT EXISTS dbo._ST_Stream (
    ST_ID serial not null, 
    Metadata_ST int not null, 
    constraint pkST_Stream primary key (
        ST_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._ST_Stream CLUSTER ON pkST_Stream;
-- DROP VIEW IF EXISTS dbo.ST_Stream;
CREATE OR REPLACE VIEW dbo.ST_Stream AS SELECT 
    ST_ID,
    Metadata_ST 
FROM dbo._ST_Stream;
-- Static attribute table ---------------------------------------------------------------------------------------------
-- ST_DEL_Stream_DeletedAt table (on ST_Stream)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._ST_DEL_Stream_DeletedAt;
CREATE TABLE IF NOT EXISTS dbo._ST_DEL_Stream_DeletedAt (
    ST_DEL_ST_ID int not null,
    ST_DEL_Stream_DeletedAt timestamp not null,
    Metadata_ST_DEL int not null,
    constraint fkST_DEL_Stream_DeletedAt foreign key (
        ST_DEL_ST_ID
    ) references dbo._ST_Stream(ST_ID),
    constraint pkST_DEL_Stream_DeletedAt primary key (
        ST_DEL_ST_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._ST_DEL_Stream_DeletedAt CLUSTER ON pkST_DEL_Stream_DeletedAt;
-- DROP VIEW IF EXISTS dbo.ST_DEL_Stream_DeletedAt;
CREATE OR REPLACE VIEW dbo.ST_DEL_Stream_DeletedAt AS SELECT
    ST_DEL_ST_ID,
    ST_DEL_Stream_DeletedAt,
    Metadata_ST_DEL
FROM dbo._ST_DEL_Stream_DeletedAt;
-- Static attribute table ---------------------------------------------------------------------------------------------
-- ST_CRE_Stream_CreatedAt table (on ST_Stream)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._ST_CRE_Stream_CreatedAt;
CREATE TABLE IF NOT EXISTS dbo._ST_CRE_Stream_CreatedAt (
    ST_CRE_ST_ID int not null,
    ST_CRE_Stream_CreatedAt timestamp not null,
    Metadata_ST_CRE int not null,
    constraint fkST_CRE_Stream_CreatedAt foreign key (
        ST_CRE_ST_ID
    ) references dbo._ST_Stream(ST_ID),
    constraint pkST_CRE_Stream_CreatedAt primary key (
        ST_CRE_ST_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._ST_CRE_Stream_CreatedAt CLUSTER ON pkST_CRE_Stream_CreatedAt;
-- DROP VIEW IF EXISTS dbo.ST_CRE_Stream_CreatedAt;
CREATE OR REPLACE VIEW dbo.ST_CRE_Stream_CreatedAt AS SELECT
    ST_CRE_ST_ID,
    ST_CRE_Stream_CreatedAt,
    Metadata_ST_CRE
FROM dbo._ST_CRE_Stream_CreatedAt;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- ST_ENA_Stream_EndAt table (on ST_Stream)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._ST_ENA_Stream_EndAt;
CREATE TABLE IF NOT EXISTS dbo._ST_ENA_Stream_EndAt (
    ST_ENA_ST_ID int not null,
    ST_ENA_Stream_EndAt timestamp not null,
    ST_ENA_ChangedAt timestamp not null,
    Metadata_ST_ENA int not null,
    constraint fkST_ENA_Stream_EndAt foreign key (
        ST_ENA_ST_ID
    ) references dbo._ST_Stream(ST_ID),
    constraint pkST_ENA_Stream_EndAt primary key (
        ST_ENA_ST_ID,
        ST_ENA_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._ST_ENA_Stream_EndAt CLUSTER ON pkST_ENA_Stream_EndAt;
-- DROP VIEW IF EXISTS dbo.ST_ENA_Stream_EndAt;
CREATE OR REPLACE VIEW dbo.ST_ENA_Stream_EndAt AS SELECT
    ST_ENA_ST_ID,
    ST_ENA_Stream_EndAt,
    ST_ENA_ChangedAt,
    Metadata_ST_ENA
FROM dbo._ST_ENA_Stream_EndAt;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- ST_STA_Stream_StartAt table (on ST_Stream)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._ST_STA_Stream_StartAt;
CREATE TABLE IF NOT EXISTS dbo._ST_STA_Stream_StartAt (
    ST_STA_ST_ID int not null,
    ST_STA_Stream_StartAt timestamp not null,
    ST_STA_ChangedAt timestamp not null,
    Metadata_ST_STA int not null,
    constraint fkST_STA_Stream_StartAt foreign key (
        ST_STA_ST_ID
    ) references dbo._ST_Stream(ST_ID),
    constraint pkST_STA_Stream_StartAt primary key (
        ST_STA_ST_ID,
        ST_STA_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._ST_STA_Stream_StartAt CLUSTER ON pkST_STA_Stream_StartAt;
-- DROP VIEW IF EXISTS dbo.ST_STA_Stream_StartAt;
CREATE OR REPLACE VIEW dbo.ST_STA_Stream_StartAt AS SELECT
    ST_STA_ST_ID,
    ST_STA_Stream_StartAt,
    ST_STA_ChangedAt,
    Metadata_ST_STA
FROM dbo._ST_STA_Stream_StartAt;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- ST_HOM_Stream_HomeworkDeadlineDays table (on ST_Stream)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._ST_HOM_Stream_HomeworkDeadlineDays;
CREATE TABLE IF NOT EXISTS dbo._ST_HOM_Stream_HomeworkDeadlineDays (
    ST_HOM_ST_ID int not null,
    ST_HOM_Stream_HomeworkDeadlineDays int not null,
    ST_HOM_ChangedAt timestamp not null,
    Metadata_ST_HOM int not null,
    constraint fkST_HOM_Stream_HomeworkDeadlineDays foreign key (
        ST_HOM_ST_ID
    ) references dbo._ST_Stream(ST_ID),
    constraint pkST_HOM_Stream_HomeworkDeadlineDays primary key (
        ST_HOM_ST_ID,
        ST_HOM_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._ST_HOM_Stream_HomeworkDeadlineDays CLUSTER ON pkST_HOM_Stream_HomeworkDeadlineDays;
-- DROP VIEW IF EXISTS dbo.ST_HOM_Stream_HomeworkDeadlineDays;
CREATE OR REPLACE VIEW dbo.ST_HOM_Stream_HomeworkDeadlineDays AS SELECT
    ST_HOM_ST_ID,
    ST_HOM_Stream_HomeworkDeadlineDays,
    ST_HOM_ChangedAt,
    Metadata_ST_HOM
FROM dbo._ST_HOM_Stream_HomeworkDeadlineDays;
-- Knotted static attribute table -------------------------------------------------------------------------------------
-- ST_OPE_Stream_IsOpen table (on ST_Stream)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._ST_OPE_Stream_IsOpen;
CREATE TABLE IF NOT EXISTS dbo._ST_OPE_Stream_IsOpen (
    ST_OPE_ST_ID int not null,
    ST_OPE_OPE_ID int not null,
    Metadata_ST_OPE int not null,
    constraint fk_A_ST_OPE_Stream_IsOpen foreign key (
        ST_OPE_ST_ID
    ) references dbo._ST_Stream(ST_ID),
    constraint fk_K_ST_OPE_Stream_IsOpen foreign key (
        ST_OPE_OPE_ID
    ) references dbo._OPE_IsOpen(OPE_ID),
    constraint pkST_OPE_Stream_IsOpen primary key (
        ST_OPE_ST_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._ST_OPE_Stream_IsOpen CLUSTER ON pkST_OPE_Stream_IsOpen;
-- DROP VIEW IF EXISTS dbo.ST_OPE_Stream_IsOpen;
CREATE OR REPLACE VIEW dbo.ST_OPE_Stream_IsOpen AS SELECT
    ST_OPE_ST_ID,
    ST_OPE_OPE_ID,
    Metadata_ST_OPE
FROM dbo._ST_OPE_Stream_IsOpen;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- ST_NAM_Stream_Name table (on ST_Stream)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._ST_NAM_Stream_Name;
CREATE TABLE IF NOT EXISTS dbo._ST_NAM_Stream_Name (
    ST_NAM_ST_ID int not null,
    ST_NAM_Stream_Name varchar(255) not null,
    ST_NAM_ChangedAt timestamp not null,
    Metadata_ST_NAM int not null,
    constraint fkST_NAM_Stream_Name foreign key (
        ST_NAM_ST_ID
    ) references dbo._ST_Stream(ST_ID),
    constraint pkST_NAM_Stream_Name primary key (
        ST_NAM_ST_ID,
        ST_NAM_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._ST_NAM_Stream_Name CLUSTER ON pkST_NAM_Stream_Name;
-- DROP VIEW IF EXISTS dbo.ST_NAM_Stream_Name;
CREATE OR REPLACE VIEW dbo.ST_NAM_Stream_Name AS SELECT
    ST_NAM_ST_ID,
    ST_NAM_Stream_Name,
    ST_NAM_ChangedAt,
    Metadata_ST_NAM
FROM dbo._ST_NAM_Stream_Name;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- ST_UPD_Stream_UpdatedAt table (on ST_Stream)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._ST_UPD_Stream_UpdatedAt;
CREATE TABLE IF NOT EXISTS dbo._ST_UPD_Stream_UpdatedAt (
    ST_UPD_ST_ID int not null,
    ST_UPD_Stream_UpdatedAt timestamp not null,
    ST_UPD_ChangedAt timestamp not null,
    Metadata_ST_UPD int not null,
    constraint fkST_UPD_Stream_UpdatedAt foreign key (
        ST_UPD_ST_ID
    ) references dbo._ST_Stream(ST_ID),
    constraint pkST_UPD_Stream_UpdatedAt primary key (
        ST_UPD_ST_ID,
        ST_UPD_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._ST_UPD_Stream_UpdatedAt CLUSTER ON pkST_UPD_Stream_UpdatedAt;
-- DROP VIEW IF EXISTS dbo.ST_UPD_Stream_UpdatedAt;
CREATE OR REPLACE VIEW dbo.ST_UPD_Stream_UpdatedAt AS SELECT
    ST_UPD_ST_ID,
    ST_UPD_Stream_UpdatedAt,
    ST_UPD_ChangedAt,
    Metadata_ST_UPD
FROM dbo._ST_UPD_Stream_UpdatedAt;
-- Anchor table -------------------------------------------------------------------------------------------------------
-- MO_Module table (with 5 attributes)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._MO_Module;
CREATE TABLE IF NOT EXISTS dbo._MO_Module (
    MO_ID serial not null, 
    Metadata_MO int not null, 
    constraint pkMO_Module primary key (
        MO_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._MO_Module CLUSTER ON pkMO_Module;
-- DROP VIEW IF EXISTS dbo.MO_Module;
CREATE OR REPLACE VIEW dbo.MO_Module AS SELECT 
    MO_ID,
    Metadata_MO 
FROM dbo._MO_Module;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- MO_UPD_Module_UpdatedAt table (on MO_Module)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._MO_UPD_Module_UpdatedAt;
CREATE TABLE IF NOT EXISTS dbo._MO_UPD_Module_UpdatedAt (
    MO_UPD_MO_ID int not null,
    MO_UPD_Module_UpdatedAt timestamp not null,
    MO_UPD_ChangedAt timestamp not null,
    Metadata_MO_UPD int not null,
    constraint fkMO_UPD_Module_UpdatedAt foreign key (
        MO_UPD_MO_ID
    ) references dbo._MO_Module(MO_ID),
    constraint pkMO_UPD_Module_UpdatedAt primary key (
        MO_UPD_MO_ID,
        MO_UPD_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._MO_UPD_Module_UpdatedAt CLUSTER ON pkMO_UPD_Module_UpdatedAt;
-- DROP VIEW IF EXISTS dbo.MO_UPD_Module_UpdatedAt;
CREATE OR REPLACE VIEW dbo.MO_UPD_Module_UpdatedAt AS SELECT
    MO_UPD_MO_ID,
    MO_UPD_Module_UpdatedAt,
    MO_UPD_ChangedAt,
    Metadata_MO_UPD
FROM dbo._MO_UPD_Module_UpdatedAt;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- MO_TIT_Module_Title table (on MO_Module)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._MO_TIT_Module_Title;
CREATE TABLE IF NOT EXISTS dbo._MO_TIT_Module_Title (
    MO_TIT_MO_ID int not null,
    MO_TIT_Module_Title varchar(255) not null,
    MO_TIT_ChangedAt timestamp not null,
    Metadata_MO_TIT int not null,
    constraint fkMO_TIT_Module_Title foreign key (
        MO_TIT_MO_ID
    ) references dbo._MO_Module(MO_ID),
    constraint pkMO_TIT_Module_Title primary key (
        MO_TIT_MO_ID,
        MO_TIT_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._MO_TIT_Module_Title CLUSTER ON pkMO_TIT_Module_Title;
-- DROP VIEW IF EXISTS dbo.MO_TIT_Module_Title;
CREATE OR REPLACE VIEW dbo.MO_TIT_Module_Title AS SELECT
    MO_TIT_MO_ID,
    MO_TIT_Module_Title,
    MO_TIT_ChangedAt,
    Metadata_MO_TIT
FROM dbo._MO_TIT_Module_Title;
-- Static attribute table ---------------------------------------------------------------------------------------------
-- MO_CRE_Module_CreatedAt table (on MO_Module)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._MO_CRE_Module_CreatedAt;
CREATE TABLE IF NOT EXISTS dbo._MO_CRE_Module_CreatedAt (
    MO_CRE_MO_ID int not null,
    MO_CRE_Module_CreatedAt timestamp not null,
    Metadata_MO_CRE int not null,
    constraint fkMO_CRE_Module_CreatedAt foreign key (
        MO_CRE_MO_ID
    ) references dbo._MO_Module(MO_ID),
    constraint pkMO_CRE_Module_CreatedAt primary key (
        MO_CRE_MO_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._MO_CRE_Module_CreatedAt CLUSTER ON pkMO_CRE_Module_CreatedAt;
-- DROP VIEW IF EXISTS dbo.MO_CRE_Module_CreatedAt;
CREATE OR REPLACE VIEW dbo.MO_CRE_Module_CreatedAt AS SELECT
    MO_CRE_MO_ID,
    MO_CRE_Module_CreatedAt,
    Metadata_MO_CRE
FROM dbo._MO_CRE_Module_CreatedAt;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- MO_ORD_Module_OrderInStream table (on MO_Module)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._MO_ORD_Module_OrderInStream;
CREATE TABLE IF NOT EXISTS dbo._MO_ORD_Module_OrderInStream (
    MO_ORD_MO_ID int not null,
    MO_ORD_Module_OrderInStream int not null,
    MO_ORD_ChangedAt timestamp not null,
    Metadata_MO_ORD int not null,
    constraint fkMO_ORD_Module_OrderInStream foreign key (
        MO_ORD_MO_ID
    ) references dbo._MO_Module(MO_ID),
    constraint pkMO_ORD_Module_OrderInStream primary key (
        MO_ORD_MO_ID,
        MO_ORD_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._MO_ORD_Module_OrderInStream CLUSTER ON pkMO_ORD_Module_OrderInStream;
-- DROP VIEW IF EXISTS dbo.MO_ORD_Module_OrderInStream;
CREATE OR REPLACE VIEW dbo.MO_ORD_Module_OrderInStream AS SELECT
    MO_ORD_MO_ID,
    MO_ORD_Module_OrderInStream,
    MO_ORD_ChangedAt,
    Metadata_MO_ORD
FROM dbo._MO_ORD_Module_OrderInStream;
-- Static attribute table ---------------------------------------------------------------------------------------------
-- MO_DEL_Module_DeletedAt table (on MO_Module)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._MO_DEL_Module_DeletedAt;
CREATE TABLE IF NOT EXISTS dbo._MO_DEL_Module_DeletedAt (
    MO_DEL_MO_ID int not null,
    MO_DEL_Module_DeletedAt timestamp not null,
    Metadata_MO_DEL int not null,
    constraint fkMO_DEL_Module_DeletedAt foreign key (
        MO_DEL_MO_ID
    ) references dbo._MO_Module(MO_ID),
    constraint pkMO_DEL_Module_DeletedAt primary key (
        MO_DEL_MO_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._MO_DEL_Module_DeletedAt CLUSTER ON pkMO_DEL_Module_DeletedAt;
-- DROP VIEW IF EXISTS dbo.MO_DEL_Module_DeletedAt;
CREATE OR REPLACE VIEW dbo.MO_DEL_Module_DeletedAt AS SELECT
    MO_DEL_MO_ID,
    MO_DEL_Module_DeletedAt,
    Metadata_MO_DEL
FROM dbo._MO_DEL_Module_DeletedAt;
-- Anchor table -------------------------------------------------------------------------------------------------------
-- LE_Lesson table (with 8 attributes)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._LE_Lesson;
CREATE TABLE IF NOT EXISTS dbo._LE_Lesson (
    LE_ID serial not null, 
    Metadata_LE int not null, 
    constraint pkLE_Lesson primary key (
        LE_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._LE_Lesson CLUSTER ON pkLE_Lesson;
-- DROP VIEW IF EXISTS dbo.LE_Lesson;
CREATE OR REPLACE VIEW dbo.LE_Lesson AS SELECT 
    LE_ID,
    Metadata_LE 
FROM dbo._LE_Lesson;
-- Static attribute table ---------------------------------------------------------------------------------------------
-- LE_DEL_Lesson_DeletedAt table (on LE_Lesson)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._LE_DEL_Lesson_DeletedAt;
CREATE TABLE IF NOT EXISTS dbo._LE_DEL_Lesson_DeletedAt (
    LE_DEL_LE_ID int not null,
    LE_DEL_Lesson_DeletedAt timestamp(0) not null,
    Metadata_LE_DEL int not null,
    constraint fkLE_DEL_Lesson_DeletedAt foreign key (
        LE_DEL_LE_ID
    ) references dbo._LE_Lesson(LE_ID),
    constraint pkLE_DEL_Lesson_DeletedAt primary key (
        LE_DEL_LE_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._LE_DEL_Lesson_DeletedAt CLUSTER ON pkLE_DEL_Lesson_DeletedAt;
-- DROP VIEW IF EXISTS dbo.LE_DEL_Lesson_DeletedAt;
CREATE OR REPLACE VIEW dbo.LE_DEL_Lesson_DeletedAt AS SELECT
    LE_DEL_LE_ID,
    LE_DEL_Lesson_DeletedAt,
    Metadata_LE_DEL
FROM dbo._LE_DEL_Lesson_DeletedAt;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- LE_JOI_Lesson_OnlineLessonJoinUrl table (on LE_Lesson)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._LE_JOI_Lesson_OnlineLessonJoinUrl;
CREATE TABLE IF NOT EXISTS dbo._LE_JOI_Lesson_OnlineLessonJoinUrl (
    LE_JOI_LE_ID int not null,
    LE_JOI_Lesson_OnlineLessonJoinUrl varchar(255) not null,
    LE_JOI_ChangedAt timestamp not null,
    Metadata_LE_JOI int not null,
    constraint fkLE_JOI_Lesson_OnlineLessonJoinUrl foreign key (
        LE_JOI_LE_ID
    ) references dbo._LE_Lesson(LE_ID),
    constraint pkLE_JOI_Lesson_OnlineLessonJoinUrl primary key (
        LE_JOI_LE_ID,
        LE_JOI_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._LE_JOI_Lesson_OnlineLessonJoinUrl CLUSTER ON pkLE_JOI_Lesson_OnlineLessonJoinUrl;
-- DROP VIEW IF EXISTS dbo.LE_JOI_Lesson_OnlineLessonJoinUrl;
CREATE OR REPLACE VIEW dbo.LE_JOI_Lesson_OnlineLessonJoinUrl AS SELECT
    LE_JOI_LE_ID,
    LE_JOI_Lesson_OnlineLessonJoinUrl,
    LE_JOI_ChangedAt,
    Metadata_LE_JOI
FROM dbo._LE_JOI_Lesson_OnlineLessonJoinUrl;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- LE_REC_Lesson_OnlineLessonRecordingUrl table (on LE_Lesson)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._LE_REC_Lesson_OnlineLessonRecordingUrl;
CREATE TABLE IF NOT EXISTS dbo._LE_REC_Lesson_OnlineLessonRecordingUrl (
    LE_REC_LE_ID int not null,
    LE_REC_Lesson_OnlineLessonRecordingUrl varchar(255) not null,
    LE_REC_ChangedAt timestamp not null,
    Metadata_LE_REC int not null,
    constraint fkLE_REC_Lesson_OnlineLessonRecordingUrl foreign key (
        LE_REC_LE_ID
    ) references dbo._LE_Lesson(LE_ID),
    constraint pkLE_REC_Lesson_OnlineLessonRecordingUrl primary key (
        LE_REC_LE_ID,
        LE_REC_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._LE_REC_Lesson_OnlineLessonRecordingUrl CLUSTER ON pkLE_REC_Lesson_OnlineLessonRecordingUrl;
-- DROP VIEW IF EXISTS dbo.LE_REC_Lesson_OnlineLessonRecordingUrl;
CREATE OR REPLACE VIEW dbo.LE_REC_Lesson_OnlineLessonRecordingUrl AS SELECT
    LE_REC_LE_ID,
    LE_REC_Lesson_OnlineLessonRecordingUrl,
    LE_REC_ChangedAt,
    Metadata_LE_REC
FROM dbo._LE_REC_Lesson_OnlineLessonRecordingUrl;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- LE_TIT_Lesson_Title table (on LE_Lesson)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._LE_TIT_Lesson_Title;
CREATE TABLE IF NOT EXISTS dbo._LE_TIT_Lesson_Title (
    LE_TIT_LE_ID int not null,
    LE_TIT_Lesson_Title varchar(255) not null,
    LE_TIT_ChangedAt timestamp not null,
    Metadata_LE_TIT int not null,
    constraint fkLE_TIT_Lesson_Title foreign key (
        LE_TIT_LE_ID
    ) references dbo._LE_Lesson(LE_ID),
    constraint pkLE_TIT_Lesson_Title primary key (
        LE_TIT_LE_ID,
        LE_TIT_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._LE_TIT_Lesson_Title CLUSTER ON pkLE_TIT_Lesson_Title;
-- DROP VIEW IF EXISTS dbo.LE_TIT_Lesson_Title;
CREATE OR REPLACE VIEW dbo.LE_TIT_Lesson_Title AS SELECT
    LE_TIT_LE_ID,
    LE_TIT_Lesson_Title,
    LE_TIT_ChangedAt,
    Metadata_LE_TIT
FROM dbo._LE_TIT_Lesson_Title;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- LE_EDT_Lesson_EndAt table (on LE_Lesson)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._LE_EDT_Lesson_EndAt;
CREATE TABLE IF NOT EXISTS dbo._LE_EDT_Lesson_EndAt (
    LE_EDT_LE_ID int not null,
    LE_EDT_Lesson_EndAt timestamp not null,
    LE_EDT_ChangedAt timestamp not null,
    Metadata_LE_EDT int not null,
    constraint fkLE_EDT_Lesson_EndAt foreign key (
        LE_EDT_LE_ID
    ) references dbo._LE_Lesson(LE_ID),
    constraint pkLE_EDT_Lesson_EndAt primary key (
        LE_EDT_LE_ID,
        LE_EDT_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._LE_EDT_Lesson_EndAt CLUSTER ON pkLE_EDT_Lesson_EndAt;
-- DROP VIEW IF EXISTS dbo.LE_EDT_Lesson_EndAt;
CREATE OR REPLACE VIEW dbo.LE_EDT_Lesson_EndAt AS SELECT
    LE_EDT_LE_ID,
    LE_EDT_Lesson_EndAt,
    LE_EDT_ChangedAt,
    Metadata_LE_EDT
FROM dbo._LE_EDT_Lesson_EndAt;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- LE_HOM_Lesson_HomeWorkUrl table (on LE_Lesson)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._LE_HOM_Lesson_HomeWorkUrl;
CREATE TABLE IF NOT EXISTS dbo._LE_HOM_Lesson_HomeWorkUrl (
    LE_HOM_LE_ID int not null,
    LE_HOM_Lesson_HomeWorkUrl varchar(500) not null,
    LE_HOM_ChangedAt timestamp not null,
    Metadata_LE_HOM int not null,
    constraint fkLE_HOM_Lesson_HomeWorkUrl foreign key (
        LE_HOM_LE_ID
    ) references dbo._LE_Lesson(LE_ID),
    constraint pkLE_HOM_Lesson_HomeWorkUrl primary key (
        LE_HOM_LE_ID,
        LE_HOM_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._LE_HOM_Lesson_HomeWorkUrl CLUSTER ON pkLE_HOM_Lesson_HomeWorkUrl;
-- DROP VIEW IF EXISTS dbo.LE_HOM_Lesson_HomeWorkUrl;
CREATE OR REPLACE VIEW dbo.LE_HOM_Lesson_HomeWorkUrl AS SELECT
    LE_HOM_LE_ID,
    LE_HOM_Lesson_HomeWorkUrl,
    LE_HOM_ChangedAt,
    Metadata_LE_HOM
FROM dbo._LE_HOM_Lesson_HomeWorkUrl;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- LE_DES_Lesson_Description table (on LE_Lesson)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._LE_DES_Lesson_Description;
CREATE TABLE IF NOT EXISTS dbo._LE_DES_Lesson_Description (
    LE_DES_LE_ID int not null,
    LE_DES_Lesson_Description text not null,
    LE_DES_ChangedAt timestamp not null,
    Metadata_LE_DES int not null,
    constraint fkLE_DES_Lesson_Description foreign key (
        LE_DES_LE_ID
    ) references dbo._LE_Lesson(LE_ID),
    constraint pkLE_DES_Lesson_Description primary key (
        LE_DES_LE_ID,
        LE_DES_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._LE_DES_Lesson_Description CLUSTER ON pkLE_DES_Lesson_Description;
-- DROP VIEW IF EXISTS dbo.LE_DES_Lesson_Description;
CREATE OR REPLACE VIEW dbo.LE_DES_Lesson_Description AS SELECT
    LE_DES_LE_ID,
    LE_DES_Lesson_Description,
    LE_DES_ChangedAt,
    Metadata_LE_DES
FROM dbo._LE_DES_Lesson_Description;
-- Historized attribute table -----------------------------------------------------------------------------------------
-- LE_STA_Lesson_StartAt table (on LE_Lesson)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._LE_STA_Lesson_StartAt;
CREATE TABLE IF NOT EXISTS dbo._LE_STA_Lesson_StartAt (
    LE_STA_LE_ID int not null,
    LE_STA_Lesson_StartAt timestamp not null,
    LE_STA_ChangedAt timestamp not null,
    Metadata_LE_STA int not null,
    constraint fkLE_STA_Lesson_StartAt foreign key (
        LE_STA_LE_ID
    ) references dbo._LE_Lesson(LE_ID),
    constraint pkLE_STA_Lesson_StartAt primary key (
        LE_STA_LE_ID,
        LE_STA_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._LE_STA_Lesson_StartAt CLUSTER ON pkLE_STA_Lesson_StartAt;
-- DROP VIEW IF EXISTS dbo.LE_STA_Lesson_StartAt;
CREATE OR REPLACE VIEW dbo.LE_STA_Lesson_StartAt AS SELECT
    LE_STA_LE_ID,
    LE_STA_Lesson_StartAt,
    LE_STA_ChangedAt,
    Metadata_LE_STA
FROM dbo._LE_STA_Lesson_StartAt;
-- Anchor table -------------------------------------------------------------------------------------------------------
-- TE_Teacher table (with 0 attributes)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._TE_Teacher;
CREATE TABLE IF NOT EXISTS dbo._TE_Teacher (
    TE_ID serial not null, 
    Metadata_TE int not null, 
    constraint pkTE_Teacher primary key (
        TE_ID
    )
);
ALTER TABLE IF EXISTS ONLY dbo._TE_Teacher CLUSTER ON pkTE_Teacher;
-- DROP VIEW IF EXISTS dbo.TE_Teacher;
CREATE OR REPLACE VIEW dbo.TE_Teacher AS SELECT 
    TE_ID,
    Metadata_TE 
FROM dbo._TE_Teacher;
-- TIES ---------------------------------------------------------------------------------------------------------------
--
-- Ties are used to represent relationships between entities.
-- They come in four flavors: static, historized, knotted static, and knotted historized.
-- Ties have cardinality, constraining how members may participate in the relationship.
-- Every entity that is a member in a tie has a specified role in the relationship.
-- Ties must have at least two anchor roles and zero or more knot roles.
--
-- Historized tie table -----------------------------------------------------------------------------------------------
-- CO_learned_ST_on table (having 2 roles)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._CO_learned_ST_on;
CREATE TABLE IF NOT EXISTS dbo._CO_learned_ST_on (
    CO_ID_learned int not null, 
    ST_ID_on int not null, 
    CO_learned_ST_on_ChangedAt timestamp not null,
    Metadata_CO_learned_ST_on int not null,
    constraint CO_learned_ST_on_fkCO_learned foreign key (
        CO_ID_learned
    ) references dbo._CO_Course(CO_ID), 
    constraint CO_learned_ST_on_fkST_on foreign key (
        ST_ID_on
    ) references dbo._ST_Stream(ST_ID), 
    constraint pkCO_learned_ST_on primary key (
        ST_ID_on,
        CO_learned_ST_on_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._CO_learned_ST_on CLUSTER ON pkCO_learned_ST_on;
-- DROP VIEW IF EXISTS dbo.CO_learned_ST_on;
CREATE OR REPLACE VIEW dbo.CO_learned_ST_on AS SELECT
    CO_ID_learned,
    ST_ID_on,
    CO_learned_ST_on_ChangedAt,
    Metadata_CO_learned_ST_on
FROM dbo._CO_learned_ST_on;
-- Historized tie table -----------------------------------------------------------------------------------------------
-- ST_consists_MO_of table (having 2 roles)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._ST_consists_MO_of;
CREATE TABLE IF NOT EXISTS dbo._ST_consists_MO_of (
    ST_ID_consists int not null, 
    MO_ID_of int not null, 
    ST_consists_MO_of_ChangedAt timestamp not null,
    Metadata_ST_consists_MO_of int not null,
    constraint ST_consists_MO_of_fkST_consists foreign key (
        ST_ID_consists
    ) references dbo._ST_Stream(ST_ID), 
    constraint ST_consists_MO_of_fkMO_of foreign key (
        MO_ID_of
    ) references dbo._MO_Module(MO_ID), 
    constraint pkST_consists_MO_of primary key (
        MO_ID_of,
        ST_consists_MO_of_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._ST_consists_MO_of CLUSTER ON pkST_consists_MO_of;
-- DROP VIEW IF EXISTS dbo.ST_consists_MO_of;
CREATE OR REPLACE VIEW dbo.ST_consists_MO_of AS SELECT
    ST_ID_consists,
    MO_ID_of,
    ST_consists_MO_of_ChangedAt,
    Metadata_ST_consists_MO_of
FROM dbo._ST_consists_MO_of;
-- Historized tie table -----------------------------------------------------------------------------------------------
-- MO_consist_LE_of table (having 2 roles)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._MO_consist_LE_of;
CREATE TABLE IF NOT EXISTS dbo._MO_consist_LE_of (
    MO_ID_consist int not null, 
    LE_ID_of int not null, 
    MO_consist_LE_of_ChangedAt timestamp not null,
    Metadata_MO_consist_LE_of int not null,
    constraint MO_consist_LE_of_fkMO_consist foreign key (
        MO_ID_consist
    ) references dbo._MO_Module(MO_ID), 
    constraint MO_consist_LE_of_fkLE_of foreign key (
        LE_ID_of
    ) references dbo._LE_Lesson(LE_ID), 
    constraint pkMO_consist_LE_of primary key (
        LE_ID_of,
        MO_consist_LE_of_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._MO_consist_LE_of CLUSTER ON pkMO_consist_LE_of;
-- DROP VIEW IF EXISTS dbo.MO_consist_LE_of;
CREATE OR REPLACE VIEW dbo.MO_consist_LE_of AS SELECT
    MO_ID_consist,
    LE_ID_of,
    MO_consist_LE_of_ChangedAt,
    Metadata_MO_consist_LE_of
FROM dbo._MO_consist_LE_of;
-- Historized tie table -----------------------------------------------------------------------------------------------
-- LE_tought_TE_by table (having 2 roles)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._LE_tought_TE_by;
CREATE TABLE IF NOT EXISTS dbo._LE_tought_TE_by (
    LE_ID_tought int not null, 
    TE_ID_by int not null, 
    LE_tought_TE_by_ChangedAt timestamp not null,
    Metadata_LE_tought_TE_by int not null,
    constraint LE_tought_TE_by_fkLE_tought foreign key (
        LE_ID_tought
    ) references dbo._LE_Lesson(LE_ID), 
    constraint LE_tought_TE_by_fkTE_by foreign key (
        TE_ID_by
    ) references dbo._TE_Teacher(TE_ID), 
    constraint pkLE_tought_TE_by primary key (
        LE_ID_tought,
        LE_tought_TE_by_ChangedAt
    )
);
ALTER TABLE IF EXISTS ONLY dbo._LE_tought_TE_by CLUSTER ON pkLE_tought_TE_by;
-- DROP VIEW IF EXISTS dbo.LE_tought_TE_by;
CREATE OR REPLACE VIEW dbo.LE_tought_TE_by AS SELECT
    LE_ID_tought,
    TE_ID_by,
    LE_tought_TE_by_ChangedAt,
    Metadata_LE_tought_TE_by
FROM dbo._LE_tought_TE_by;
-- ATTRIBUTE RESTATEMENT CONSTRAINTS ----------------------------------------------------------------------------------
--
-- Attributes may be prevented from storing restatements.
-- A restatement is when the same value occurs for two adjacent points
-- in changing time.
--
-- returns 1 for at least one equal surrounding value, 0 for different surrounding values
--
-- id the identity of the anchored entity
-- eq the equivalent (when applicable)
-- value the value of the attribute
-- changed the point in time from which this value shall represent a change
--
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfCO_TIT_Course_Title restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcCO_TIT_Course_Title restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfCO_TIT_Course_Title(
    int,
    varchar(255),
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfCO_TIT_Course_Title(
    id int,
    value varchar(255),
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.CO_TIT_Course_Title
                    FROM
                        dbo.CO_TIT_Course_Title pre
                    WHERE
                        pre.CO_TIT_CO_ID = id
                    AND
                        pre.CO_TIT_ChangedAt < changed
                    ORDER BY
                        pre.CO_TIT_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.CO_TIT_Course_Title
                    FROM
                        dbo.CO_TIT_Course_Title fol
                    WHERE
                        fol.CO_TIT_CO_ID = id
                    AND
                        fol.CO_TIT_ChangedAt > changed
                    ORDER BY
                        fol.CO_TIT_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfCO_ICO_Course_IconUrl restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcCO_ICO_Course_IconUrl restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfCO_ICO_Course_IconUrl(
    int,
    varchar(255),
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfCO_ICO_Course_IconUrl(
    id int,
    value varchar(255),
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.CO_ICO_Course_IconUrl
                    FROM
                        dbo.CO_ICO_Course_IconUrl pre
                    WHERE
                        pre.CO_ICO_CO_ID = id
                    AND
                        pre.CO_ICO_ChangedAt < changed
                    ORDER BY
                        pre.CO_ICO_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.CO_ICO_Course_IconUrl
                    FROM
                        dbo.CO_ICO_Course_IconUrl fol
                    WHERE
                        fol.CO_ICO_CO_ID = id
                    AND
                        fol.CO_ICO_ChangedAt > changed
                    ORDER BY
                        fol.CO_ICO_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfCO_UPD_Course_UpdatedAt restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcCO_UPD_Course_UpdatedAt restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfCO_UPD_Course_UpdatedAt(
    int,
    timestamp,
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfCO_UPD_Course_UpdatedAt(
    id int,
    value timestamp,
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.CO_UPD_Course_UpdatedAt
                    FROM
                        dbo.CO_UPD_Course_UpdatedAt pre
                    WHERE
                        pre.CO_UPD_CO_ID = id
                    AND
                        pre.CO_UPD_ChangedAt < changed
                    ORDER BY
                        pre.CO_UPD_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.CO_UPD_Course_UpdatedAt
                    FROM
                        dbo.CO_UPD_Course_UpdatedAt fol
                    WHERE
                        fol.CO_UPD_CO_ID = id
                    AND
                        fol.CO_UPD_ChangedAt > changed
                    ORDER BY
                        fol.CO_UPD_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfST_ENA_Stream_EndAt restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcST_ENA_Stream_EndAt restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfST_ENA_Stream_EndAt(
    int,
    timestamp,
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfST_ENA_Stream_EndAt(
    id int,
    value timestamp,
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.ST_ENA_Stream_EndAt
                    FROM
                        dbo.ST_ENA_Stream_EndAt pre
                    WHERE
                        pre.ST_ENA_ST_ID = id
                    AND
                        pre.ST_ENA_ChangedAt < changed
                    ORDER BY
                        pre.ST_ENA_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.ST_ENA_Stream_EndAt
                    FROM
                        dbo.ST_ENA_Stream_EndAt fol
                    WHERE
                        fol.ST_ENA_ST_ID = id
                    AND
                        fol.ST_ENA_ChangedAt > changed
                    ORDER BY
                        fol.ST_ENA_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfST_STA_Stream_StartAt restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcST_STA_Stream_StartAt restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfST_STA_Stream_StartAt(
    int,
    timestamp,
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfST_STA_Stream_StartAt(
    id int,
    value timestamp,
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.ST_STA_Stream_StartAt
                    FROM
                        dbo.ST_STA_Stream_StartAt pre
                    WHERE
                        pre.ST_STA_ST_ID = id
                    AND
                        pre.ST_STA_ChangedAt < changed
                    ORDER BY
                        pre.ST_STA_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.ST_STA_Stream_StartAt
                    FROM
                        dbo.ST_STA_Stream_StartAt fol
                    WHERE
                        fol.ST_STA_ST_ID = id
                    AND
                        fol.ST_STA_ChangedAt > changed
                    ORDER BY
                        fol.ST_STA_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfST_HOM_Stream_HomeworkDeadlineDays restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcST_HOM_Stream_HomeworkDeadlineDays restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfST_HOM_Stream_HomeworkDeadlineDays(
    int,
    int,
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfST_HOM_Stream_HomeworkDeadlineDays(
    id int,
    value int,
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.ST_HOM_Stream_HomeworkDeadlineDays
                    FROM
                        dbo.ST_HOM_Stream_HomeworkDeadlineDays pre
                    WHERE
                        pre.ST_HOM_ST_ID = id
                    AND
                        pre.ST_HOM_ChangedAt < changed
                    ORDER BY
                        pre.ST_HOM_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.ST_HOM_Stream_HomeworkDeadlineDays
                    FROM
                        dbo.ST_HOM_Stream_HomeworkDeadlineDays fol
                    WHERE
                        fol.ST_HOM_ST_ID = id
                    AND
                        fol.ST_HOM_ChangedAt > changed
                    ORDER BY
                        fol.ST_HOM_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfST_NAM_Stream_Name restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcST_NAM_Stream_Name restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfST_NAM_Stream_Name(
    int,
    varchar(255),
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfST_NAM_Stream_Name(
    id int,
    value varchar(255),
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.ST_NAM_Stream_Name
                    FROM
                        dbo.ST_NAM_Stream_Name pre
                    WHERE
                        pre.ST_NAM_ST_ID = id
                    AND
                        pre.ST_NAM_ChangedAt < changed
                    ORDER BY
                        pre.ST_NAM_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.ST_NAM_Stream_Name
                    FROM
                        dbo.ST_NAM_Stream_Name fol
                    WHERE
                        fol.ST_NAM_ST_ID = id
                    AND
                        fol.ST_NAM_ChangedAt > changed
                    ORDER BY
                        fol.ST_NAM_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfST_UPD_Stream_UpdatedAt restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcST_UPD_Stream_UpdatedAt restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfST_UPD_Stream_UpdatedAt(
    int,
    timestamp,
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfST_UPD_Stream_UpdatedAt(
    id int,
    value timestamp,
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.ST_UPD_Stream_UpdatedAt
                    FROM
                        dbo.ST_UPD_Stream_UpdatedAt pre
                    WHERE
                        pre.ST_UPD_ST_ID = id
                    AND
                        pre.ST_UPD_ChangedAt < changed
                    ORDER BY
                        pre.ST_UPD_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.ST_UPD_Stream_UpdatedAt
                    FROM
                        dbo.ST_UPD_Stream_UpdatedAt fol
                    WHERE
                        fol.ST_UPD_ST_ID = id
                    AND
                        fol.ST_UPD_ChangedAt > changed
                    ORDER BY
                        fol.ST_UPD_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfMO_UPD_Module_UpdatedAt restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcMO_UPD_Module_UpdatedAt restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfMO_UPD_Module_UpdatedAt(
    int,
    timestamp,
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfMO_UPD_Module_UpdatedAt(
    id int,
    value timestamp,
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.MO_UPD_Module_UpdatedAt
                    FROM
                        dbo.MO_UPD_Module_UpdatedAt pre
                    WHERE
                        pre.MO_UPD_MO_ID = id
                    AND
                        pre.MO_UPD_ChangedAt < changed
                    ORDER BY
                        pre.MO_UPD_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.MO_UPD_Module_UpdatedAt
                    FROM
                        dbo.MO_UPD_Module_UpdatedAt fol
                    WHERE
                        fol.MO_UPD_MO_ID = id
                    AND
                        fol.MO_UPD_ChangedAt > changed
                    ORDER BY
                        fol.MO_UPD_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfMO_TIT_Module_Title restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcMO_TIT_Module_Title restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfMO_TIT_Module_Title(
    int,
    varchar(255),
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfMO_TIT_Module_Title(
    id int,
    value varchar(255),
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.MO_TIT_Module_Title
                    FROM
                        dbo.MO_TIT_Module_Title pre
                    WHERE
                        pre.MO_TIT_MO_ID = id
                    AND
                        pre.MO_TIT_ChangedAt < changed
                    ORDER BY
                        pre.MO_TIT_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.MO_TIT_Module_Title
                    FROM
                        dbo.MO_TIT_Module_Title fol
                    WHERE
                        fol.MO_TIT_MO_ID = id
                    AND
                        fol.MO_TIT_ChangedAt > changed
                    ORDER BY
                        fol.MO_TIT_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfMO_ORD_Module_OrderInStream restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcMO_ORD_Module_OrderInStream restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfMO_ORD_Module_OrderInStream(
    int,
    int,
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfMO_ORD_Module_OrderInStream(
    id int,
    value int,
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.MO_ORD_Module_OrderInStream
                    FROM
                        dbo.MO_ORD_Module_OrderInStream pre
                    WHERE
                        pre.MO_ORD_MO_ID = id
                    AND
                        pre.MO_ORD_ChangedAt < changed
                    ORDER BY
                        pre.MO_ORD_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.MO_ORD_Module_OrderInStream
                    FROM
                        dbo.MO_ORD_Module_OrderInStream fol
                    WHERE
                        fol.MO_ORD_MO_ID = id
                    AND
                        fol.MO_ORD_ChangedAt > changed
                    ORDER BY
                        fol.MO_ORD_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfLE_JOI_Lesson_OnlineLessonJoinUrl restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcLE_JOI_Lesson_OnlineLessonJoinUrl restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfLE_JOI_Lesson_OnlineLessonJoinUrl(
    int,
    varchar(255),
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfLE_JOI_Lesson_OnlineLessonJoinUrl(
    id int,
    value varchar(255),
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.LE_JOI_Lesson_OnlineLessonJoinUrl
                    FROM
                        dbo.LE_JOI_Lesson_OnlineLessonJoinUrl pre
                    WHERE
                        pre.LE_JOI_LE_ID = id
                    AND
                        pre.LE_JOI_ChangedAt < changed
                    ORDER BY
                        pre.LE_JOI_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.LE_JOI_Lesson_OnlineLessonJoinUrl
                    FROM
                        dbo.LE_JOI_Lesson_OnlineLessonJoinUrl fol
                    WHERE
                        fol.LE_JOI_LE_ID = id
                    AND
                        fol.LE_JOI_ChangedAt > changed
                    ORDER BY
                        fol.LE_JOI_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfLE_REC_Lesson_OnlineLessonRecordingUrl restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcLE_REC_Lesson_OnlineLessonRecordingUrl restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfLE_REC_Lesson_OnlineLessonRecordingUrl(
    int,
    varchar(255),
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfLE_REC_Lesson_OnlineLessonRecordingUrl(
    id int,
    value varchar(255),
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.LE_REC_Lesson_OnlineLessonRecordingUrl
                    FROM
                        dbo.LE_REC_Lesson_OnlineLessonRecordingUrl pre
                    WHERE
                        pre.LE_REC_LE_ID = id
                    AND
                        pre.LE_REC_ChangedAt < changed
                    ORDER BY
                        pre.LE_REC_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.LE_REC_Lesson_OnlineLessonRecordingUrl
                    FROM
                        dbo.LE_REC_Lesson_OnlineLessonRecordingUrl fol
                    WHERE
                        fol.LE_REC_LE_ID = id
                    AND
                        fol.LE_REC_ChangedAt > changed
                    ORDER BY
                        fol.LE_REC_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfLE_TIT_Lesson_Title restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcLE_TIT_Lesson_Title restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfLE_TIT_Lesson_Title(
    int,
    varchar(255),
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfLE_TIT_Lesson_Title(
    id int,
    value varchar(255),
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.LE_TIT_Lesson_Title
                    FROM
                        dbo.LE_TIT_Lesson_Title pre
                    WHERE
                        pre.LE_TIT_LE_ID = id
                    AND
                        pre.LE_TIT_ChangedAt < changed
                    ORDER BY
                        pre.LE_TIT_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.LE_TIT_Lesson_Title
                    FROM
                        dbo.LE_TIT_Lesson_Title fol
                    WHERE
                        fol.LE_TIT_LE_ID = id
                    AND
                        fol.LE_TIT_ChangedAt > changed
                    ORDER BY
                        fol.LE_TIT_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfLE_EDT_Lesson_EndAt restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcLE_EDT_Lesson_EndAt restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfLE_EDT_Lesson_EndAt(
    int,
    timestamp,
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfLE_EDT_Lesson_EndAt(
    id int,
    value timestamp,
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.LE_EDT_Lesson_EndAt
                    FROM
                        dbo.LE_EDT_Lesson_EndAt pre
                    WHERE
                        pre.LE_EDT_LE_ID = id
                    AND
                        pre.LE_EDT_ChangedAt < changed
                    ORDER BY
                        pre.LE_EDT_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.LE_EDT_Lesson_EndAt
                    FROM
                        dbo.LE_EDT_Lesson_EndAt fol
                    WHERE
                        fol.LE_EDT_LE_ID = id
                    AND
                        fol.LE_EDT_ChangedAt > changed
                    ORDER BY
                        fol.LE_EDT_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfLE_HOM_Lesson_HomeWorkUrl restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcLE_HOM_Lesson_HomeWorkUrl restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfLE_HOM_Lesson_HomeWorkUrl(
    int,
    varchar(500),
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfLE_HOM_Lesson_HomeWorkUrl(
    id int,
    value varchar(500),
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.LE_HOM_Lesson_HomeWorkUrl
                    FROM
                        dbo.LE_HOM_Lesson_HomeWorkUrl pre
                    WHERE
                        pre.LE_HOM_LE_ID = id
                    AND
                        pre.LE_HOM_ChangedAt < changed
                    ORDER BY
                        pre.LE_HOM_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.LE_HOM_Lesson_HomeWorkUrl
                    FROM
                        dbo.LE_HOM_Lesson_HomeWorkUrl fol
                    WHERE
                        fol.LE_HOM_LE_ID = id
                    AND
                        fol.LE_HOM_ChangedAt > changed
                    ORDER BY
                        fol.LE_HOM_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfLE_DES_Lesson_Description restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcLE_DES_Lesson_Description restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfLE_DES_Lesson_Description(
    int,
    text,
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfLE_DES_Lesson_Description(
    id int,
    value text,
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.LE_DES_Lesson_Description
                    FROM
                        dbo.LE_DES_Lesson_Description pre
                    WHERE
                        pre.LE_DES_LE_ID = id
                    AND
                        pre.LE_DES_ChangedAt < changed
                    ORDER BY
                        pre.LE_DES_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.LE_DES_Lesson_Description
                    FROM
                        dbo.LE_DES_Lesson_Description fol
                    WHERE
                        fol.LE_DES_LE_ID = id
                    AND
                        fol.LE_DES_ChangedAt > changed
                    ORDER BY
                        fol.LE_DES_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfLE_STA_Lesson_StartAt restatement finder, also used by the insert and update triggers for idempotent attributes
-- rcLE_STA_Lesson_StartAt restatement constraint (available only in attributes that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rfLE_STA_Lesson_StartAt(
    int,
    timestamp,
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rfLE_STA_Lesson_StartAt(
    id int,
    value timestamp,
    changed timestamp
) RETURNS smallint AS '
    BEGIN
        IF EXISTS (
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        pre.LE_STA_Lesson_StartAt
                    FROM
                        dbo.LE_STA_Lesson_StartAt pre
                    WHERE
                        pre.LE_STA_LE_ID = id
                    AND
                        pre.LE_STA_ChangedAt < changed
                    ORDER BY
                        pre.LE_STA_ChangedAt DESC
                    LIMIT 1
            )
        )
        OR EXISTS(
            SELECT
                value 
            WHERE
                value = (
                    SELECT
                        fol.LE_STA_Lesson_StartAt
                    FROM
                        dbo.LE_STA_Lesson_StartAt fol
                    WHERE
                        fol.LE_STA_LE_ID = id
                    AND
                        fol.LE_STA_ChangedAt > changed
                    ORDER BY
                        fol.LE_STA_ChangedAt ASC
                    LIMIT 1
            )
        )
        THEN
            RETURN 1;
        END IF;
        RETURN 0;
    END;
' LANGUAGE plpgsql;
-- KEY GENERATORS -----------------------------------------------------------------------------------------------------
--
-- These stored procedures can be used to generate identities of entities.
-- Corresponding anchors must have an incrementing identity column.
--
-- Key Generation Stored Procedure ------------------------------------------------------------------------------------
-- kCO_Course identity by surrogate key generation stored procedure
-----------------------------------------------------------------------------------------------------------------------
--DROP FUNCTION IF EXISTS dbo.kCO_Course(
-- bigint,
-- int
--);
CREATE OR REPLACE FUNCTION dbo.kCO_Course(
    requestedNumberOfIdentities bigint,
    metadata int
) RETURNS void AS '
    BEGIN
        IF requestedNumberOfIdentities > 0
        THEN
            WITH RECURSIVE idGenerator (idNumber) AS (
                SELECT
                    1
                UNION ALL
                SELECT
                    idNumber + 1
                FROM
                    idGenerator
                WHERE
                    idNumber < requestedNumberOfIdentities
            )
            INSERT INTO dbo.CO_Course (
                Metadata_CO
            )
            SELECT
                metadata
            FROM
                idGenerator;
        END IF;
    END;
' LANGUAGE plpgsql;
-- Key Generation Stored Procedure ------------------------------------------------------------------------------------
-- kST_Stream identity by surrogate key generation stored procedure
-----------------------------------------------------------------------------------------------------------------------
--DROP FUNCTION IF EXISTS dbo.kST_Stream(
-- bigint,
-- int
--);
CREATE OR REPLACE FUNCTION dbo.kST_Stream(
    requestedNumberOfIdentities bigint,
    metadata int
) RETURNS void AS '
    BEGIN
        IF requestedNumberOfIdentities > 0
        THEN
            WITH RECURSIVE idGenerator (idNumber) AS (
                SELECT
                    1
                UNION ALL
                SELECT
                    idNumber + 1
                FROM
                    idGenerator
                WHERE
                    idNumber < requestedNumberOfIdentities
            )
            INSERT INTO dbo.ST_Stream (
                Metadata_ST
            )
            SELECT
                metadata
            FROM
                idGenerator;
        END IF;
    END;
' LANGUAGE plpgsql;
-- Key Generation Stored Procedure ------------------------------------------------------------------------------------
-- kMO_Module identity by surrogate key generation stored procedure
-----------------------------------------------------------------------------------------------------------------------
--DROP FUNCTION IF EXISTS dbo.kMO_Module(
-- bigint,
-- int
--);
CREATE OR REPLACE FUNCTION dbo.kMO_Module(
    requestedNumberOfIdentities bigint,
    metadata int
) RETURNS void AS '
    BEGIN
        IF requestedNumberOfIdentities > 0
        THEN
            WITH RECURSIVE idGenerator (idNumber) AS (
                SELECT
                    1
                UNION ALL
                SELECT
                    idNumber + 1
                FROM
                    idGenerator
                WHERE
                    idNumber < requestedNumberOfIdentities
            )
            INSERT INTO dbo.MO_Module (
                Metadata_MO
            )
            SELECT
                metadata
            FROM
                idGenerator;
        END IF;
    END;
' LANGUAGE plpgsql;
-- Key Generation Stored Procedure ------------------------------------------------------------------------------------
-- kLE_Lesson identity by surrogate key generation stored procedure
-----------------------------------------------------------------------------------------------------------------------
--DROP FUNCTION IF EXISTS dbo.kLE_Lesson(
-- bigint,
-- int
--);
CREATE OR REPLACE FUNCTION dbo.kLE_Lesson(
    requestedNumberOfIdentities bigint,
    metadata int
) RETURNS void AS '
    BEGIN
        IF requestedNumberOfIdentities > 0
        THEN
            WITH RECURSIVE idGenerator (idNumber) AS (
                SELECT
                    1
                UNION ALL
                SELECT
                    idNumber + 1
                FROM
                    idGenerator
                WHERE
                    idNumber < requestedNumberOfIdentities
            )
            INSERT INTO dbo.LE_Lesson (
                Metadata_LE
            )
            SELECT
                metadata
            FROM
                idGenerator;
        END IF;
    END;
' LANGUAGE plpgsql;
-- Key Generation Stored Procedure ------------------------------------------------------------------------------------
-- kTE_Teacher identity by surrogate key generation stored procedure
-----------------------------------------------------------------------------------------------------------------------
--DROP FUNCTION IF EXISTS dbo.kTE_Teacher(
-- bigint,
-- int
--);
CREATE OR REPLACE FUNCTION dbo.kTE_Teacher(
    requestedNumberOfIdentities bigint,
    metadata int
) RETURNS void AS '
    BEGIN
        IF requestedNumberOfIdentities > 0
        THEN
            WITH RECURSIVE idGenerator (idNumber) AS (
                SELECT
                    1
                UNION ALL
                SELECT
                    idNumber + 1
                FROM
                    idGenerator
                WHERE
                    idNumber < requestedNumberOfIdentities
            )
            INSERT INTO dbo.TE_Teacher (
                Metadata_TE
            )
            SELECT
                metadata
            FROM
                idGenerator;
        END IF;
    END;
' LANGUAGE plpgsql;
-- ATTRIBUTE REWINDERS ------------------------------------------------------------------------------------------------
--
-- These table valued functions rewind an attribute table to the given
-- point in changing time. It does not pick a temporal perspective and
-- instead shows all rows that have been in effect before that point
-- in time.
--
-- changingTimepoint the point in changing time to rewind to
--
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rCO_TIT_Course_Title rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rCO_TIT_Course_Title(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rCO_TIT_Course_Title(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_CO_TIT int,
    CO_TIT_CO_ID int,
    CO_TIT_Course_Title varchar(255),
    CO_TIT_ChangedAt timestamp
) AS '
    SELECT
        Metadata_CO_TIT,
        CO_TIT_CO_ID,
        CO_TIT_Course_Title,
        CO_TIT_ChangedAt
    FROM
        dbo.CO_TIT_Course_Title
    WHERE
        CO_TIT_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rCO_ICO_Course_IconUrl rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rCO_ICO_Course_IconUrl(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rCO_ICO_Course_IconUrl(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_CO_ICO int,
    CO_ICO_CO_ID int,
    CO_ICO_Course_IconUrl varchar(255),
    CO_ICO_ChangedAt timestamp
) AS '
    SELECT
        Metadata_CO_ICO,
        CO_ICO_CO_ID,
        CO_ICO_Course_IconUrl,
        CO_ICO_ChangedAt
    FROM
        dbo.CO_ICO_Course_IconUrl
    WHERE
        CO_ICO_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rCO_UPD_Course_UpdatedAt rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rCO_UPD_Course_UpdatedAt(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rCO_UPD_Course_UpdatedAt(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_CO_UPD int,
    CO_UPD_CO_ID int,
    CO_UPD_Course_UpdatedAt timestamp,
    CO_UPD_ChangedAt timestamp
) AS '
    SELECT
        Metadata_CO_UPD,
        CO_UPD_CO_ID,
        CO_UPD_Course_UpdatedAt,
        CO_UPD_ChangedAt
    FROM
        dbo.CO_UPD_Course_UpdatedAt
    WHERE
        CO_UPD_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rST_ENA_Stream_EndAt rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rST_ENA_Stream_EndAt(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rST_ENA_Stream_EndAt(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_ST_ENA int,
    ST_ENA_ST_ID int,
    ST_ENA_Stream_EndAt timestamp,
    ST_ENA_ChangedAt timestamp
) AS '
    SELECT
        Metadata_ST_ENA,
        ST_ENA_ST_ID,
        ST_ENA_Stream_EndAt,
        ST_ENA_ChangedAt
    FROM
        dbo.ST_ENA_Stream_EndAt
    WHERE
        ST_ENA_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rST_STA_Stream_StartAt rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rST_STA_Stream_StartAt(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rST_STA_Stream_StartAt(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_ST_STA int,
    ST_STA_ST_ID int,
    ST_STA_Stream_StartAt timestamp,
    ST_STA_ChangedAt timestamp
) AS '
    SELECT
        Metadata_ST_STA,
        ST_STA_ST_ID,
        ST_STA_Stream_StartAt,
        ST_STA_ChangedAt
    FROM
        dbo.ST_STA_Stream_StartAt
    WHERE
        ST_STA_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rST_HOM_Stream_HomeworkDeadlineDays rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rST_HOM_Stream_HomeworkDeadlineDays(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rST_HOM_Stream_HomeworkDeadlineDays(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_ST_HOM int,
    ST_HOM_ST_ID int,
    ST_HOM_Stream_HomeworkDeadlineDays int,
    ST_HOM_ChangedAt timestamp
) AS '
    SELECT
        Metadata_ST_HOM,
        ST_HOM_ST_ID,
        ST_HOM_Stream_HomeworkDeadlineDays,
        ST_HOM_ChangedAt
    FROM
        dbo.ST_HOM_Stream_HomeworkDeadlineDays
    WHERE
        ST_HOM_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rST_NAM_Stream_Name rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rST_NAM_Stream_Name(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rST_NAM_Stream_Name(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_ST_NAM int,
    ST_NAM_ST_ID int,
    ST_NAM_Stream_Name varchar(255),
    ST_NAM_ChangedAt timestamp
) AS '
    SELECT
        Metadata_ST_NAM,
        ST_NAM_ST_ID,
        ST_NAM_Stream_Name,
        ST_NAM_ChangedAt
    FROM
        dbo.ST_NAM_Stream_Name
    WHERE
        ST_NAM_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rST_UPD_Stream_UpdatedAt rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rST_UPD_Stream_UpdatedAt(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rST_UPD_Stream_UpdatedAt(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_ST_UPD int,
    ST_UPD_ST_ID int,
    ST_UPD_Stream_UpdatedAt timestamp,
    ST_UPD_ChangedAt timestamp
) AS '
    SELECT
        Metadata_ST_UPD,
        ST_UPD_ST_ID,
        ST_UPD_Stream_UpdatedAt,
        ST_UPD_ChangedAt
    FROM
        dbo.ST_UPD_Stream_UpdatedAt
    WHERE
        ST_UPD_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rMO_UPD_Module_UpdatedAt rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rMO_UPD_Module_UpdatedAt(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rMO_UPD_Module_UpdatedAt(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_MO_UPD int,
    MO_UPD_MO_ID int,
    MO_UPD_Module_UpdatedAt timestamp,
    MO_UPD_ChangedAt timestamp
) AS '
    SELECT
        Metadata_MO_UPD,
        MO_UPD_MO_ID,
        MO_UPD_Module_UpdatedAt,
        MO_UPD_ChangedAt
    FROM
        dbo.MO_UPD_Module_UpdatedAt
    WHERE
        MO_UPD_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rMO_TIT_Module_Title rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rMO_TIT_Module_Title(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rMO_TIT_Module_Title(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_MO_TIT int,
    MO_TIT_MO_ID int,
    MO_TIT_Module_Title varchar(255),
    MO_TIT_ChangedAt timestamp
) AS '
    SELECT
        Metadata_MO_TIT,
        MO_TIT_MO_ID,
        MO_TIT_Module_Title,
        MO_TIT_ChangedAt
    FROM
        dbo.MO_TIT_Module_Title
    WHERE
        MO_TIT_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rMO_ORD_Module_OrderInStream rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rMO_ORD_Module_OrderInStream(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rMO_ORD_Module_OrderInStream(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_MO_ORD int,
    MO_ORD_MO_ID int,
    MO_ORD_Module_OrderInStream int,
    MO_ORD_ChangedAt timestamp
) AS '
    SELECT
        Metadata_MO_ORD,
        MO_ORD_MO_ID,
        MO_ORD_Module_OrderInStream,
        MO_ORD_ChangedAt
    FROM
        dbo.MO_ORD_Module_OrderInStream
    WHERE
        MO_ORD_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rLE_JOI_Lesson_OnlineLessonJoinUrl rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rLE_JOI_Lesson_OnlineLessonJoinUrl(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rLE_JOI_Lesson_OnlineLessonJoinUrl(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_LE_JOI int,
    LE_JOI_LE_ID int,
    LE_JOI_Lesson_OnlineLessonJoinUrl varchar(255),
    LE_JOI_ChangedAt timestamp
) AS '
    SELECT
        Metadata_LE_JOI,
        LE_JOI_LE_ID,
        LE_JOI_Lesson_OnlineLessonJoinUrl,
        LE_JOI_ChangedAt
    FROM
        dbo.LE_JOI_Lesson_OnlineLessonJoinUrl
    WHERE
        LE_JOI_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rLE_REC_Lesson_OnlineLessonRecordingUrl rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rLE_REC_Lesson_OnlineLessonRecordingUrl(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rLE_REC_Lesson_OnlineLessonRecordingUrl(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_LE_REC int,
    LE_REC_LE_ID int,
    LE_REC_Lesson_OnlineLessonRecordingUrl varchar(255),
    LE_REC_ChangedAt timestamp
) AS '
    SELECT
        Metadata_LE_REC,
        LE_REC_LE_ID,
        LE_REC_Lesson_OnlineLessonRecordingUrl,
        LE_REC_ChangedAt
    FROM
        dbo.LE_REC_Lesson_OnlineLessonRecordingUrl
    WHERE
        LE_REC_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rLE_TIT_Lesson_Title rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rLE_TIT_Lesson_Title(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rLE_TIT_Lesson_Title(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_LE_TIT int,
    LE_TIT_LE_ID int,
    LE_TIT_Lesson_Title varchar(255),
    LE_TIT_ChangedAt timestamp
) AS '
    SELECT
        Metadata_LE_TIT,
        LE_TIT_LE_ID,
        LE_TIT_Lesson_Title,
        LE_TIT_ChangedAt
    FROM
        dbo.LE_TIT_Lesson_Title
    WHERE
        LE_TIT_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rLE_EDT_Lesson_EndAt rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rLE_EDT_Lesson_EndAt(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rLE_EDT_Lesson_EndAt(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_LE_EDT int,
    LE_EDT_LE_ID int,
    LE_EDT_Lesson_EndAt timestamp,
    LE_EDT_ChangedAt timestamp
) AS '
    SELECT
        Metadata_LE_EDT,
        LE_EDT_LE_ID,
        LE_EDT_Lesson_EndAt,
        LE_EDT_ChangedAt
    FROM
        dbo.LE_EDT_Lesson_EndAt
    WHERE
        LE_EDT_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rLE_HOM_Lesson_HomeWorkUrl rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rLE_HOM_Lesson_HomeWorkUrl(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rLE_HOM_Lesson_HomeWorkUrl(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_LE_HOM int,
    LE_HOM_LE_ID int,
    LE_HOM_Lesson_HomeWorkUrl varchar(500),
    LE_HOM_ChangedAt timestamp
) AS '
    SELECT
        Metadata_LE_HOM,
        LE_HOM_LE_ID,
        LE_HOM_Lesson_HomeWorkUrl,
        LE_HOM_ChangedAt
    FROM
        dbo.LE_HOM_Lesson_HomeWorkUrl
    WHERE
        LE_HOM_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rLE_DES_Lesson_Description rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rLE_DES_Lesson_Description(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rLE_DES_Lesson_Description(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_LE_DES int,
    LE_DES_LE_ID int,
    LE_DES_Lesson_Description text,
    LE_DES_ChangedAt timestamp
) AS '
    SELECT
        Metadata_LE_DES,
        LE_DES_LE_ID,
        LE_DES_Lesson_Description,
        LE_DES_ChangedAt
    FROM
        dbo.LE_DES_Lesson_Description
    WHERE
        LE_DES_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- Attribute rewinder -------------------------------------------------------------------------------------------------
-- rLE_STA_Lesson_StartAt rewinding over changing time function
-----------------------------------------------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.rLE_STA_Lesson_StartAt(
    timestamp
);
*/
CREATE OR REPLACE FUNCTION dbo.rLE_STA_Lesson_StartAt(
    changingTimepoint timestamp
) RETURNS TABLE(
    Metadata_LE_STA int,
    LE_STA_LE_ID int,
    LE_STA_Lesson_StartAt timestamp,
    LE_STA_ChangedAt timestamp
) AS '
    SELECT
        Metadata_LE_STA,
        LE_STA_LE_ID,
        LE_STA_Lesson_StartAt,
        LE_STA_ChangedAt
    FROM
        dbo.LE_STA_Lesson_StartAt
    WHERE
        LE_STA_ChangedAt <= changingTimepoint;
' LANGUAGE SQL;
-- ANCHOR TEMPORAL PERSPECTIVES ---------------------------------------------------------------------------------------
--
-- These functions simplify temporal querying by providing a temporal
-- perspective of each anchor. There are four types of perspectives: latest,
-- point-in-time, difference, and now. They also denormalize the anchor, its attributes,
-- and referenced knots from sixth to third normal form.
--
-- The latest perspective shows the latest available information for each anchor.
-- The now perspective shows the information as it is right now.
-- The point-in-time perspective lets you travel through the information to the given timepoint.
--
-- changingTimepoint the point in changing time to travel to
--
-- The difference perspective shows changes between the two given timepoints, and for
-- changes in all or a selection of attributes.
--
-- intervalStart the start of the interval for finding changes
-- intervalEnd the end of the interval for finding changes
-- selection a list of mnemonics for tracked attributes, ie 'MNE MON ICS', or null for all
--
-- Under equivalence all these views default to equivalent = 0, however, corresponding
-- prepended-e perspectives are provided in order to select a specific equivalent.
--
-- equivalent the equivalent for which to retrieve data
--
-- DROP ANCHOR TEMPORAL PERSPECTIVES ----------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.dCO_Course(
    timestamp without time zone, 
    timestamp without time zone, 
    text
);
DROP VIEW IF EXISTS dbo.nCO_Course;
DROP FUNCTION IF EXISTS dbo.pCO_Course(
    timestamp without time zone
);
DROP VIEW IF EXISTS dbo.lCO_Course;
DROP FUNCTION IF EXISTS dbo.dST_Stream(
    timestamp without time zone, 
    timestamp without time zone, 
    text
);
DROP VIEW IF EXISTS dbo.nST_Stream;
DROP FUNCTION IF EXISTS dbo.pST_Stream(
    timestamp without time zone
);
DROP VIEW IF EXISTS dbo.lST_Stream;
DROP FUNCTION IF EXISTS dbo.dMO_Module(
    timestamp without time zone, 
    timestamp without time zone, 
    text
);
DROP VIEW IF EXISTS dbo.nMO_Module;
DROP FUNCTION IF EXISTS dbo.pMO_Module(
    timestamp without time zone
);
DROP VIEW IF EXISTS dbo.lMO_Module;
DROP FUNCTION IF EXISTS dbo.dLE_Lesson(
    timestamp without time zone, 
    timestamp without time zone, 
    text
);
DROP VIEW IF EXISTS dbo.nLE_Lesson;
DROP FUNCTION IF EXISTS dbo.pLE_Lesson(
    timestamp without time zone
);
DROP VIEW IF EXISTS dbo.lLE_Lesson;
*/
-- Latest perspective -------------------------------------------------------------------------------------------------
-- lCO_Course viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.lCO_Course AS
SELECT
    CO.CO_ID,
    CO.Metadata_CO,
    TIT.CO_TIT_CO_ID,
    TIT.Metadata_CO_TIT,
    TIT.CO_TIT_ChangedAt,
    TIT.CO_TIT_Course_Title,
    ICO.CO_ICO_CO_ID,
    ICO.Metadata_CO_ICO,
    ICO.CO_ICO_ChangedAt,
    ICO.CO_ICO_Course_IconUrl,
    DEL.CO_DEL_CO_ID,
    DEL.Metadata_CO_DEL,
    DEL.CO_DEL_Course_Deleted,
    CRE.CO_CRE_CO_ID,
    CRE.Metadata_CO_CRE,
    CRE.CO_CRE_Course_Created,
    AUT.CO_AUT_CO_ID,
    AUT.Metadata_CO_AUT,
    kAUT.AUT_IsAutoCourseEnrole AS CO_AUT_AUT_IsAutoCourseEnrole,
    kAUT.Metadata_AUT AS CO_AUT_Metadata_AUT,
    AUT.CO_AUT_AUT_ID,
    DEM.CO_DEM_CO_ID,
    DEM.Metadata_CO_DEM,
    kDEM.DEM_IsDemoEnrole AS CO_DEM_DEM_IsDemoEnrole,
    kDEM.Metadata_DEM AS CO_DEM_Metadata_DEM,
    DEM.CO_DEM_DEM_ID,
    UPD.CO_UPD_CO_ID,
    UPD.Metadata_CO_UPD,
    UPD.CO_UPD_ChangedAt,
    UPD.CO_UPD_Course_UpdatedAt
FROM
    dbo.CO_Course CO
LEFT JOIN
    dbo.CO_TIT_Course_Title TIT
ON
    TIT.CO_TIT_CO_ID = CO.CO_ID
AND
    TIT.CO_TIT_ChangedAt = (
        SELECT
            max(sub.CO_TIT_ChangedAt)
        FROM
            dbo.CO_TIT_Course_Title sub
        WHERE
            sub.CO_TIT_CO_ID = CO.CO_ID
   )
LEFT JOIN
    dbo.CO_ICO_Course_IconUrl ICO
ON
    ICO.CO_ICO_CO_ID = CO.CO_ID
AND
    ICO.CO_ICO_ChangedAt = (
        SELECT
            max(sub.CO_ICO_ChangedAt)
        FROM
            dbo.CO_ICO_Course_IconUrl sub
        WHERE
            sub.CO_ICO_CO_ID = CO.CO_ID
   )
LEFT JOIN
    dbo.CO_DEL_Course_Deleted DEL
ON
    DEL.CO_DEL_CO_ID = CO.CO_ID
LEFT JOIN
    dbo.CO_CRE_Course_Created CRE
ON
    CRE.CO_CRE_CO_ID = CO.CO_ID
LEFT JOIN
    dbo.CO_AUT_Course_IsAutoCourseEnrole AUT
ON
    AUT.CO_AUT_CO_ID = CO.CO_ID
LEFT JOIN
    dbo.AUT_IsAutoCourseEnrole kAUT
ON
    kAUT.AUT_ID = AUT.CO_AUT_AUT_ID
LEFT JOIN
    dbo.CO_DEM_Course_IsDemoEnrole DEM
ON
    DEM.CO_DEM_CO_ID = CO.CO_ID
LEFT JOIN
    dbo.DEM_IsDemoEnrole kDEM
ON
    kDEM.DEM_ID = DEM.CO_DEM_DEM_ID
LEFT JOIN
    dbo.CO_UPD_Course_UpdatedAt UPD
ON
    UPD.CO_UPD_CO_ID = CO.CO_ID
AND
    UPD.CO_UPD_ChangedAt = (
        SELECT
            max(sub.CO_UPD_ChangedAt)
        FROM
            dbo.CO_UPD_Course_UpdatedAt sub
        WHERE
            sub.CO_UPD_CO_ID = CO.CO_ID
   );
-- Latest perspective -------------------------------------------------------------------------------------------------
-- lST_Stream viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.lST_Stream AS
SELECT
    ST.ST_ID,
    ST.Metadata_ST,
    DEL.ST_DEL_ST_ID,
    DEL.Metadata_ST_DEL,
    DEL.ST_DEL_Stream_DeletedAt,
    CRE.ST_CRE_ST_ID,
    CRE.Metadata_ST_CRE,
    CRE.ST_CRE_Stream_CreatedAt,
    ENA.ST_ENA_ST_ID,
    ENA.Metadata_ST_ENA,
    ENA.ST_ENA_ChangedAt,
    ENA.ST_ENA_Stream_EndAt,
    STA.ST_STA_ST_ID,
    STA.Metadata_ST_STA,
    STA.ST_STA_ChangedAt,
    STA.ST_STA_Stream_StartAt,
    HOM.ST_HOM_ST_ID,
    HOM.Metadata_ST_HOM,
    HOM.ST_HOM_ChangedAt,
    HOM.ST_HOM_Stream_HomeworkDeadlineDays,
    OPE.ST_OPE_ST_ID,
    OPE.Metadata_ST_OPE,
    kOPE.OPE_IsOpen AS ST_OPE_OPE_IsOpen,
    kOPE.Metadata_OPE AS ST_OPE_Metadata_OPE,
    OPE.ST_OPE_OPE_ID,
    NAM.ST_NAM_ST_ID,
    NAM.Metadata_ST_NAM,
    NAM.ST_NAM_ChangedAt,
    NAM.ST_NAM_Stream_Name,
    UPD.ST_UPD_ST_ID,
    UPD.Metadata_ST_UPD,
    UPD.ST_UPD_ChangedAt,
    UPD.ST_UPD_Stream_UpdatedAt
FROM
    dbo.ST_Stream ST
LEFT JOIN
    dbo.ST_DEL_Stream_DeletedAt DEL
ON
    DEL.ST_DEL_ST_ID = ST.ST_ID
LEFT JOIN
    dbo.ST_CRE_Stream_CreatedAt CRE
ON
    CRE.ST_CRE_ST_ID = ST.ST_ID
LEFT JOIN
    dbo.ST_ENA_Stream_EndAt ENA
ON
    ENA.ST_ENA_ST_ID = ST.ST_ID
AND
    ENA.ST_ENA_ChangedAt = (
        SELECT
            max(sub.ST_ENA_ChangedAt)
        FROM
            dbo.ST_ENA_Stream_EndAt sub
        WHERE
            sub.ST_ENA_ST_ID = ST.ST_ID
   )
LEFT JOIN
    dbo.ST_STA_Stream_StartAt STA
ON
    STA.ST_STA_ST_ID = ST.ST_ID
AND
    STA.ST_STA_ChangedAt = (
        SELECT
            max(sub.ST_STA_ChangedAt)
        FROM
            dbo.ST_STA_Stream_StartAt sub
        WHERE
            sub.ST_STA_ST_ID = ST.ST_ID
   )
LEFT JOIN
    dbo.ST_HOM_Stream_HomeworkDeadlineDays HOM
ON
    HOM.ST_HOM_ST_ID = ST.ST_ID
AND
    HOM.ST_HOM_ChangedAt = (
        SELECT
            max(sub.ST_HOM_ChangedAt)
        FROM
            dbo.ST_HOM_Stream_HomeworkDeadlineDays sub
        WHERE
            sub.ST_HOM_ST_ID = ST.ST_ID
   )
LEFT JOIN
    dbo.ST_OPE_Stream_IsOpen OPE
ON
    OPE.ST_OPE_ST_ID = ST.ST_ID
LEFT JOIN
    dbo.OPE_IsOpen kOPE
ON
    kOPE.OPE_ID = OPE.ST_OPE_OPE_ID
LEFT JOIN
    dbo.ST_NAM_Stream_Name NAM
ON
    NAM.ST_NAM_ST_ID = ST.ST_ID
AND
    NAM.ST_NAM_ChangedAt = (
        SELECT
            max(sub.ST_NAM_ChangedAt)
        FROM
            dbo.ST_NAM_Stream_Name sub
        WHERE
            sub.ST_NAM_ST_ID = ST.ST_ID
   )
LEFT JOIN
    dbo.ST_UPD_Stream_UpdatedAt UPD
ON
    UPD.ST_UPD_ST_ID = ST.ST_ID
AND
    UPD.ST_UPD_ChangedAt = (
        SELECT
            max(sub.ST_UPD_ChangedAt)
        FROM
            dbo.ST_UPD_Stream_UpdatedAt sub
        WHERE
            sub.ST_UPD_ST_ID = ST.ST_ID
   );
-- Latest perspective -------------------------------------------------------------------------------------------------
-- lMO_Module viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.lMO_Module AS
SELECT
    MO.MO_ID,
    MO.Metadata_MO,
    UPD.MO_UPD_MO_ID,
    UPD.Metadata_MO_UPD,
    UPD.MO_UPD_ChangedAt,
    UPD.MO_UPD_Module_UpdatedAt,
    TIT.MO_TIT_MO_ID,
    TIT.Metadata_MO_TIT,
    TIT.MO_TIT_ChangedAt,
    TIT.MO_TIT_Module_Title,
    CRE.MO_CRE_MO_ID,
    CRE.Metadata_MO_CRE,
    CRE.MO_CRE_Module_CreatedAt,
    ORD.MO_ORD_MO_ID,
    ORD.Metadata_MO_ORD,
    ORD.MO_ORD_ChangedAt,
    ORD.MO_ORD_Module_OrderInStream,
    DEL.MO_DEL_MO_ID,
    DEL.Metadata_MO_DEL,
    DEL.MO_DEL_Module_DeletedAt
FROM
    dbo.MO_Module MO
LEFT JOIN
    dbo.MO_UPD_Module_UpdatedAt UPD
ON
    UPD.MO_UPD_MO_ID = MO.MO_ID
AND
    UPD.MO_UPD_ChangedAt = (
        SELECT
            max(sub.MO_UPD_ChangedAt)
        FROM
            dbo.MO_UPD_Module_UpdatedAt sub
        WHERE
            sub.MO_UPD_MO_ID = MO.MO_ID
   )
LEFT JOIN
    dbo.MO_TIT_Module_Title TIT
ON
    TIT.MO_TIT_MO_ID = MO.MO_ID
AND
    TIT.MO_TIT_ChangedAt = (
        SELECT
            max(sub.MO_TIT_ChangedAt)
        FROM
            dbo.MO_TIT_Module_Title sub
        WHERE
            sub.MO_TIT_MO_ID = MO.MO_ID
   )
LEFT JOIN
    dbo.MO_CRE_Module_CreatedAt CRE
ON
    CRE.MO_CRE_MO_ID = MO.MO_ID
LEFT JOIN
    dbo.MO_ORD_Module_OrderInStream ORD
ON
    ORD.MO_ORD_MO_ID = MO.MO_ID
AND
    ORD.MO_ORD_ChangedAt = (
        SELECT
            max(sub.MO_ORD_ChangedAt)
        FROM
            dbo.MO_ORD_Module_OrderInStream sub
        WHERE
            sub.MO_ORD_MO_ID = MO.MO_ID
   )
LEFT JOIN
    dbo.MO_DEL_Module_DeletedAt DEL
ON
    DEL.MO_DEL_MO_ID = MO.MO_ID;
-- Latest perspective -------------------------------------------------------------------------------------------------
-- lLE_Lesson viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.lLE_Lesson AS
SELECT
    LE.LE_ID,
    LE.Metadata_LE,
    DEL.LE_DEL_LE_ID,
    DEL.Metadata_LE_DEL,
    DEL.LE_DEL_Lesson_DeletedAt,
    JOI.LE_JOI_LE_ID,
    JOI.Metadata_LE_JOI,
    JOI.LE_JOI_ChangedAt,
    JOI.LE_JOI_Lesson_OnlineLessonJoinUrl,
    REC.LE_REC_LE_ID,
    REC.Metadata_LE_REC,
    REC.LE_REC_ChangedAt,
    REC.LE_REC_Lesson_OnlineLessonRecordingUrl,
    TIT.LE_TIT_LE_ID,
    TIT.Metadata_LE_TIT,
    TIT.LE_TIT_ChangedAt,
    TIT.LE_TIT_Lesson_Title,
    EDT.LE_EDT_LE_ID,
    EDT.Metadata_LE_EDT,
    EDT.LE_EDT_ChangedAt,
    EDT.LE_EDT_Lesson_EndAt,
    HOM.LE_HOM_LE_ID,
    HOM.Metadata_LE_HOM,
    HOM.LE_HOM_ChangedAt,
    HOM.LE_HOM_Lesson_HomeWorkUrl,
    DES.LE_DES_LE_ID,
    DES.Metadata_LE_DES,
    DES.LE_DES_ChangedAt,
    DES.LE_DES_Lesson_Description,
    STA.LE_STA_LE_ID,
    STA.Metadata_LE_STA,
    STA.LE_STA_ChangedAt,
    STA.LE_STA_Lesson_StartAt
FROM
    dbo.LE_Lesson LE
LEFT JOIN
    dbo.LE_DEL_Lesson_DeletedAt DEL
ON
    DEL.LE_DEL_LE_ID = LE.LE_ID
LEFT JOIN
    dbo.LE_JOI_Lesson_OnlineLessonJoinUrl JOI
ON
    JOI.LE_JOI_LE_ID = LE.LE_ID
AND
    JOI.LE_JOI_ChangedAt = (
        SELECT
            max(sub.LE_JOI_ChangedAt)
        FROM
            dbo.LE_JOI_Lesson_OnlineLessonJoinUrl sub
        WHERE
            sub.LE_JOI_LE_ID = LE.LE_ID
   )
LEFT JOIN
    dbo.LE_REC_Lesson_OnlineLessonRecordingUrl REC
ON
    REC.LE_REC_LE_ID = LE.LE_ID
AND
    REC.LE_REC_ChangedAt = (
        SELECT
            max(sub.LE_REC_ChangedAt)
        FROM
            dbo.LE_REC_Lesson_OnlineLessonRecordingUrl sub
        WHERE
            sub.LE_REC_LE_ID = LE.LE_ID
   )
LEFT JOIN
    dbo.LE_TIT_Lesson_Title TIT
ON
    TIT.LE_TIT_LE_ID = LE.LE_ID
AND
    TIT.LE_TIT_ChangedAt = (
        SELECT
            max(sub.LE_TIT_ChangedAt)
        FROM
            dbo.LE_TIT_Lesson_Title sub
        WHERE
            sub.LE_TIT_LE_ID = LE.LE_ID
   )
LEFT JOIN
    dbo.LE_EDT_Lesson_EndAt EDT
ON
    EDT.LE_EDT_LE_ID = LE.LE_ID
AND
    EDT.LE_EDT_ChangedAt = (
        SELECT
            max(sub.LE_EDT_ChangedAt)
        FROM
            dbo.LE_EDT_Lesson_EndAt sub
        WHERE
            sub.LE_EDT_LE_ID = LE.LE_ID
   )
LEFT JOIN
    dbo.LE_HOM_Lesson_HomeWorkUrl HOM
ON
    HOM.LE_HOM_LE_ID = LE.LE_ID
AND
    HOM.LE_HOM_ChangedAt = (
        SELECT
            max(sub.LE_HOM_ChangedAt)
        FROM
            dbo.LE_HOM_Lesson_HomeWorkUrl sub
        WHERE
            sub.LE_HOM_LE_ID = LE.LE_ID
   )
LEFT JOIN
    dbo.LE_DES_Lesson_Description DES
ON
    DES.LE_DES_LE_ID = LE.LE_ID
AND
    DES.LE_DES_ChangedAt = (
        SELECT
            max(sub.LE_DES_ChangedAt)
        FROM
            dbo.LE_DES_Lesson_Description sub
        WHERE
            sub.LE_DES_LE_ID = LE.LE_ID
   )
LEFT JOIN
    dbo.LE_STA_Lesson_StartAt STA
ON
    STA.LE_STA_LE_ID = LE.LE_ID
AND
    STA.LE_STA_ChangedAt = (
        SELECT
            max(sub.LE_STA_ChangedAt)
        FROM
            dbo.LE_STA_Lesson_StartAt sub
        WHERE
            sub.LE_STA_LE_ID = LE.LE_ID
   );
-- Point-in-time perspective ------------------------------------------------------------------------------------------
-- pCO_Course viewed as it was on the given timepoint
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.pCO_Course (
    changingTimepoint timestamp without time zone
)
RETURNS TABLE (
    CO_ID int,
    Metadata_CO int,
    CO_TIT_CO_ID int,
    Metadata_CO_TIT int,
    CO_TIT_ChangedAt timestamp,
    CO_TIT_Course_Title varchar(255),
    CO_ICO_CO_ID int,
    Metadata_CO_ICO int,
    CO_ICO_ChangedAt timestamp,
    CO_ICO_Course_IconUrl varchar(255),
    CO_DEL_CO_ID int,
    Metadata_CO_DEL int,
    CO_DEL_Course_Deleted timestamp(0),
    CO_CRE_CO_ID int,
    Metadata_CO_CRE int,
    CO_CRE_Course_Created timestamp,
    CO_AUT_CO_ID int,
    Metadata_CO_AUT int,
    CO_AUT_AUT_IsAutoCourseEnrole bool,
    CO_AUT_Metadata_AUT int, 
    CO_AUT_AUT_ID int,
    CO_DEM_CO_ID int,
    Metadata_CO_DEM int,
    CO_DEM_DEM_IsDemoEnrole bool,
    CO_DEM_Metadata_DEM int, 
    CO_DEM_DEM_ID int,
    CO_UPD_CO_ID int,
    Metadata_CO_UPD int,
    CO_UPD_ChangedAt timestamp,
    CO_UPD_Course_UpdatedAt timestamp
) AS '
SELECT
    CO.CO_ID,
    CO.Metadata_CO,
    TIT.CO_TIT_CO_ID,
    TIT.Metadata_CO_TIT,
    TIT.CO_TIT_ChangedAt,
    TIT.CO_TIT_Course_Title,
    ICO.CO_ICO_CO_ID,
    ICO.Metadata_CO_ICO,
    ICO.CO_ICO_ChangedAt,
    ICO.CO_ICO_Course_IconUrl,
    DEL.CO_DEL_CO_ID,
    DEL.Metadata_CO_DEL,
    DEL.CO_DEL_Course_Deleted,
    CRE.CO_CRE_CO_ID,
    CRE.Metadata_CO_CRE,
    CRE.CO_CRE_Course_Created,
    AUT.CO_AUT_CO_ID,
    AUT.Metadata_CO_AUT,
    kAUT.AUT_IsAutoCourseEnrole AS CO_AUT_AUT_IsAutoCourseEnrole,
    kAUT.Metadata_AUT AS CO_AUT_Metadata_AUT,
    AUT.CO_AUT_AUT_ID,
    DEM.CO_DEM_CO_ID,
    DEM.Metadata_CO_DEM,
    kDEM.DEM_IsDemoEnrole AS CO_DEM_DEM_IsDemoEnrole,
    kDEM.Metadata_DEM AS CO_DEM_Metadata_DEM,
    DEM.CO_DEM_DEM_ID,
    UPD.CO_UPD_CO_ID,
    UPD.Metadata_CO_UPD,
    UPD.CO_UPD_ChangedAt,
    UPD.CO_UPD_Course_UpdatedAt
FROM
    dbo.CO_Course CO
LEFT JOIN
    dbo.rCO_TIT_Course_Title(CAST(changingTimepoint AS timestamp)) TIT
ON
    TIT.CO_TIT_CO_ID = CO.CO_ID
AND
    TIT.CO_TIT_ChangedAt = (
        SELECT
            max(sub.CO_TIT_ChangedAt)
        FROM
            dbo.rCO_TIT_Course_Title(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.CO_TIT_CO_ID = CO.CO_ID
   )
LEFT JOIN
    dbo.rCO_ICO_Course_IconUrl(CAST(changingTimepoint AS timestamp)) ICO
ON
    ICO.CO_ICO_CO_ID = CO.CO_ID
AND
    ICO.CO_ICO_ChangedAt = (
        SELECT
            max(sub.CO_ICO_ChangedAt)
        FROM
            dbo.rCO_ICO_Course_IconUrl(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.CO_ICO_CO_ID = CO.CO_ID
   )
LEFT JOIN
    dbo.CO_DEL_Course_Deleted DEL
ON
    DEL.CO_DEL_CO_ID = CO.CO_ID
LEFT JOIN
    dbo.CO_CRE_Course_Created CRE
ON
    CRE.CO_CRE_CO_ID = CO.CO_ID
LEFT JOIN
    dbo.CO_AUT_Course_IsAutoCourseEnrole AUT
ON
    AUT.CO_AUT_CO_ID = CO.CO_ID
LEFT JOIN
    dbo.AUT_IsAutoCourseEnrole kAUT
ON
    kAUT.AUT_ID = AUT.CO_AUT_AUT_ID
LEFT JOIN
    dbo.CO_DEM_Course_IsDemoEnrole DEM
ON
    DEM.CO_DEM_CO_ID = CO.CO_ID
LEFT JOIN
    dbo.DEM_IsDemoEnrole kDEM
ON
    kDEM.DEM_ID = DEM.CO_DEM_DEM_ID
LEFT JOIN
    dbo.rCO_UPD_Course_UpdatedAt(CAST(changingTimepoint AS timestamp)) UPD
ON
    UPD.CO_UPD_CO_ID = CO.CO_ID
AND
    UPD.CO_UPD_ChangedAt = (
        SELECT
            max(sub.CO_UPD_ChangedAt)
        FROM
            dbo.rCO_UPD_Course_UpdatedAt(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.CO_UPD_CO_ID = CO.CO_ID
   );
' LANGUAGE SQL;
-- Point-in-time perspective ------------------------------------------------------------------------------------------
-- pST_Stream viewed as it was on the given timepoint
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.pST_Stream (
    changingTimepoint timestamp without time zone
)
RETURNS TABLE (
    ST_ID int,
    Metadata_ST int,
    ST_DEL_ST_ID int,
    Metadata_ST_DEL int,
    ST_DEL_Stream_DeletedAt timestamp,
    ST_CRE_ST_ID int,
    Metadata_ST_CRE int,
    ST_CRE_Stream_CreatedAt timestamp,
    ST_ENA_ST_ID int,
    Metadata_ST_ENA int,
    ST_ENA_ChangedAt timestamp,
    ST_ENA_Stream_EndAt timestamp,
    ST_STA_ST_ID int,
    Metadata_ST_STA int,
    ST_STA_ChangedAt timestamp,
    ST_STA_Stream_StartAt timestamp,
    ST_HOM_ST_ID int,
    Metadata_ST_HOM int,
    ST_HOM_ChangedAt timestamp,
    ST_HOM_Stream_HomeworkDeadlineDays int,
    ST_OPE_ST_ID int,
    Metadata_ST_OPE int,
    ST_OPE_OPE_IsOpen bool,
    ST_OPE_Metadata_OPE int, 
    ST_OPE_OPE_ID int,
    ST_NAM_ST_ID int,
    Metadata_ST_NAM int,
    ST_NAM_ChangedAt timestamp,
    ST_NAM_Stream_Name varchar(255),
    ST_UPD_ST_ID int,
    Metadata_ST_UPD int,
    ST_UPD_ChangedAt timestamp,
    ST_UPD_Stream_UpdatedAt timestamp
) AS '
SELECT
    ST.ST_ID,
    ST.Metadata_ST,
    DEL.ST_DEL_ST_ID,
    DEL.Metadata_ST_DEL,
    DEL.ST_DEL_Stream_DeletedAt,
    CRE.ST_CRE_ST_ID,
    CRE.Metadata_ST_CRE,
    CRE.ST_CRE_Stream_CreatedAt,
    ENA.ST_ENA_ST_ID,
    ENA.Metadata_ST_ENA,
    ENA.ST_ENA_ChangedAt,
    ENA.ST_ENA_Stream_EndAt,
    STA.ST_STA_ST_ID,
    STA.Metadata_ST_STA,
    STA.ST_STA_ChangedAt,
    STA.ST_STA_Stream_StartAt,
    HOM.ST_HOM_ST_ID,
    HOM.Metadata_ST_HOM,
    HOM.ST_HOM_ChangedAt,
    HOM.ST_HOM_Stream_HomeworkDeadlineDays,
    OPE.ST_OPE_ST_ID,
    OPE.Metadata_ST_OPE,
    kOPE.OPE_IsOpen AS ST_OPE_OPE_IsOpen,
    kOPE.Metadata_OPE AS ST_OPE_Metadata_OPE,
    OPE.ST_OPE_OPE_ID,
    NAM.ST_NAM_ST_ID,
    NAM.Metadata_ST_NAM,
    NAM.ST_NAM_ChangedAt,
    NAM.ST_NAM_Stream_Name,
    UPD.ST_UPD_ST_ID,
    UPD.Metadata_ST_UPD,
    UPD.ST_UPD_ChangedAt,
    UPD.ST_UPD_Stream_UpdatedAt
FROM
    dbo.ST_Stream ST
LEFT JOIN
    dbo.ST_DEL_Stream_DeletedAt DEL
ON
    DEL.ST_DEL_ST_ID = ST.ST_ID
LEFT JOIN
    dbo.ST_CRE_Stream_CreatedAt CRE
ON
    CRE.ST_CRE_ST_ID = ST.ST_ID
LEFT JOIN
    dbo.rST_ENA_Stream_EndAt(CAST(changingTimepoint AS timestamp)) ENA
ON
    ENA.ST_ENA_ST_ID = ST.ST_ID
AND
    ENA.ST_ENA_ChangedAt = (
        SELECT
            max(sub.ST_ENA_ChangedAt)
        FROM
            dbo.rST_ENA_Stream_EndAt(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.ST_ENA_ST_ID = ST.ST_ID
   )
LEFT JOIN
    dbo.rST_STA_Stream_StartAt(CAST(changingTimepoint AS timestamp)) STA
ON
    STA.ST_STA_ST_ID = ST.ST_ID
AND
    STA.ST_STA_ChangedAt = (
        SELECT
            max(sub.ST_STA_ChangedAt)
        FROM
            dbo.rST_STA_Stream_StartAt(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.ST_STA_ST_ID = ST.ST_ID
   )
LEFT JOIN
    dbo.rST_HOM_Stream_HomeworkDeadlineDays(CAST(changingTimepoint AS timestamp)) HOM
ON
    HOM.ST_HOM_ST_ID = ST.ST_ID
AND
    HOM.ST_HOM_ChangedAt = (
        SELECT
            max(sub.ST_HOM_ChangedAt)
        FROM
            dbo.rST_HOM_Stream_HomeworkDeadlineDays(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.ST_HOM_ST_ID = ST.ST_ID
   )
LEFT JOIN
    dbo.ST_OPE_Stream_IsOpen OPE
ON
    OPE.ST_OPE_ST_ID = ST.ST_ID
LEFT JOIN
    dbo.OPE_IsOpen kOPE
ON
    kOPE.OPE_ID = OPE.ST_OPE_OPE_ID
LEFT JOIN
    dbo.rST_NAM_Stream_Name(CAST(changingTimepoint AS timestamp)) NAM
ON
    NAM.ST_NAM_ST_ID = ST.ST_ID
AND
    NAM.ST_NAM_ChangedAt = (
        SELECT
            max(sub.ST_NAM_ChangedAt)
        FROM
            dbo.rST_NAM_Stream_Name(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.ST_NAM_ST_ID = ST.ST_ID
   )
LEFT JOIN
    dbo.rST_UPD_Stream_UpdatedAt(CAST(changingTimepoint AS timestamp)) UPD
ON
    UPD.ST_UPD_ST_ID = ST.ST_ID
AND
    UPD.ST_UPD_ChangedAt = (
        SELECT
            max(sub.ST_UPD_ChangedAt)
        FROM
            dbo.rST_UPD_Stream_UpdatedAt(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.ST_UPD_ST_ID = ST.ST_ID
   );
' LANGUAGE SQL;
-- Point-in-time perspective ------------------------------------------------------------------------------------------
-- pMO_Module viewed as it was on the given timepoint
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.pMO_Module (
    changingTimepoint timestamp without time zone
)
RETURNS TABLE (
    MO_ID int,
    Metadata_MO int,
    MO_UPD_MO_ID int,
    Metadata_MO_UPD int,
    MO_UPD_ChangedAt timestamp,
    MO_UPD_Module_UpdatedAt timestamp,
    MO_TIT_MO_ID int,
    Metadata_MO_TIT int,
    MO_TIT_ChangedAt timestamp,
    MO_TIT_Module_Title varchar(255),
    MO_CRE_MO_ID int,
    Metadata_MO_CRE int,
    MO_CRE_Module_CreatedAt timestamp,
    MO_ORD_MO_ID int,
    Metadata_MO_ORD int,
    MO_ORD_ChangedAt timestamp,
    MO_ORD_Module_OrderInStream int,
    MO_DEL_MO_ID int,
    Metadata_MO_DEL int,
    MO_DEL_Module_DeletedAt timestamp
) AS '
SELECT
    MO.MO_ID,
    MO.Metadata_MO,
    UPD.MO_UPD_MO_ID,
    UPD.Metadata_MO_UPD,
    UPD.MO_UPD_ChangedAt,
    UPD.MO_UPD_Module_UpdatedAt,
    TIT.MO_TIT_MO_ID,
    TIT.Metadata_MO_TIT,
    TIT.MO_TIT_ChangedAt,
    TIT.MO_TIT_Module_Title,
    CRE.MO_CRE_MO_ID,
    CRE.Metadata_MO_CRE,
    CRE.MO_CRE_Module_CreatedAt,
    ORD.MO_ORD_MO_ID,
    ORD.Metadata_MO_ORD,
    ORD.MO_ORD_ChangedAt,
    ORD.MO_ORD_Module_OrderInStream,
    DEL.MO_DEL_MO_ID,
    DEL.Metadata_MO_DEL,
    DEL.MO_DEL_Module_DeletedAt
FROM
    dbo.MO_Module MO
LEFT JOIN
    dbo.rMO_UPD_Module_UpdatedAt(CAST(changingTimepoint AS timestamp)) UPD
ON
    UPD.MO_UPD_MO_ID = MO.MO_ID
AND
    UPD.MO_UPD_ChangedAt = (
        SELECT
            max(sub.MO_UPD_ChangedAt)
        FROM
            dbo.rMO_UPD_Module_UpdatedAt(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.MO_UPD_MO_ID = MO.MO_ID
   )
LEFT JOIN
    dbo.rMO_TIT_Module_Title(CAST(changingTimepoint AS timestamp)) TIT
ON
    TIT.MO_TIT_MO_ID = MO.MO_ID
AND
    TIT.MO_TIT_ChangedAt = (
        SELECT
            max(sub.MO_TIT_ChangedAt)
        FROM
            dbo.rMO_TIT_Module_Title(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.MO_TIT_MO_ID = MO.MO_ID
   )
LEFT JOIN
    dbo.MO_CRE_Module_CreatedAt CRE
ON
    CRE.MO_CRE_MO_ID = MO.MO_ID
LEFT JOIN
    dbo.rMO_ORD_Module_OrderInStream(CAST(changingTimepoint AS timestamp)) ORD
ON
    ORD.MO_ORD_MO_ID = MO.MO_ID
AND
    ORD.MO_ORD_ChangedAt = (
        SELECT
            max(sub.MO_ORD_ChangedAt)
        FROM
            dbo.rMO_ORD_Module_OrderInStream(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.MO_ORD_MO_ID = MO.MO_ID
   )
LEFT JOIN
    dbo.MO_DEL_Module_DeletedAt DEL
ON
    DEL.MO_DEL_MO_ID = MO.MO_ID;
' LANGUAGE SQL;
-- Point-in-time perspective ------------------------------------------------------------------------------------------
-- pLE_Lesson viewed as it was on the given timepoint
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.pLE_Lesson (
    changingTimepoint timestamp without time zone
)
RETURNS TABLE (
    LE_ID int,
    Metadata_LE int,
    LE_DEL_LE_ID int,
    Metadata_LE_DEL int,
    LE_DEL_Lesson_DeletedAt timestamp(0),
    LE_JOI_LE_ID int,
    Metadata_LE_JOI int,
    LE_JOI_ChangedAt timestamp,
    LE_JOI_Lesson_OnlineLessonJoinUrl varchar(255),
    LE_REC_LE_ID int,
    Metadata_LE_REC int,
    LE_REC_ChangedAt timestamp,
    LE_REC_Lesson_OnlineLessonRecordingUrl varchar(255),
    LE_TIT_LE_ID int,
    Metadata_LE_TIT int,
    LE_TIT_ChangedAt timestamp,
    LE_TIT_Lesson_Title varchar(255),
    LE_EDT_LE_ID int,
    Metadata_LE_EDT int,
    LE_EDT_ChangedAt timestamp,
    LE_EDT_Lesson_EndAt timestamp,
    LE_HOM_LE_ID int,
    Metadata_LE_HOM int,
    LE_HOM_ChangedAt timestamp,
    LE_HOM_Lesson_HomeWorkUrl varchar(500),
    LE_DES_LE_ID int,
    Metadata_LE_DES int,
    LE_DES_ChangedAt timestamp,
    LE_DES_Lesson_Description text,
    LE_STA_LE_ID int,
    Metadata_LE_STA int,
    LE_STA_ChangedAt timestamp,
    LE_STA_Lesson_StartAt timestamp
) AS '
SELECT
    LE.LE_ID,
    LE.Metadata_LE,
    DEL.LE_DEL_LE_ID,
    DEL.Metadata_LE_DEL,
    DEL.LE_DEL_Lesson_DeletedAt,
    JOI.LE_JOI_LE_ID,
    JOI.Metadata_LE_JOI,
    JOI.LE_JOI_ChangedAt,
    JOI.LE_JOI_Lesson_OnlineLessonJoinUrl,
    REC.LE_REC_LE_ID,
    REC.Metadata_LE_REC,
    REC.LE_REC_ChangedAt,
    REC.LE_REC_Lesson_OnlineLessonRecordingUrl,
    TIT.LE_TIT_LE_ID,
    TIT.Metadata_LE_TIT,
    TIT.LE_TIT_ChangedAt,
    TIT.LE_TIT_Lesson_Title,
    EDT.LE_EDT_LE_ID,
    EDT.Metadata_LE_EDT,
    EDT.LE_EDT_ChangedAt,
    EDT.LE_EDT_Lesson_EndAt,
    HOM.LE_HOM_LE_ID,
    HOM.Metadata_LE_HOM,
    HOM.LE_HOM_ChangedAt,
    HOM.LE_HOM_Lesson_HomeWorkUrl,
    DES.LE_DES_LE_ID,
    DES.Metadata_LE_DES,
    DES.LE_DES_ChangedAt,
    DES.LE_DES_Lesson_Description,
    STA.LE_STA_LE_ID,
    STA.Metadata_LE_STA,
    STA.LE_STA_ChangedAt,
    STA.LE_STA_Lesson_StartAt
FROM
    dbo.LE_Lesson LE
LEFT JOIN
    dbo.LE_DEL_Lesson_DeletedAt DEL
ON
    DEL.LE_DEL_LE_ID = LE.LE_ID
LEFT JOIN
    dbo.rLE_JOI_Lesson_OnlineLessonJoinUrl(CAST(changingTimepoint AS timestamp)) JOI
ON
    JOI.LE_JOI_LE_ID = LE.LE_ID
AND
    JOI.LE_JOI_ChangedAt = (
        SELECT
            max(sub.LE_JOI_ChangedAt)
        FROM
            dbo.rLE_JOI_Lesson_OnlineLessonJoinUrl(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.LE_JOI_LE_ID = LE.LE_ID
   )
LEFT JOIN
    dbo.rLE_REC_Lesson_OnlineLessonRecordingUrl(CAST(changingTimepoint AS timestamp)) REC
ON
    REC.LE_REC_LE_ID = LE.LE_ID
AND
    REC.LE_REC_ChangedAt = (
        SELECT
            max(sub.LE_REC_ChangedAt)
        FROM
            dbo.rLE_REC_Lesson_OnlineLessonRecordingUrl(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.LE_REC_LE_ID = LE.LE_ID
   )
LEFT JOIN
    dbo.rLE_TIT_Lesson_Title(CAST(changingTimepoint AS timestamp)) TIT
ON
    TIT.LE_TIT_LE_ID = LE.LE_ID
AND
    TIT.LE_TIT_ChangedAt = (
        SELECT
            max(sub.LE_TIT_ChangedAt)
        FROM
            dbo.rLE_TIT_Lesson_Title(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.LE_TIT_LE_ID = LE.LE_ID
   )
LEFT JOIN
    dbo.rLE_EDT_Lesson_EndAt(CAST(changingTimepoint AS timestamp)) EDT
ON
    EDT.LE_EDT_LE_ID = LE.LE_ID
AND
    EDT.LE_EDT_ChangedAt = (
        SELECT
            max(sub.LE_EDT_ChangedAt)
        FROM
            dbo.rLE_EDT_Lesson_EndAt(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.LE_EDT_LE_ID = LE.LE_ID
   )
LEFT JOIN
    dbo.rLE_HOM_Lesson_HomeWorkUrl(CAST(changingTimepoint AS timestamp)) HOM
ON
    HOM.LE_HOM_LE_ID = LE.LE_ID
AND
    HOM.LE_HOM_ChangedAt = (
        SELECT
            max(sub.LE_HOM_ChangedAt)
        FROM
            dbo.rLE_HOM_Lesson_HomeWorkUrl(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.LE_HOM_LE_ID = LE.LE_ID
   )
LEFT JOIN
    dbo.rLE_DES_Lesson_Description(CAST(changingTimepoint AS timestamp)) DES
ON
    DES.LE_DES_LE_ID = LE.LE_ID
AND
    DES.LE_DES_ChangedAt = (
        SELECT
            max(sub.LE_DES_ChangedAt)
        FROM
            dbo.rLE_DES_Lesson_Description(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.LE_DES_LE_ID = LE.LE_ID
   )
LEFT JOIN
    dbo.rLE_STA_Lesson_StartAt(CAST(changingTimepoint AS timestamp)) STA
ON
    STA.LE_STA_LE_ID = LE.LE_ID
AND
    STA.LE_STA_ChangedAt = (
        SELECT
            max(sub.LE_STA_ChangedAt)
        FROM
            dbo.rLE_STA_Lesson_StartAt(CAST(changingTimepoint AS timestamp)) sub
        WHERE
            sub.LE_STA_LE_ID = LE.LE_ID
   );
' LANGUAGE SQL;
-- Now perspective ----------------------------------------------------------------------------------------------------
-- nCO_Course viewed as it currently is (cannot include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.nCO_Course AS
SELECT
    *
FROM
    dbo.pCO_Course(LOCALTIMESTAMP);
-- Now perspective ----------------------------------------------------------------------------------------------------
-- nST_Stream viewed as it currently is (cannot include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.nST_Stream AS
SELECT
    *
FROM
    dbo.pST_Stream(LOCALTIMESTAMP);
-- Now perspective ----------------------------------------------------------------------------------------------------
-- nMO_Module viewed as it currently is (cannot include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.nMO_Module AS
SELECT
    *
FROM
    dbo.pMO_Module(LOCALTIMESTAMP);
-- Now perspective ----------------------------------------------------------------------------------------------------
-- nLE_Lesson viewed as it currently is (cannot include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.nLE_Lesson AS
SELECT
    *
FROM
    dbo.pLE_Lesson(LOCALTIMESTAMP);
-- Difference perspective ---------------------------------------------------------------------------------------------
-- dCO_Course showing all differences between the given timepoints and optionally for a subset of attributes
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.dCO_Course (
    intervalStart timestamp without time zone,
    intervalEnd timestamp without time zone,
    selection text = null
) RETURNS TABLE (
    inspectedTimepoint timestamp without time zone,
    mnemonic text,
    CO_ID int,
    Metadata_CO int,
    CO_TIT_CO_ID int,
    Metadata_CO_TIT int,
    CO_TIT_ChangedAt timestamp,
    CO_TIT_Course_Title varchar(255),
    CO_ICO_CO_ID int,
    Metadata_CO_ICO int,
    CO_ICO_ChangedAt timestamp,
    CO_ICO_Course_IconUrl varchar(255),
    CO_DEL_CO_ID int,
    Metadata_CO_DEL int,
    CO_DEL_Course_Deleted timestamp(0),
    CO_CRE_CO_ID int,
    Metadata_CO_CRE int,
    CO_CRE_Course_Created timestamp,
    CO_AUT_CO_ID int,
    Metadata_CO_AUT int,
    CO_AUT_AUT_IsAutoCourseEnrole bool,
    CO_AUT_Metadata_AUT int, 
    CO_AUT_AUT_ID int,
    CO_DEM_CO_ID int,
    Metadata_CO_DEM int,
    CO_DEM_DEM_IsDemoEnrole bool,
    CO_DEM_Metadata_DEM int, 
    CO_DEM_DEM_ID int,
    CO_UPD_CO_ID int,
    Metadata_CO_UPD int,
    CO_UPD_ChangedAt timestamp,
    CO_UPD_Course_UpdatedAt timestamp
) AS '
SELECT
    timepoints.inspectedTimepoint,
    timepoints.mnemonic,
    pCO.*
FROM (
    SELECT DISTINCT
        CO_TIT_CO_ID AS CO_ID,
        CAST(CO_TIT_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''TIT'' AS mnemonic
    FROM
        dbo.CO_TIT_Course_Title
    WHERE
        (selection is null OR selection like ''%TIT%'')
    AND
        CO_TIT_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        CO_ICO_CO_ID AS CO_ID,
        CAST(CO_ICO_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''ICO'' AS mnemonic
    FROM
        dbo.CO_ICO_Course_IconUrl
    WHERE
        (selection is null OR selection like ''%ICO%'')
    AND
        CO_ICO_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        CO_UPD_CO_ID AS CO_ID,
        CAST(CO_UPD_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''UPD'' AS mnemonic
    FROM
        dbo.CO_UPD_Course_UpdatedAt
    WHERE
        (selection is null OR selection like ''%UPD%'')
    AND
        CO_UPD_ChangedAt BETWEEN intervalStart AND intervalEnd
) timepoints
CROSS JOIN LATERAL
    dbo.pCO_Course(timepoints.inspectedTimepoint) pCO
WHERE
    pCO.CO_ID = timepoints.CO_ID;
' LANGUAGE SQL;
-- Difference perspective ---------------------------------------------------------------------------------------------
-- dST_Stream showing all differences between the given timepoints and optionally for a subset of attributes
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.dST_Stream (
    intervalStart timestamp without time zone,
    intervalEnd timestamp without time zone,
    selection text = null
) RETURNS TABLE (
    inspectedTimepoint timestamp without time zone,
    mnemonic text,
    ST_ID int,
    Metadata_ST int,
    ST_DEL_ST_ID int,
    Metadata_ST_DEL int,
    ST_DEL_Stream_DeletedAt timestamp,
    ST_CRE_ST_ID int,
    Metadata_ST_CRE int,
    ST_CRE_Stream_CreatedAt timestamp,
    ST_ENA_ST_ID int,
    Metadata_ST_ENA int,
    ST_ENA_ChangedAt timestamp,
    ST_ENA_Stream_EndAt timestamp,
    ST_STA_ST_ID int,
    Metadata_ST_STA int,
    ST_STA_ChangedAt timestamp,
    ST_STA_Stream_StartAt timestamp,
    ST_HOM_ST_ID int,
    Metadata_ST_HOM int,
    ST_HOM_ChangedAt timestamp,
    ST_HOM_Stream_HomeworkDeadlineDays int,
    ST_OPE_ST_ID int,
    Metadata_ST_OPE int,
    ST_OPE_OPE_IsOpen bool,
    ST_OPE_Metadata_OPE int, 
    ST_OPE_OPE_ID int,
    ST_NAM_ST_ID int,
    Metadata_ST_NAM int,
    ST_NAM_ChangedAt timestamp,
    ST_NAM_Stream_Name varchar(255),
    ST_UPD_ST_ID int,
    Metadata_ST_UPD int,
    ST_UPD_ChangedAt timestamp,
    ST_UPD_Stream_UpdatedAt timestamp
) AS '
SELECT
    timepoints.inspectedTimepoint,
    timepoints.mnemonic,
    pST.*
FROM (
    SELECT DISTINCT
        ST_ENA_ST_ID AS ST_ID,
        CAST(ST_ENA_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''ENA'' AS mnemonic
    FROM
        dbo.ST_ENA_Stream_EndAt
    WHERE
        (selection is null OR selection like ''%ENA%'')
    AND
        ST_ENA_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        ST_STA_ST_ID AS ST_ID,
        CAST(ST_STA_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''STA'' AS mnemonic
    FROM
        dbo.ST_STA_Stream_StartAt
    WHERE
        (selection is null OR selection like ''%STA%'')
    AND
        ST_STA_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        ST_HOM_ST_ID AS ST_ID,
        CAST(ST_HOM_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''HOM'' AS mnemonic
    FROM
        dbo.ST_HOM_Stream_HomeworkDeadlineDays
    WHERE
        (selection is null OR selection like ''%HOM%'')
    AND
        ST_HOM_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        ST_NAM_ST_ID AS ST_ID,
        CAST(ST_NAM_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''NAM'' AS mnemonic
    FROM
        dbo.ST_NAM_Stream_Name
    WHERE
        (selection is null OR selection like ''%NAM%'')
    AND
        ST_NAM_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        ST_UPD_ST_ID AS ST_ID,
        CAST(ST_UPD_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''UPD'' AS mnemonic
    FROM
        dbo.ST_UPD_Stream_UpdatedAt
    WHERE
        (selection is null OR selection like ''%UPD%'')
    AND
        ST_UPD_ChangedAt BETWEEN intervalStart AND intervalEnd
) timepoints
CROSS JOIN LATERAL
    dbo.pST_Stream(timepoints.inspectedTimepoint) pST
WHERE
    pST.ST_ID = timepoints.ST_ID;
' LANGUAGE SQL;
-- Difference perspective ---------------------------------------------------------------------------------------------
-- dMO_Module showing all differences between the given timepoints and optionally for a subset of attributes
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.dMO_Module (
    intervalStart timestamp without time zone,
    intervalEnd timestamp without time zone,
    selection text = null
) RETURNS TABLE (
    inspectedTimepoint timestamp without time zone,
    mnemonic text,
    MO_ID int,
    Metadata_MO int,
    MO_UPD_MO_ID int,
    Metadata_MO_UPD int,
    MO_UPD_ChangedAt timestamp,
    MO_UPD_Module_UpdatedAt timestamp,
    MO_TIT_MO_ID int,
    Metadata_MO_TIT int,
    MO_TIT_ChangedAt timestamp,
    MO_TIT_Module_Title varchar(255),
    MO_CRE_MO_ID int,
    Metadata_MO_CRE int,
    MO_CRE_Module_CreatedAt timestamp,
    MO_ORD_MO_ID int,
    Metadata_MO_ORD int,
    MO_ORD_ChangedAt timestamp,
    MO_ORD_Module_OrderInStream int,
    MO_DEL_MO_ID int,
    Metadata_MO_DEL int,
    MO_DEL_Module_DeletedAt timestamp
) AS '
SELECT
    timepoints.inspectedTimepoint,
    timepoints.mnemonic,
    pMO.*
FROM (
    SELECT DISTINCT
        MO_UPD_MO_ID AS MO_ID,
        CAST(MO_UPD_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''UPD'' AS mnemonic
    FROM
        dbo.MO_UPD_Module_UpdatedAt
    WHERE
        (selection is null OR selection like ''%UPD%'')
    AND
        MO_UPD_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        MO_TIT_MO_ID AS MO_ID,
        CAST(MO_TIT_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''TIT'' AS mnemonic
    FROM
        dbo.MO_TIT_Module_Title
    WHERE
        (selection is null OR selection like ''%TIT%'')
    AND
        MO_TIT_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        MO_ORD_MO_ID AS MO_ID,
        CAST(MO_ORD_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''ORD'' AS mnemonic
    FROM
        dbo.MO_ORD_Module_OrderInStream
    WHERE
        (selection is null OR selection like ''%ORD%'')
    AND
        MO_ORD_ChangedAt BETWEEN intervalStart AND intervalEnd
) timepoints
CROSS JOIN LATERAL
    dbo.pMO_Module(timepoints.inspectedTimepoint) pMO
WHERE
    pMO.MO_ID = timepoints.MO_ID;
' LANGUAGE SQL;
-- Difference perspective ---------------------------------------------------------------------------------------------
-- dLE_Lesson showing all differences between the given timepoints and optionally for a subset of attributes
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.dLE_Lesson (
    intervalStart timestamp without time zone,
    intervalEnd timestamp without time zone,
    selection text = null
) RETURNS TABLE (
    inspectedTimepoint timestamp without time zone,
    mnemonic text,
    LE_ID int,
    Metadata_LE int,
    LE_DEL_LE_ID int,
    Metadata_LE_DEL int,
    LE_DEL_Lesson_DeletedAt timestamp(0),
    LE_JOI_LE_ID int,
    Metadata_LE_JOI int,
    LE_JOI_ChangedAt timestamp,
    LE_JOI_Lesson_OnlineLessonJoinUrl varchar(255),
    LE_REC_LE_ID int,
    Metadata_LE_REC int,
    LE_REC_ChangedAt timestamp,
    LE_REC_Lesson_OnlineLessonRecordingUrl varchar(255),
    LE_TIT_LE_ID int,
    Metadata_LE_TIT int,
    LE_TIT_ChangedAt timestamp,
    LE_TIT_Lesson_Title varchar(255),
    LE_EDT_LE_ID int,
    Metadata_LE_EDT int,
    LE_EDT_ChangedAt timestamp,
    LE_EDT_Lesson_EndAt timestamp,
    LE_HOM_LE_ID int,
    Metadata_LE_HOM int,
    LE_HOM_ChangedAt timestamp,
    LE_HOM_Lesson_HomeWorkUrl varchar(500),
    LE_DES_LE_ID int,
    Metadata_LE_DES int,
    LE_DES_ChangedAt timestamp,
    LE_DES_Lesson_Description text,
    LE_STA_LE_ID int,
    Metadata_LE_STA int,
    LE_STA_ChangedAt timestamp,
    LE_STA_Lesson_StartAt timestamp
) AS '
SELECT
    timepoints.inspectedTimepoint,
    timepoints.mnemonic,
    pLE.*
FROM (
    SELECT DISTINCT
        LE_JOI_LE_ID AS LE_ID,
        CAST(LE_JOI_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''JOI'' AS mnemonic
    FROM
        dbo.LE_JOI_Lesson_OnlineLessonJoinUrl
    WHERE
        (selection is null OR selection like ''%JOI%'')
    AND
        LE_JOI_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        LE_REC_LE_ID AS LE_ID,
        CAST(LE_REC_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''REC'' AS mnemonic
    FROM
        dbo.LE_REC_Lesson_OnlineLessonRecordingUrl
    WHERE
        (selection is null OR selection like ''%REC%'')
    AND
        LE_REC_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        LE_TIT_LE_ID AS LE_ID,
        CAST(LE_TIT_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''TIT'' AS mnemonic
    FROM
        dbo.LE_TIT_Lesson_Title
    WHERE
        (selection is null OR selection like ''%TIT%'')
    AND
        LE_TIT_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        LE_EDT_LE_ID AS LE_ID,
        CAST(LE_EDT_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''EDT'' AS mnemonic
    FROM
        dbo.LE_EDT_Lesson_EndAt
    WHERE
        (selection is null OR selection like ''%EDT%'')
    AND
        LE_EDT_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        LE_HOM_LE_ID AS LE_ID,
        CAST(LE_HOM_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''HOM'' AS mnemonic
    FROM
        dbo.LE_HOM_Lesson_HomeWorkUrl
    WHERE
        (selection is null OR selection like ''%HOM%'')
    AND
        LE_HOM_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        LE_DES_LE_ID AS LE_ID,
        CAST(LE_DES_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''DES'' AS mnemonic
    FROM
        dbo.LE_DES_Lesson_Description
    WHERE
        (selection is null OR selection like ''%DES%'')
    AND
        LE_DES_ChangedAt BETWEEN intervalStart AND intervalEnd
    UNION
    SELECT DISTINCT
        LE_STA_LE_ID AS LE_ID,
        CAST(LE_STA_ChangedAt AS timestamp without time zone) AS inspectedTimepoint,
        ''STA'' AS mnemonic
    FROM
        dbo.LE_STA_Lesson_StartAt
    WHERE
        (selection is null OR selection like ''%STA%'')
    AND
        LE_STA_ChangedAt BETWEEN intervalStart AND intervalEnd
) timepoints
CROSS JOIN LATERAL
    dbo.pLE_Lesson(timepoints.inspectedTimepoint) pLE
WHERE
    pLE.LE_ID = timepoints.LE_ID;
' LANGUAGE SQL;
-- ATTRIBUTE TRIGGERS -------------------------------------------------------------------------------------------------
--
-- The following triggers on the attributes make them behave like tables.
-- They will ensure that such operations are propagated to the underlying tables
-- in a consistent way. Default values are used for some columns if not provided
-- by the corresponding SQL statements.
--
-- For idempotent attributes, only changes that represent a value different from
-- the previous or following value are stored. Others are silently ignored in
-- order to avoid unnecessary temporal duplicates.
--
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbCO_TIT_Course_Title ON dbo.CO_TIT_Course_Title;
-- DROP FUNCTION IF EXISTS dbo.tcbCO_TIT_Course_Title();
CREATE OR REPLACE FUNCTION dbo.tcbCO_TIT_Course_Title() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_CO_TIT_Course_Title (
            CO_TIT_CO_ID int not null,
            Metadata_CO_TIT int not null,
            CO_TIT_ChangedAt timestamp not null,
            CO_TIT_Course_Title varchar(255) not null,
            CO_TIT_Version bigint not null,
            CO_TIT_StatementType char(1) not null,
            primary key(
                CO_TIT_Version,
                CO_TIT_CO_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbCO_TIT_Course_Title
BEFORE INSERT ON dbo.CO_TIT_Course_Title
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbCO_TIT_Course_Title();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciCO_TIT_Course_Title ON dbo.CO_TIT_Course_Title;
-- DROP FUNCTION IF EXISTS dbo.tciCO_TIT_Course_Title();
CREATE OR REPLACE FUNCTION dbo.tciCO_TIT_Course_Title() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_CO_TIT_Course_Title
        SELECT
            NEW.CO_TIT_CO_ID,
            NEW.Metadata_CO_TIT,
            NEW.CO_TIT_ChangedAt,
            NEW.CO_TIT_Course_Title,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciCO_TIT_Course_Title
INSTEAD OF INSERT ON dbo.CO_TIT_Course_Title
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciCO_TIT_Course_Title();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaCO_TIT_Course_Title ON dbo.CO_TIT_Course_Title;
-- DROP FUNCTION IF EXISTS dbo.tcaCO_TIT_Course_Title();
CREATE OR REPLACE FUNCTION dbo.tcaCO_TIT_Course_Title() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_CO_TIT_Course_Title
    SET CO_TIT_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    CO_TIT_CO_ID
                ORDER BY
                    CO_TIT_ChangedAt ASC
            ) AS rank,
            CO_TIT_CO_ID AS pk
        FROM _tmp_CO_TIT_Course_Title
    ) AS v
    WHERE CO_TIT_CO_ID = v.pk
    AND CO_TIT_Version = 0;
    -- find max version
    SELECT
        MAX(CO_TIT_Version) INTO maxVersion
    FROM
        _tmp_CO_TIT_Course_Title;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_CO_TIT_Course_Title
        SET
            CO_TIT_StatementType =
                CASE
                    WHEN TIT.CO_TIT_CO_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfCO_TIT_Course_Title(
                        v.CO_TIT_CO_ID,
                        v.CO_TIT_Course_Title,
                        v.CO_TIT_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_CO_TIT_Course_Title v
        LEFT JOIN
            dbo._CO_TIT_Course_Title TIT
        ON
            TIT.CO_TIT_CO_ID = v.CO_TIT_CO_ID
        AND
            TIT.CO_TIT_ChangedAt = v.CO_TIT_ChangedAt
        AND
            TIT.CO_TIT_Course_Title = v.CO_TIT_Course_Title
        WHERE
            v.CO_TIT_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._CO_TIT_Course_Title (
            CO_TIT_CO_ID,
            Metadata_CO_TIT,
            CO_TIT_ChangedAt,
            CO_TIT_Course_Title
        )
        SELECT
            CO_TIT_CO_ID,
            Metadata_CO_TIT,
            CO_TIT_ChangedAt,
            CO_TIT_Course_Title
        FROM
            _tmp_CO_TIT_Course_Title
        WHERE
            CO_TIT_Version = currentVersion
        AND
            CO_TIT_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_CO_TIT_Course_Title;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaCO_TIT_Course_Title
AFTER INSERT ON dbo.CO_TIT_Course_Title
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaCO_TIT_Course_Title();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbCO_ICO_Course_IconUrl ON dbo.CO_ICO_Course_IconUrl;
-- DROP FUNCTION IF EXISTS dbo.tcbCO_ICO_Course_IconUrl();
CREATE OR REPLACE FUNCTION dbo.tcbCO_ICO_Course_IconUrl() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_CO_ICO_Course_IconUrl (
            CO_ICO_CO_ID int not null,
            Metadata_CO_ICO int not null,
            CO_ICO_ChangedAt timestamp not null,
            CO_ICO_Course_IconUrl varchar(255) not null,
            CO_ICO_Version bigint not null,
            CO_ICO_StatementType char(1) not null,
            primary key(
                CO_ICO_Version,
                CO_ICO_CO_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbCO_ICO_Course_IconUrl
BEFORE INSERT ON dbo.CO_ICO_Course_IconUrl
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbCO_ICO_Course_IconUrl();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciCO_ICO_Course_IconUrl ON dbo.CO_ICO_Course_IconUrl;
-- DROP FUNCTION IF EXISTS dbo.tciCO_ICO_Course_IconUrl();
CREATE OR REPLACE FUNCTION dbo.tciCO_ICO_Course_IconUrl() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_CO_ICO_Course_IconUrl
        SELECT
            NEW.CO_ICO_CO_ID,
            NEW.Metadata_CO_ICO,
            NEW.CO_ICO_ChangedAt,
            NEW.CO_ICO_Course_IconUrl,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciCO_ICO_Course_IconUrl
INSTEAD OF INSERT ON dbo.CO_ICO_Course_IconUrl
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciCO_ICO_Course_IconUrl();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaCO_ICO_Course_IconUrl ON dbo.CO_ICO_Course_IconUrl;
-- DROP FUNCTION IF EXISTS dbo.tcaCO_ICO_Course_IconUrl();
CREATE OR REPLACE FUNCTION dbo.tcaCO_ICO_Course_IconUrl() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_CO_ICO_Course_IconUrl
    SET CO_ICO_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    CO_ICO_CO_ID
                ORDER BY
                    CO_ICO_ChangedAt ASC
            ) AS rank,
            CO_ICO_CO_ID AS pk
        FROM _tmp_CO_ICO_Course_IconUrl
    ) AS v
    WHERE CO_ICO_CO_ID = v.pk
    AND CO_ICO_Version = 0;
    -- find max version
    SELECT
        MAX(CO_ICO_Version) INTO maxVersion
    FROM
        _tmp_CO_ICO_Course_IconUrl;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_CO_ICO_Course_IconUrl
        SET
            CO_ICO_StatementType =
                CASE
                    WHEN ICO.CO_ICO_CO_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfCO_ICO_Course_IconUrl(
                        v.CO_ICO_CO_ID,
                        v.CO_ICO_Course_IconUrl,
                        v.CO_ICO_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_CO_ICO_Course_IconUrl v
        LEFT JOIN
            dbo._CO_ICO_Course_IconUrl ICO
        ON
            ICO.CO_ICO_CO_ID = v.CO_ICO_CO_ID
        AND
            ICO.CO_ICO_ChangedAt = v.CO_ICO_ChangedAt
        AND
            ICO.CO_ICO_Course_IconUrl = v.CO_ICO_Course_IconUrl
        WHERE
            v.CO_ICO_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._CO_ICO_Course_IconUrl (
            CO_ICO_CO_ID,
            Metadata_CO_ICO,
            CO_ICO_ChangedAt,
            CO_ICO_Course_IconUrl
        )
        SELECT
            CO_ICO_CO_ID,
            Metadata_CO_ICO,
            CO_ICO_ChangedAt,
            CO_ICO_Course_IconUrl
        FROM
            _tmp_CO_ICO_Course_IconUrl
        WHERE
            CO_ICO_Version = currentVersion
        AND
            CO_ICO_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_CO_ICO_Course_IconUrl;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaCO_ICO_Course_IconUrl
AFTER INSERT ON dbo.CO_ICO_Course_IconUrl
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaCO_ICO_Course_IconUrl();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbCO_DEL_Course_Deleted ON dbo.CO_DEL_Course_Deleted;
-- DROP FUNCTION IF EXISTS dbo.tcbCO_DEL_Course_Deleted();
CREATE OR REPLACE FUNCTION dbo.tcbCO_DEL_Course_Deleted() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_CO_DEL_Course_Deleted (
            CO_DEL_CO_ID int not null,
            Metadata_CO_DEL int not null,
            CO_DEL_Course_Deleted timestamp(0) not null,
            CO_DEL_Version bigint not null,
            CO_DEL_StatementType char(1) not null,
            primary key(
                CO_DEL_Version,
                CO_DEL_CO_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbCO_DEL_Course_Deleted
BEFORE INSERT ON dbo.CO_DEL_Course_Deleted
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbCO_DEL_Course_Deleted();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciCO_DEL_Course_Deleted ON dbo.CO_DEL_Course_Deleted;
-- DROP FUNCTION IF EXISTS dbo.tciCO_DEL_Course_Deleted();
CREATE OR REPLACE FUNCTION dbo.tciCO_DEL_Course_Deleted() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_CO_DEL_Course_Deleted
        SELECT
            NEW.CO_DEL_CO_ID,
            NEW.Metadata_CO_DEL,
            NEW.CO_DEL_Course_Deleted,
            1,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciCO_DEL_Course_Deleted
INSTEAD OF INSERT ON dbo.CO_DEL_Course_Deleted
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciCO_DEL_Course_Deleted();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaCO_DEL_Course_Deleted ON dbo.CO_DEL_Course_Deleted;
-- DROP FUNCTION IF EXISTS dbo.tcaCO_DEL_Course_Deleted();
CREATE OR REPLACE FUNCTION dbo.tcaCO_DEL_Course_Deleted() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find max version
    SELECT
        MAX(CO_DEL_Version) INTO maxVersion
    FROM
        _tmp_CO_DEL_Course_Deleted;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_CO_DEL_Course_Deleted
        SET
            CO_DEL_StatementType =
                CASE
                    WHEN DEL.CO_DEL_CO_ID is not null
                    THEN ''D'' -- duplicate
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_CO_DEL_Course_Deleted v
        LEFT JOIN
            dbo._CO_DEL_Course_Deleted DEL
        ON
            DEL.CO_DEL_CO_ID = v.CO_DEL_CO_ID
        AND
            DEL.CO_DEL_Course_Deleted = v.CO_DEL_Course_Deleted
        WHERE
            v.CO_DEL_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._CO_DEL_Course_Deleted (
            CO_DEL_CO_ID,
            Metadata_CO_DEL,
            CO_DEL_Course_Deleted
        )
        SELECT
            CO_DEL_CO_ID,
            Metadata_CO_DEL,
            CO_DEL_Course_Deleted
        FROM
            _tmp_CO_DEL_Course_Deleted
        WHERE
            CO_DEL_Version = currentVersion
        AND
            CO_DEL_StatementType in (''N'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_CO_DEL_Course_Deleted;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaCO_DEL_Course_Deleted
AFTER INSERT ON dbo.CO_DEL_Course_Deleted
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaCO_DEL_Course_Deleted();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbCO_CRE_Course_Created ON dbo.CO_CRE_Course_Created;
-- DROP FUNCTION IF EXISTS dbo.tcbCO_CRE_Course_Created();
CREATE OR REPLACE FUNCTION dbo.tcbCO_CRE_Course_Created() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_CO_CRE_Course_Created (
            CO_CRE_CO_ID int not null,
            Metadata_CO_CRE int not null,
            CO_CRE_Course_Created timestamp not null,
            CO_CRE_Version bigint not null,
            CO_CRE_StatementType char(1) not null,
            primary key(
                CO_CRE_Version,
                CO_CRE_CO_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbCO_CRE_Course_Created
BEFORE INSERT ON dbo.CO_CRE_Course_Created
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbCO_CRE_Course_Created();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciCO_CRE_Course_Created ON dbo.CO_CRE_Course_Created;
-- DROP FUNCTION IF EXISTS dbo.tciCO_CRE_Course_Created();
CREATE OR REPLACE FUNCTION dbo.tciCO_CRE_Course_Created() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_CO_CRE_Course_Created
        SELECT
            NEW.CO_CRE_CO_ID,
            NEW.Metadata_CO_CRE,
            NEW.CO_CRE_Course_Created,
            1,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciCO_CRE_Course_Created
INSTEAD OF INSERT ON dbo.CO_CRE_Course_Created
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciCO_CRE_Course_Created();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaCO_CRE_Course_Created ON dbo.CO_CRE_Course_Created;
-- DROP FUNCTION IF EXISTS dbo.tcaCO_CRE_Course_Created();
CREATE OR REPLACE FUNCTION dbo.tcaCO_CRE_Course_Created() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find max version
    SELECT
        MAX(CO_CRE_Version) INTO maxVersion
    FROM
        _tmp_CO_CRE_Course_Created;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_CO_CRE_Course_Created
        SET
            CO_CRE_StatementType =
                CASE
                    WHEN CRE.CO_CRE_CO_ID is not null
                    THEN ''D'' -- duplicate
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_CO_CRE_Course_Created v
        LEFT JOIN
            dbo._CO_CRE_Course_Created CRE
        ON
            CRE.CO_CRE_CO_ID = v.CO_CRE_CO_ID
        AND
            CRE.CO_CRE_Course_Created = v.CO_CRE_Course_Created
        WHERE
            v.CO_CRE_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._CO_CRE_Course_Created (
            CO_CRE_CO_ID,
            Metadata_CO_CRE,
            CO_CRE_Course_Created
        )
        SELECT
            CO_CRE_CO_ID,
            Metadata_CO_CRE,
            CO_CRE_Course_Created
        FROM
            _tmp_CO_CRE_Course_Created
        WHERE
            CO_CRE_Version = currentVersion
        AND
            CO_CRE_StatementType in (''N'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_CO_CRE_Course_Created;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaCO_CRE_Course_Created
AFTER INSERT ON dbo.CO_CRE_Course_Created
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaCO_CRE_Course_Created();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbCO_AUT_Course_IsAutoCourseEnrole ON dbo.CO_AUT_Course_IsAutoCourseEnrole;
-- DROP FUNCTION IF EXISTS dbo.tcbCO_AUT_Course_IsAutoCourseEnrole();
CREATE OR REPLACE FUNCTION dbo.tcbCO_AUT_Course_IsAutoCourseEnrole() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_CO_AUT_Course_IsAutoCourseEnrole (
            CO_AUT_CO_ID int not null,
            Metadata_CO_AUT int not null,
            CO_AUT_AUT_ID int not null, 
            CO_AUT_Version bigint not null,
            CO_AUT_StatementType char(1) not null,
            primary key(
                CO_AUT_Version,
                CO_AUT_CO_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbCO_AUT_Course_IsAutoCourseEnrole
BEFORE INSERT ON dbo.CO_AUT_Course_IsAutoCourseEnrole
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbCO_AUT_Course_IsAutoCourseEnrole();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciCO_AUT_Course_IsAutoCourseEnrole ON dbo.CO_AUT_Course_IsAutoCourseEnrole;
-- DROP FUNCTION IF EXISTS dbo.tciCO_AUT_Course_IsAutoCourseEnrole();
CREATE OR REPLACE FUNCTION dbo.tciCO_AUT_Course_IsAutoCourseEnrole() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_CO_AUT_Course_IsAutoCourseEnrole
        SELECT
            NEW.CO_AUT_CO_ID,
            NEW.Metadata_CO_AUT,
            NEW.CO_AUT_AUT_ID,
            1,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciCO_AUT_Course_IsAutoCourseEnrole
INSTEAD OF INSERT ON dbo.CO_AUT_Course_IsAutoCourseEnrole
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciCO_AUT_Course_IsAutoCourseEnrole();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaCO_AUT_Course_IsAutoCourseEnrole ON dbo.CO_AUT_Course_IsAutoCourseEnrole;
-- DROP FUNCTION IF EXISTS dbo.tcaCO_AUT_Course_IsAutoCourseEnrole();
CREATE OR REPLACE FUNCTION dbo.tcaCO_AUT_Course_IsAutoCourseEnrole() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find max version
    SELECT
        MAX(CO_AUT_Version) INTO maxVersion
    FROM
        _tmp_CO_AUT_Course_IsAutoCourseEnrole;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_CO_AUT_Course_IsAutoCourseEnrole
        SET
            CO_AUT_StatementType =
                CASE
                    WHEN AUT.CO_AUT_CO_ID is not null
                    THEN ''D'' -- duplicate
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_CO_AUT_Course_IsAutoCourseEnrole v
        LEFT JOIN
            dbo._CO_AUT_Course_IsAutoCourseEnrole AUT
        ON
            AUT.CO_AUT_CO_ID = v.CO_AUT_CO_ID
        AND
            AUT.CO_AUT_AUT_ID = v.CO_AUT_AUT_ID
        WHERE
            v.CO_AUT_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._CO_AUT_Course_IsAutoCourseEnrole (
            CO_AUT_CO_ID,
            Metadata_CO_AUT,
            CO_AUT_AUT_ID
        )
        SELECT
            CO_AUT_CO_ID,
            Metadata_CO_AUT,
            CO_AUT_AUT_ID
        FROM
            _tmp_CO_AUT_Course_IsAutoCourseEnrole
        WHERE
            CO_AUT_Version = currentVersion
        AND
            CO_AUT_StatementType in (''N'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_CO_AUT_Course_IsAutoCourseEnrole;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaCO_AUT_Course_IsAutoCourseEnrole
AFTER INSERT ON dbo.CO_AUT_Course_IsAutoCourseEnrole
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaCO_AUT_Course_IsAutoCourseEnrole();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbCO_DEM_Course_IsDemoEnrole ON dbo.CO_DEM_Course_IsDemoEnrole;
-- DROP FUNCTION IF EXISTS dbo.tcbCO_DEM_Course_IsDemoEnrole();
CREATE OR REPLACE FUNCTION dbo.tcbCO_DEM_Course_IsDemoEnrole() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_CO_DEM_Course_IsDemoEnrole (
            CO_DEM_CO_ID int not null,
            Metadata_CO_DEM int not null,
            CO_DEM_DEM_ID int not null, 
            CO_DEM_Version bigint not null,
            CO_DEM_StatementType char(1) not null,
            primary key(
                CO_DEM_Version,
                CO_DEM_CO_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbCO_DEM_Course_IsDemoEnrole
BEFORE INSERT ON dbo.CO_DEM_Course_IsDemoEnrole
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbCO_DEM_Course_IsDemoEnrole();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciCO_DEM_Course_IsDemoEnrole ON dbo.CO_DEM_Course_IsDemoEnrole;
-- DROP FUNCTION IF EXISTS dbo.tciCO_DEM_Course_IsDemoEnrole();
CREATE OR REPLACE FUNCTION dbo.tciCO_DEM_Course_IsDemoEnrole() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_CO_DEM_Course_IsDemoEnrole
        SELECT
            NEW.CO_DEM_CO_ID,
            NEW.Metadata_CO_DEM,
            NEW.CO_DEM_DEM_ID,
            1,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciCO_DEM_Course_IsDemoEnrole
INSTEAD OF INSERT ON dbo.CO_DEM_Course_IsDemoEnrole
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciCO_DEM_Course_IsDemoEnrole();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaCO_DEM_Course_IsDemoEnrole ON dbo.CO_DEM_Course_IsDemoEnrole;
-- DROP FUNCTION IF EXISTS dbo.tcaCO_DEM_Course_IsDemoEnrole();
CREATE OR REPLACE FUNCTION dbo.tcaCO_DEM_Course_IsDemoEnrole() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find max version
    SELECT
        MAX(CO_DEM_Version) INTO maxVersion
    FROM
        _tmp_CO_DEM_Course_IsDemoEnrole;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_CO_DEM_Course_IsDemoEnrole
        SET
            CO_DEM_StatementType =
                CASE
                    WHEN DEM.CO_DEM_CO_ID is not null
                    THEN ''D'' -- duplicate
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_CO_DEM_Course_IsDemoEnrole v
        LEFT JOIN
            dbo._CO_DEM_Course_IsDemoEnrole DEM
        ON
            DEM.CO_DEM_CO_ID = v.CO_DEM_CO_ID
        AND
            DEM.CO_DEM_DEM_ID = v.CO_DEM_DEM_ID
        WHERE
            v.CO_DEM_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._CO_DEM_Course_IsDemoEnrole (
            CO_DEM_CO_ID,
            Metadata_CO_DEM,
            CO_DEM_DEM_ID
        )
        SELECT
            CO_DEM_CO_ID,
            Metadata_CO_DEM,
            CO_DEM_DEM_ID
        FROM
            _tmp_CO_DEM_Course_IsDemoEnrole
        WHERE
            CO_DEM_Version = currentVersion
        AND
            CO_DEM_StatementType in (''N'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_CO_DEM_Course_IsDemoEnrole;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaCO_DEM_Course_IsDemoEnrole
AFTER INSERT ON dbo.CO_DEM_Course_IsDemoEnrole
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaCO_DEM_Course_IsDemoEnrole();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbCO_UPD_Course_UpdatedAt ON dbo.CO_UPD_Course_UpdatedAt;
-- DROP FUNCTION IF EXISTS dbo.tcbCO_UPD_Course_UpdatedAt();
CREATE OR REPLACE FUNCTION dbo.tcbCO_UPD_Course_UpdatedAt() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_CO_UPD_Course_UpdatedAt (
            CO_UPD_CO_ID int not null,
            Metadata_CO_UPD int not null,
            CO_UPD_ChangedAt timestamp not null,
            CO_UPD_Course_UpdatedAt timestamp not null,
            CO_UPD_Version bigint not null,
            CO_UPD_StatementType char(1) not null,
            primary key(
                CO_UPD_Version,
                CO_UPD_CO_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbCO_UPD_Course_UpdatedAt
BEFORE INSERT ON dbo.CO_UPD_Course_UpdatedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbCO_UPD_Course_UpdatedAt();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciCO_UPD_Course_UpdatedAt ON dbo.CO_UPD_Course_UpdatedAt;
-- DROP FUNCTION IF EXISTS dbo.tciCO_UPD_Course_UpdatedAt();
CREATE OR REPLACE FUNCTION dbo.tciCO_UPD_Course_UpdatedAt() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_CO_UPD_Course_UpdatedAt
        SELECT
            NEW.CO_UPD_CO_ID,
            NEW.Metadata_CO_UPD,
            NEW.CO_UPD_ChangedAt,
            NEW.CO_UPD_Course_UpdatedAt,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciCO_UPD_Course_UpdatedAt
INSTEAD OF INSERT ON dbo.CO_UPD_Course_UpdatedAt
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciCO_UPD_Course_UpdatedAt();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaCO_UPD_Course_UpdatedAt ON dbo.CO_UPD_Course_UpdatedAt;
-- DROP FUNCTION IF EXISTS dbo.tcaCO_UPD_Course_UpdatedAt();
CREATE OR REPLACE FUNCTION dbo.tcaCO_UPD_Course_UpdatedAt() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_CO_UPD_Course_UpdatedAt
    SET CO_UPD_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    CO_UPD_CO_ID
                ORDER BY
                    CO_UPD_ChangedAt ASC
            ) AS rank,
            CO_UPD_CO_ID AS pk
        FROM _tmp_CO_UPD_Course_UpdatedAt
    ) AS v
    WHERE CO_UPD_CO_ID = v.pk
    AND CO_UPD_Version = 0;
    -- find max version
    SELECT
        MAX(CO_UPD_Version) INTO maxVersion
    FROM
        _tmp_CO_UPD_Course_UpdatedAt;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_CO_UPD_Course_UpdatedAt
        SET
            CO_UPD_StatementType =
                CASE
                    WHEN UPD.CO_UPD_CO_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfCO_UPD_Course_UpdatedAt(
                        v.CO_UPD_CO_ID,
                        v.CO_UPD_Course_UpdatedAt,
                        v.CO_UPD_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_CO_UPD_Course_UpdatedAt v
        LEFT JOIN
            dbo._CO_UPD_Course_UpdatedAt UPD
        ON
            UPD.CO_UPD_CO_ID = v.CO_UPD_CO_ID
        AND
            UPD.CO_UPD_ChangedAt = v.CO_UPD_ChangedAt
        AND
            UPD.CO_UPD_Course_UpdatedAt = v.CO_UPD_Course_UpdatedAt
        WHERE
            v.CO_UPD_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._CO_UPD_Course_UpdatedAt (
            CO_UPD_CO_ID,
            Metadata_CO_UPD,
            CO_UPD_ChangedAt,
            CO_UPD_Course_UpdatedAt
        )
        SELECT
            CO_UPD_CO_ID,
            Metadata_CO_UPD,
            CO_UPD_ChangedAt,
            CO_UPD_Course_UpdatedAt
        FROM
            _tmp_CO_UPD_Course_UpdatedAt
        WHERE
            CO_UPD_Version = currentVersion
        AND
            CO_UPD_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_CO_UPD_Course_UpdatedAt;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaCO_UPD_Course_UpdatedAt
AFTER INSERT ON dbo.CO_UPD_Course_UpdatedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaCO_UPD_Course_UpdatedAt();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbST_DEL_Stream_DeletedAt ON dbo.ST_DEL_Stream_DeletedAt;
-- DROP FUNCTION IF EXISTS dbo.tcbST_DEL_Stream_DeletedAt();
CREATE OR REPLACE FUNCTION dbo.tcbST_DEL_Stream_DeletedAt() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_ST_DEL_Stream_DeletedAt (
            ST_DEL_ST_ID int not null,
            Metadata_ST_DEL int not null,
            ST_DEL_Stream_DeletedAt timestamp not null,
            ST_DEL_Version bigint not null,
            ST_DEL_StatementType char(1) not null,
            primary key(
                ST_DEL_Version,
                ST_DEL_ST_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbST_DEL_Stream_DeletedAt
BEFORE INSERT ON dbo.ST_DEL_Stream_DeletedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbST_DEL_Stream_DeletedAt();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciST_DEL_Stream_DeletedAt ON dbo.ST_DEL_Stream_DeletedAt;
-- DROP FUNCTION IF EXISTS dbo.tciST_DEL_Stream_DeletedAt();
CREATE OR REPLACE FUNCTION dbo.tciST_DEL_Stream_DeletedAt() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_ST_DEL_Stream_DeletedAt
        SELECT
            NEW.ST_DEL_ST_ID,
            NEW.Metadata_ST_DEL,
            NEW.ST_DEL_Stream_DeletedAt,
            1,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciST_DEL_Stream_DeletedAt
INSTEAD OF INSERT ON dbo.ST_DEL_Stream_DeletedAt
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciST_DEL_Stream_DeletedAt();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaST_DEL_Stream_DeletedAt ON dbo.ST_DEL_Stream_DeletedAt;
-- DROP FUNCTION IF EXISTS dbo.tcaST_DEL_Stream_DeletedAt();
CREATE OR REPLACE FUNCTION dbo.tcaST_DEL_Stream_DeletedAt() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find max version
    SELECT
        MAX(ST_DEL_Version) INTO maxVersion
    FROM
        _tmp_ST_DEL_Stream_DeletedAt;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_ST_DEL_Stream_DeletedAt
        SET
            ST_DEL_StatementType =
                CASE
                    WHEN DEL.ST_DEL_ST_ID is not null
                    THEN ''D'' -- duplicate
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_ST_DEL_Stream_DeletedAt v
        LEFT JOIN
            dbo._ST_DEL_Stream_DeletedAt DEL
        ON
            DEL.ST_DEL_ST_ID = v.ST_DEL_ST_ID
        AND
            DEL.ST_DEL_Stream_DeletedAt = v.ST_DEL_Stream_DeletedAt
        WHERE
            v.ST_DEL_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._ST_DEL_Stream_DeletedAt (
            ST_DEL_ST_ID,
            Metadata_ST_DEL,
            ST_DEL_Stream_DeletedAt
        )
        SELECT
            ST_DEL_ST_ID,
            Metadata_ST_DEL,
            ST_DEL_Stream_DeletedAt
        FROM
            _tmp_ST_DEL_Stream_DeletedAt
        WHERE
            ST_DEL_Version = currentVersion
        AND
            ST_DEL_StatementType in (''N'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_ST_DEL_Stream_DeletedAt;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaST_DEL_Stream_DeletedAt
AFTER INSERT ON dbo.ST_DEL_Stream_DeletedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaST_DEL_Stream_DeletedAt();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbST_CRE_Stream_CreatedAt ON dbo.ST_CRE_Stream_CreatedAt;
-- DROP FUNCTION IF EXISTS dbo.tcbST_CRE_Stream_CreatedAt();
CREATE OR REPLACE FUNCTION dbo.tcbST_CRE_Stream_CreatedAt() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_ST_CRE_Stream_CreatedAt (
            ST_CRE_ST_ID int not null,
            Metadata_ST_CRE int not null,
            ST_CRE_Stream_CreatedAt timestamp not null,
            ST_CRE_Version bigint not null,
            ST_CRE_StatementType char(1) not null,
            primary key(
                ST_CRE_Version,
                ST_CRE_ST_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbST_CRE_Stream_CreatedAt
BEFORE INSERT ON dbo.ST_CRE_Stream_CreatedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbST_CRE_Stream_CreatedAt();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciST_CRE_Stream_CreatedAt ON dbo.ST_CRE_Stream_CreatedAt;
-- DROP FUNCTION IF EXISTS dbo.tciST_CRE_Stream_CreatedAt();
CREATE OR REPLACE FUNCTION dbo.tciST_CRE_Stream_CreatedAt() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_ST_CRE_Stream_CreatedAt
        SELECT
            NEW.ST_CRE_ST_ID,
            NEW.Metadata_ST_CRE,
            NEW.ST_CRE_Stream_CreatedAt,
            1,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciST_CRE_Stream_CreatedAt
INSTEAD OF INSERT ON dbo.ST_CRE_Stream_CreatedAt
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciST_CRE_Stream_CreatedAt();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaST_CRE_Stream_CreatedAt ON dbo.ST_CRE_Stream_CreatedAt;
-- DROP FUNCTION IF EXISTS dbo.tcaST_CRE_Stream_CreatedAt();
CREATE OR REPLACE FUNCTION dbo.tcaST_CRE_Stream_CreatedAt() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find max version
    SELECT
        MAX(ST_CRE_Version) INTO maxVersion
    FROM
        _tmp_ST_CRE_Stream_CreatedAt;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_ST_CRE_Stream_CreatedAt
        SET
            ST_CRE_StatementType =
                CASE
                    WHEN CRE.ST_CRE_ST_ID is not null
                    THEN ''D'' -- duplicate
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_ST_CRE_Stream_CreatedAt v
        LEFT JOIN
            dbo._ST_CRE_Stream_CreatedAt CRE
        ON
            CRE.ST_CRE_ST_ID = v.ST_CRE_ST_ID
        AND
            CRE.ST_CRE_Stream_CreatedAt = v.ST_CRE_Stream_CreatedAt
        WHERE
            v.ST_CRE_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._ST_CRE_Stream_CreatedAt (
            ST_CRE_ST_ID,
            Metadata_ST_CRE,
            ST_CRE_Stream_CreatedAt
        )
        SELECT
            ST_CRE_ST_ID,
            Metadata_ST_CRE,
            ST_CRE_Stream_CreatedAt
        FROM
            _tmp_ST_CRE_Stream_CreatedAt
        WHERE
            ST_CRE_Version = currentVersion
        AND
            ST_CRE_StatementType in (''N'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_ST_CRE_Stream_CreatedAt;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaST_CRE_Stream_CreatedAt
AFTER INSERT ON dbo.ST_CRE_Stream_CreatedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaST_CRE_Stream_CreatedAt();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbST_ENA_Stream_EndAt ON dbo.ST_ENA_Stream_EndAt;
-- DROP FUNCTION IF EXISTS dbo.tcbST_ENA_Stream_EndAt();
CREATE OR REPLACE FUNCTION dbo.tcbST_ENA_Stream_EndAt() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_ST_ENA_Stream_EndAt (
            ST_ENA_ST_ID int not null,
            Metadata_ST_ENA int not null,
            ST_ENA_ChangedAt timestamp not null,
            ST_ENA_Stream_EndAt timestamp not null,
            ST_ENA_Version bigint not null,
            ST_ENA_StatementType char(1) not null,
            primary key(
                ST_ENA_Version,
                ST_ENA_ST_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbST_ENA_Stream_EndAt
BEFORE INSERT ON dbo.ST_ENA_Stream_EndAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbST_ENA_Stream_EndAt();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciST_ENA_Stream_EndAt ON dbo.ST_ENA_Stream_EndAt;
-- DROP FUNCTION IF EXISTS dbo.tciST_ENA_Stream_EndAt();
CREATE OR REPLACE FUNCTION dbo.tciST_ENA_Stream_EndAt() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_ST_ENA_Stream_EndAt
        SELECT
            NEW.ST_ENA_ST_ID,
            NEW.Metadata_ST_ENA,
            NEW.ST_ENA_ChangedAt,
            NEW.ST_ENA_Stream_EndAt,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciST_ENA_Stream_EndAt
INSTEAD OF INSERT ON dbo.ST_ENA_Stream_EndAt
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciST_ENA_Stream_EndAt();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaST_ENA_Stream_EndAt ON dbo.ST_ENA_Stream_EndAt;
-- DROP FUNCTION IF EXISTS dbo.tcaST_ENA_Stream_EndAt();
CREATE OR REPLACE FUNCTION dbo.tcaST_ENA_Stream_EndAt() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_ST_ENA_Stream_EndAt
    SET ST_ENA_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    ST_ENA_ST_ID
                ORDER BY
                    ST_ENA_ChangedAt ASC
            ) AS rank,
            ST_ENA_ST_ID AS pk
        FROM _tmp_ST_ENA_Stream_EndAt
    ) AS v
    WHERE ST_ENA_ST_ID = v.pk
    AND ST_ENA_Version = 0;
    -- find max version
    SELECT
        MAX(ST_ENA_Version) INTO maxVersion
    FROM
        _tmp_ST_ENA_Stream_EndAt;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_ST_ENA_Stream_EndAt
        SET
            ST_ENA_StatementType =
                CASE
                    WHEN ENA.ST_ENA_ST_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfST_ENA_Stream_EndAt(
                        v.ST_ENA_ST_ID,
                        v.ST_ENA_Stream_EndAt,
                        v.ST_ENA_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_ST_ENA_Stream_EndAt v
        LEFT JOIN
            dbo._ST_ENA_Stream_EndAt ENA
        ON
            ENA.ST_ENA_ST_ID = v.ST_ENA_ST_ID
        AND
            ENA.ST_ENA_ChangedAt = v.ST_ENA_ChangedAt
        AND
            ENA.ST_ENA_Stream_EndAt = v.ST_ENA_Stream_EndAt
        WHERE
            v.ST_ENA_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._ST_ENA_Stream_EndAt (
            ST_ENA_ST_ID,
            Metadata_ST_ENA,
            ST_ENA_ChangedAt,
            ST_ENA_Stream_EndAt
        )
        SELECT
            ST_ENA_ST_ID,
            Metadata_ST_ENA,
            ST_ENA_ChangedAt,
            ST_ENA_Stream_EndAt
        FROM
            _tmp_ST_ENA_Stream_EndAt
        WHERE
            ST_ENA_Version = currentVersion
        AND
            ST_ENA_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_ST_ENA_Stream_EndAt;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaST_ENA_Stream_EndAt
AFTER INSERT ON dbo.ST_ENA_Stream_EndAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaST_ENA_Stream_EndAt();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbST_STA_Stream_StartAt ON dbo.ST_STA_Stream_StartAt;
-- DROP FUNCTION IF EXISTS dbo.tcbST_STA_Stream_StartAt();
CREATE OR REPLACE FUNCTION dbo.tcbST_STA_Stream_StartAt() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_ST_STA_Stream_StartAt (
            ST_STA_ST_ID int not null,
            Metadata_ST_STA int not null,
            ST_STA_ChangedAt timestamp not null,
            ST_STA_Stream_StartAt timestamp not null,
            ST_STA_Version bigint not null,
            ST_STA_StatementType char(1) not null,
            primary key(
                ST_STA_Version,
                ST_STA_ST_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbST_STA_Stream_StartAt
BEFORE INSERT ON dbo.ST_STA_Stream_StartAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbST_STA_Stream_StartAt();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciST_STA_Stream_StartAt ON dbo.ST_STA_Stream_StartAt;
-- DROP FUNCTION IF EXISTS dbo.tciST_STA_Stream_StartAt();
CREATE OR REPLACE FUNCTION dbo.tciST_STA_Stream_StartAt() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_ST_STA_Stream_StartAt
        SELECT
            NEW.ST_STA_ST_ID,
            NEW.Metadata_ST_STA,
            NEW.ST_STA_ChangedAt,
            NEW.ST_STA_Stream_StartAt,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciST_STA_Stream_StartAt
INSTEAD OF INSERT ON dbo.ST_STA_Stream_StartAt
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciST_STA_Stream_StartAt();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaST_STA_Stream_StartAt ON dbo.ST_STA_Stream_StartAt;
-- DROP FUNCTION IF EXISTS dbo.tcaST_STA_Stream_StartAt();
CREATE OR REPLACE FUNCTION dbo.tcaST_STA_Stream_StartAt() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_ST_STA_Stream_StartAt
    SET ST_STA_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    ST_STA_ST_ID
                ORDER BY
                    ST_STA_ChangedAt ASC
            ) AS rank,
            ST_STA_ST_ID AS pk
        FROM _tmp_ST_STA_Stream_StartAt
    ) AS v
    WHERE ST_STA_ST_ID = v.pk
    AND ST_STA_Version = 0;
    -- find max version
    SELECT
        MAX(ST_STA_Version) INTO maxVersion
    FROM
        _tmp_ST_STA_Stream_StartAt;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_ST_STA_Stream_StartAt
        SET
            ST_STA_StatementType =
                CASE
                    WHEN STA.ST_STA_ST_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfST_STA_Stream_StartAt(
                        v.ST_STA_ST_ID,
                        v.ST_STA_Stream_StartAt,
                        v.ST_STA_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_ST_STA_Stream_StartAt v
        LEFT JOIN
            dbo._ST_STA_Stream_StartAt STA
        ON
            STA.ST_STA_ST_ID = v.ST_STA_ST_ID
        AND
            STA.ST_STA_ChangedAt = v.ST_STA_ChangedAt
        AND
            STA.ST_STA_Stream_StartAt = v.ST_STA_Stream_StartAt
        WHERE
            v.ST_STA_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._ST_STA_Stream_StartAt (
            ST_STA_ST_ID,
            Metadata_ST_STA,
            ST_STA_ChangedAt,
            ST_STA_Stream_StartAt
        )
        SELECT
            ST_STA_ST_ID,
            Metadata_ST_STA,
            ST_STA_ChangedAt,
            ST_STA_Stream_StartAt
        FROM
            _tmp_ST_STA_Stream_StartAt
        WHERE
            ST_STA_Version = currentVersion
        AND
            ST_STA_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_ST_STA_Stream_StartAt;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaST_STA_Stream_StartAt
AFTER INSERT ON dbo.ST_STA_Stream_StartAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaST_STA_Stream_StartAt();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbST_HOM_Stream_HomeworkDeadlineDays ON dbo.ST_HOM_Stream_HomeworkDeadlineDays;
-- DROP FUNCTION IF EXISTS dbo.tcbST_HOM_Stream_HomeworkDeadlineDays();
CREATE OR REPLACE FUNCTION dbo.tcbST_HOM_Stream_HomeworkDeadlineDays() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_ST_HOM_Stream_HomeworkDeadlineDays (
            ST_HOM_ST_ID int not null,
            Metadata_ST_HOM int not null,
            ST_HOM_ChangedAt timestamp not null,
            ST_HOM_Stream_HomeworkDeadlineDays int not null,
            ST_HOM_Version bigint not null,
            ST_HOM_StatementType char(1) not null,
            primary key(
                ST_HOM_Version,
                ST_HOM_ST_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbST_HOM_Stream_HomeworkDeadlineDays
BEFORE INSERT ON dbo.ST_HOM_Stream_HomeworkDeadlineDays
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbST_HOM_Stream_HomeworkDeadlineDays();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciST_HOM_Stream_HomeworkDeadlineDays ON dbo.ST_HOM_Stream_HomeworkDeadlineDays;
-- DROP FUNCTION IF EXISTS dbo.tciST_HOM_Stream_HomeworkDeadlineDays();
CREATE OR REPLACE FUNCTION dbo.tciST_HOM_Stream_HomeworkDeadlineDays() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_ST_HOM_Stream_HomeworkDeadlineDays
        SELECT
            NEW.ST_HOM_ST_ID,
            NEW.Metadata_ST_HOM,
            NEW.ST_HOM_ChangedAt,
            NEW.ST_HOM_Stream_HomeworkDeadlineDays,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciST_HOM_Stream_HomeworkDeadlineDays
INSTEAD OF INSERT ON dbo.ST_HOM_Stream_HomeworkDeadlineDays
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciST_HOM_Stream_HomeworkDeadlineDays();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaST_HOM_Stream_HomeworkDeadlineDays ON dbo.ST_HOM_Stream_HomeworkDeadlineDays;
-- DROP FUNCTION IF EXISTS dbo.tcaST_HOM_Stream_HomeworkDeadlineDays();
CREATE OR REPLACE FUNCTION dbo.tcaST_HOM_Stream_HomeworkDeadlineDays() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_ST_HOM_Stream_HomeworkDeadlineDays
    SET ST_HOM_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    ST_HOM_ST_ID
                ORDER BY
                    ST_HOM_ChangedAt ASC
            ) AS rank,
            ST_HOM_ST_ID AS pk
        FROM _tmp_ST_HOM_Stream_HomeworkDeadlineDays
    ) AS v
    WHERE ST_HOM_ST_ID = v.pk
    AND ST_HOM_Version = 0;
    -- find max version
    SELECT
        MAX(ST_HOM_Version) INTO maxVersion
    FROM
        _tmp_ST_HOM_Stream_HomeworkDeadlineDays;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_ST_HOM_Stream_HomeworkDeadlineDays
        SET
            ST_HOM_StatementType =
                CASE
                    WHEN HOM.ST_HOM_ST_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfST_HOM_Stream_HomeworkDeadlineDays(
                        v.ST_HOM_ST_ID,
                        v.ST_HOM_Stream_HomeworkDeadlineDays,
                        v.ST_HOM_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_ST_HOM_Stream_HomeworkDeadlineDays v
        LEFT JOIN
            dbo._ST_HOM_Stream_HomeworkDeadlineDays HOM
        ON
            HOM.ST_HOM_ST_ID = v.ST_HOM_ST_ID
        AND
            HOM.ST_HOM_ChangedAt = v.ST_HOM_ChangedAt
        AND
            HOM.ST_HOM_Stream_HomeworkDeadlineDays = v.ST_HOM_Stream_HomeworkDeadlineDays
        WHERE
            v.ST_HOM_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._ST_HOM_Stream_HomeworkDeadlineDays (
            ST_HOM_ST_ID,
            Metadata_ST_HOM,
            ST_HOM_ChangedAt,
            ST_HOM_Stream_HomeworkDeadlineDays
        )
        SELECT
            ST_HOM_ST_ID,
            Metadata_ST_HOM,
            ST_HOM_ChangedAt,
            ST_HOM_Stream_HomeworkDeadlineDays
        FROM
            _tmp_ST_HOM_Stream_HomeworkDeadlineDays
        WHERE
            ST_HOM_Version = currentVersion
        AND
            ST_HOM_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_ST_HOM_Stream_HomeworkDeadlineDays;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaST_HOM_Stream_HomeworkDeadlineDays
AFTER INSERT ON dbo.ST_HOM_Stream_HomeworkDeadlineDays
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaST_HOM_Stream_HomeworkDeadlineDays();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbST_OPE_Stream_IsOpen ON dbo.ST_OPE_Stream_IsOpen;
-- DROP FUNCTION IF EXISTS dbo.tcbST_OPE_Stream_IsOpen();
CREATE OR REPLACE FUNCTION dbo.tcbST_OPE_Stream_IsOpen() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_ST_OPE_Stream_IsOpen (
            ST_OPE_ST_ID int not null,
            Metadata_ST_OPE int not null,
            ST_OPE_OPE_ID int not null, 
            ST_OPE_Version bigint not null,
            ST_OPE_StatementType char(1) not null,
            primary key(
                ST_OPE_Version,
                ST_OPE_ST_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbST_OPE_Stream_IsOpen
BEFORE INSERT ON dbo.ST_OPE_Stream_IsOpen
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbST_OPE_Stream_IsOpen();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciST_OPE_Stream_IsOpen ON dbo.ST_OPE_Stream_IsOpen;
-- DROP FUNCTION IF EXISTS dbo.tciST_OPE_Stream_IsOpen();
CREATE OR REPLACE FUNCTION dbo.tciST_OPE_Stream_IsOpen() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_ST_OPE_Stream_IsOpen
        SELECT
            NEW.ST_OPE_ST_ID,
            NEW.Metadata_ST_OPE,
            NEW.ST_OPE_OPE_ID,
            1,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciST_OPE_Stream_IsOpen
INSTEAD OF INSERT ON dbo.ST_OPE_Stream_IsOpen
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciST_OPE_Stream_IsOpen();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaST_OPE_Stream_IsOpen ON dbo.ST_OPE_Stream_IsOpen;
-- DROP FUNCTION IF EXISTS dbo.tcaST_OPE_Stream_IsOpen();
CREATE OR REPLACE FUNCTION dbo.tcaST_OPE_Stream_IsOpen() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find max version
    SELECT
        MAX(ST_OPE_Version) INTO maxVersion
    FROM
        _tmp_ST_OPE_Stream_IsOpen;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_ST_OPE_Stream_IsOpen
        SET
            ST_OPE_StatementType =
                CASE
                    WHEN OPE.ST_OPE_ST_ID is not null
                    THEN ''D'' -- duplicate
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_ST_OPE_Stream_IsOpen v
        LEFT JOIN
            dbo._ST_OPE_Stream_IsOpen OPE
        ON
            OPE.ST_OPE_ST_ID = v.ST_OPE_ST_ID
        AND
            OPE.ST_OPE_OPE_ID = v.ST_OPE_OPE_ID
        WHERE
            v.ST_OPE_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._ST_OPE_Stream_IsOpen (
            ST_OPE_ST_ID,
            Metadata_ST_OPE,
            ST_OPE_OPE_ID
        )
        SELECT
            ST_OPE_ST_ID,
            Metadata_ST_OPE,
            ST_OPE_OPE_ID
        FROM
            _tmp_ST_OPE_Stream_IsOpen
        WHERE
            ST_OPE_Version = currentVersion
        AND
            ST_OPE_StatementType in (''N'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_ST_OPE_Stream_IsOpen;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaST_OPE_Stream_IsOpen
AFTER INSERT ON dbo.ST_OPE_Stream_IsOpen
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaST_OPE_Stream_IsOpen();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbST_NAM_Stream_Name ON dbo.ST_NAM_Stream_Name;
-- DROP FUNCTION IF EXISTS dbo.tcbST_NAM_Stream_Name();
CREATE OR REPLACE FUNCTION dbo.tcbST_NAM_Stream_Name() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_ST_NAM_Stream_Name (
            ST_NAM_ST_ID int not null,
            Metadata_ST_NAM int not null,
            ST_NAM_ChangedAt timestamp not null,
            ST_NAM_Stream_Name varchar(255) not null,
            ST_NAM_Version bigint not null,
            ST_NAM_StatementType char(1) not null,
            primary key(
                ST_NAM_Version,
                ST_NAM_ST_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbST_NAM_Stream_Name
BEFORE INSERT ON dbo.ST_NAM_Stream_Name
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbST_NAM_Stream_Name();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciST_NAM_Stream_Name ON dbo.ST_NAM_Stream_Name;
-- DROP FUNCTION IF EXISTS dbo.tciST_NAM_Stream_Name();
CREATE OR REPLACE FUNCTION dbo.tciST_NAM_Stream_Name() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_ST_NAM_Stream_Name
        SELECT
            NEW.ST_NAM_ST_ID,
            NEW.Metadata_ST_NAM,
            NEW.ST_NAM_ChangedAt,
            NEW.ST_NAM_Stream_Name,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciST_NAM_Stream_Name
INSTEAD OF INSERT ON dbo.ST_NAM_Stream_Name
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciST_NAM_Stream_Name();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaST_NAM_Stream_Name ON dbo.ST_NAM_Stream_Name;
-- DROP FUNCTION IF EXISTS dbo.tcaST_NAM_Stream_Name();
CREATE OR REPLACE FUNCTION dbo.tcaST_NAM_Stream_Name() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_ST_NAM_Stream_Name
    SET ST_NAM_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    ST_NAM_ST_ID
                ORDER BY
                    ST_NAM_ChangedAt ASC
            ) AS rank,
            ST_NAM_ST_ID AS pk
        FROM _tmp_ST_NAM_Stream_Name
    ) AS v
    WHERE ST_NAM_ST_ID = v.pk
    AND ST_NAM_Version = 0;
    -- find max version
    SELECT
        MAX(ST_NAM_Version) INTO maxVersion
    FROM
        _tmp_ST_NAM_Stream_Name;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_ST_NAM_Stream_Name
        SET
            ST_NAM_StatementType =
                CASE
                    WHEN NAM.ST_NAM_ST_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfST_NAM_Stream_Name(
                        v.ST_NAM_ST_ID,
                        v.ST_NAM_Stream_Name,
                        v.ST_NAM_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_ST_NAM_Stream_Name v
        LEFT JOIN
            dbo._ST_NAM_Stream_Name NAM
        ON
            NAM.ST_NAM_ST_ID = v.ST_NAM_ST_ID
        AND
            NAM.ST_NAM_ChangedAt = v.ST_NAM_ChangedAt
        AND
            NAM.ST_NAM_Stream_Name = v.ST_NAM_Stream_Name
        WHERE
            v.ST_NAM_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._ST_NAM_Stream_Name (
            ST_NAM_ST_ID,
            Metadata_ST_NAM,
            ST_NAM_ChangedAt,
            ST_NAM_Stream_Name
        )
        SELECT
            ST_NAM_ST_ID,
            Metadata_ST_NAM,
            ST_NAM_ChangedAt,
            ST_NAM_Stream_Name
        FROM
            _tmp_ST_NAM_Stream_Name
        WHERE
            ST_NAM_Version = currentVersion
        AND
            ST_NAM_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_ST_NAM_Stream_Name;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaST_NAM_Stream_Name
AFTER INSERT ON dbo.ST_NAM_Stream_Name
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaST_NAM_Stream_Name();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbST_UPD_Stream_UpdatedAt ON dbo.ST_UPD_Stream_UpdatedAt;
-- DROP FUNCTION IF EXISTS dbo.tcbST_UPD_Stream_UpdatedAt();
CREATE OR REPLACE FUNCTION dbo.tcbST_UPD_Stream_UpdatedAt() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_ST_UPD_Stream_UpdatedAt (
            ST_UPD_ST_ID int not null,
            Metadata_ST_UPD int not null,
            ST_UPD_ChangedAt timestamp not null,
            ST_UPD_Stream_UpdatedAt timestamp not null,
            ST_UPD_Version bigint not null,
            ST_UPD_StatementType char(1) not null,
            primary key(
                ST_UPD_Version,
                ST_UPD_ST_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbST_UPD_Stream_UpdatedAt
BEFORE INSERT ON dbo.ST_UPD_Stream_UpdatedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbST_UPD_Stream_UpdatedAt();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciST_UPD_Stream_UpdatedAt ON dbo.ST_UPD_Stream_UpdatedAt;
-- DROP FUNCTION IF EXISTS dbo.tciST_UPD_Stream_UpdatedAt();
CREATE OR REPLACE FUNCTION dbo.tciST_UPD_Stream_UpdatedAt() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_ST_UPD_Stream_UpdatedAt
        SELECT
            NEW.ST_UPD_ST_ID,
            NEW.Metadata_ST_UPD,
            NEW.ST_UPD_ChangedAt,
            NEW.ST_UPD_Stream_UpdatedAt,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciST_UPD_Stream_UpdatedAt
INSTEAD OF INSERT ON dbo.ST_UPD_Stream_UpdatedAt
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciST_UPD_Stream_UpdatedAt();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaST_UPD_Stream_UpdatedAt ON dbo.ST_UPD_Stream_UpdatedAt;
-- DROP FUNCTION IF EXISTS dbo.tcaST_UPD_Stream_UpdatedAt();
CREATE OR REPLACE FUNCTION dbo.tcaST_UPD_Stream_UpdatedAt() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_ST_UPD_Stream_UpdatedAt
    SET ST_UPD_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    ST_UPD_ST_ID
                ORDER BY
                    ST_UPD_ChangedAt ASC
            ) AS rank,
            ST_UPD_ST_ID AS pk
        FROM _tmp_ST_UPD_Stream_UpdatedAt
    ) AS v
    WHERE ST_UPD_ST_ID = v.pk
    AND ST_UPD_Version = 0;
    -- find max version
    SELECT
        MAX(ST_UPD_Version) INTO maxVersion
    FROM
        _tmp_ST_UPD_Stream_UpdatedAt;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_ST_UPD_Stream_UpdatedAt
        SET
            ST_UPD_StatementType =
                CASE
                    WHEN UPD.ST_UPD_ST_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfST_UPD_Stream_UpdatedAt(
                        v.ST_UPD_ST_ID,
                        v.ST_UPD_Stream_UpdatedAt,
                        v.ST_UPD_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_ST_UPD_Stream_UpdatedAt v
        LEFT JOIN
            dbo._ST_UPD_Stream_UpdatedAt UPD
        ON
            UPD.ST_UPD_ST_ID = v.ST_UPD_ST_ID
        AND
            UPD.ST_UPD_ChangedAt = v.ST_UPD_ChangedAt
        AND
            UPD.ST_UPD_Stream_UpdatedAt = v.ST_UPD_Stream_UpdatedAt
        WHERE
            v.ST_UPD_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._ST_UPD_Stream_UpdatedAt (
            ST_UPD_ST_ID,
            Metadata_ST_UPD,
            ST_UPD_ChangedAt,
            ST_UPD_Stream_UpdatedAt
        )
        SELECT
            ST_UPD_ST_ID,
            Metadata_ST_UPD,
            ST_UPD_ChangedAt,
            ST_UPD_Stream_UpdatedAt
        FROM
            _tmp_ST_UPD_Stream_UpdatedAt
        WHERE
            ST_UPD_Version = currentVersion
        AND
            ST_UPD_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_ST_UPD_Stream_UpdatedAt;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaST_UPD_Stream_UpdatedAt
AFTER INSERT ON dbo.ST_UPD_Stream_UpdatedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaST_UPD_Stream_UpdatedAt();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbMO_UPD_Module_UpdatedAt ON dbo.MO_UPD_Module_UpdatedAt;
-- DROP FUNCTION IF EXISTS dbo.tcbMO_UPD_Module_UpdatedAt();
CREATE OR REPLACE FUNCTION dbo.tcbMO_UPD_Module_UpdatedAt() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_MO_UPD_Module_UpdatedAt (
            MO_UPD_MO_ID int not null,
            Metadata_MO_UPD int not null,
            MO_UPD_ChangedAt timestamp not null,
            MO_UPD_Module_UpdatedAt timestamp not null,
            MO_UPD_Version bigint not null,
            MO_UPD_StatementType char(1) not null,
            primary key(
                MO_UPD_Version,
                MO_UPD_MO_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbMO_UPD_Module_UpdatedAt
BEFORE INSERT ON dbo.MO_UPD_Module_UpdatedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbMO_UPD_Module_UpdatedAt();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciMO_UPD_Module_UpdatedAt ON dbo.MO_UPD_Module_UpdatedAt;
-- DROP FUNCTION IF EXISTS dbo.tciMO_UPD_Module_UpdatedAt();
CREATE OR REPLACE FUNCTION dbo.tciMO_UPD_Module_UpdatedAt() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_MO_UPD_Module_UpdatedAt
        SELECT
            NEW.MO_UPD_MO_ID,
            NEW.Metadata_MO_UPD,
            NEW.MO_UPD_ChangedAt,
            NEW.MO_UPD_Module_UpdatedAt,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciMO_UPD_Module_UpdatedAt
INSTEAD OF INSERT ON dbo.MO_UPD_Module_UpdatedAt
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciMO_UPD_Module_UpdatedAt();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaMO_UPD_Module_UpdatedAt ON dbo.MO_UPD_Module_UpdatedAt;
-- DROP FUNCTION IF EXISTS dbo.tcaMO_UPD_Module_UpdatedAt();
CREATE OR REPLACE FUNCTION dbo.tcaMO_UPD_Module_UpdatedAt() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_MO_UPD_Module_UpdatedAt
    SET MO_UPD_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    MO_UPD_MO_ID
                ORDER BY
                    MO_UPD_ChangedAt ASC
            ) AS rank,
            MO_UPD_MO_ID AS pk
        FROM _tmp_MO_UPD_Module_UpdatedAt
    ) AS v
    WHERE MO_UPD_MO_ID = v.pk
    AND MO_UPD_Version = 0;
    -- find max version
    SELECT
        MAX(MO_UPD_Version) INTO maxVersion
    FROM
        _tmp_MO_UPD_Module_UpdatedAt;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_MO_UPD_Module_UpdatedAt
        SET
            MO_UPD_StatementType =
                CASE
                    WHEN UPD.MO_UPD_MO_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfMO_UPD_Module_UpdatedAt(
                        v.MO_UPD_MO_ID,
                        v.MO_UPD_Module_UpdatedAt,
                        v.MO_UPD_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_MO_UPD_Module_UpdatedAt v
        LEFT JOIN
            dbo._MO_UPD_Module_UpdatedAt UPD
        ON
            UPD.MO_UPD_MO_ID = v.MO_UPD_MO_ID
        AND
            UPD.MO_UPD_ChangedAt = v.MO_UPD_ChangedAt
        AND
            UPD.MO_UPD_Module_UpdatedAt = v.MO_UPD_Module_UpdatedAt
        WHERE
            v.MO_UPD_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._MO_UPD_Module_UpdatedAt (
            MO_UPD_MO_ID,
            Metadata_MO_UPD,
            MO_UPD_ChangedAt,
            MO_UPD_Module_UpdatedAt
        )
        SELECT
            MO_UPD_MO_ID,
            Metadata_MO_UPD,
            MO_UPD_ChangedAt,
            MO_UPD_Module_UpdatedAt
        FROM
            _tmp_MO_UPD_Module_UpdatedAt
        WHERE
            MO_UPD_Version = currentVersion
        AND
            MO_UPD_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_MO_UPD_Module_UpdatedAt;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaMO_UPD_Module_UpdatedAt
AFTER INSERT ON dbo.MO_UPD_Module_UpdatedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaMO_UPD_Module_UpdatedAt();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbMO_TIT_Module_Title ON dbo.MO_TIT_Module_Title;
-- DROP FUNCTION IF EXISTS dbo.tcbMO_TIT_Module_Title();
CREATE OR REPLACE FUNCTION dbo.tcbMO_TIT_Module_Title() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_MO_TIT_Module_Title (
            MO_TIT_MO_ID int not null,
            Metadata_MO_TIT int not null,
            MO_TIT_ChangedAt timestamp not null,
            MO_TIT_Module_Title varchar(255) not null,
            MO_TIT_Version bigint not null,
            MO_TIT_StatementType char(1) not null,
            primary key(
                MO_TIT_Version,
                MO_TIT_MO_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbMO_TIT_Module_Title
BEFORE INSERT ON dbo.MO_TIT_Module_Title
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbMO_TIT_Module_Title();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciMO_TIT_Module_Title ON dbo.MO_TIT_Module_Title;
-- DROP FUNCTION IF EXISTS dbo.tciMO_TIT_Module_Title();
CREATE OR REPLACE FUNCTION dbo.tciMO_TIT_Module_Title() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_MO_TIT_Module_Title
        SELECT
            NEW.MO_TIT_MO_ID,
            NEW.Metadata_MO_TIT,
            NEW.MO_TIT_ChangedAt,
            NEW.MO_TIT_Module_Title,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciMO_TIT_Module_Title
INSTEAD OF INSERT ON dbo.MO_TIT_Module_Title
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciMO_TIT_Module_Title();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaMO_TIT_Module_Title ON dbo.MO_TIT_Module_Title;
-- DROP FUNCTION IF EXISTS dbo.tcaMO_TIT_Module_Title();
CREATE OR REPLACE FUNCTION dbo.tcaMO_TIT_Module_Title() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_MO_TIT_Module_Title
    SET MO_TIT_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    MO_TIT_MO_ID
                ORDER BY
                    MO_TIT_ChangedAt ASC
            ) AS rank,
            MO_TIT_MO_ID AS pk
        FROM _tmp_MO_TIT_Module_Title
    ) AS v
    WHERE MO_TIT_MO_ID = v.pk
    AND MO_TIT_Version = 0;
    -- find max version
    SELECT
        MAX(MO_TIT_Version) INTO maxVersion
    FROM
        _tmp_MO_TIT_Module_Title;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_MO_TIT_Module_Title
        SET
            MO_TIT_StatementType =
                CASE
                    WHEN TIT.MO_TIT_MO_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfMO_TIT_Module_Title(
                        v.MO_TIT_MO_ID,
                        v.MO_TIT_Module_Title,
                        v.MO_TIT_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_MO_TIT_Module_Title v
        LEFT JOIN
            dbo._MO_TIT_Module_Title TIT
        ON
            TIT.MO_TIT_MO_ID = v.MO_TIT_MO_ID
        AND
            TIT.MO_TIT_ChangedAt = v.MO_TIT_ChangedAt
        AND
            TIT.MO_TIT_Module_Title = v.MO_TIT_Module_Title
        WHERE
            v.MO_TIT_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._MO_TIT_Module_Title (
            MO_TIT_MO_ID,
            Metadata_MO_TIT,
            MO_TIT_ChangedAt,
            MO_TIT_Module_Title
        )
        SELECT
            MO_TIT_MO_ID,
            Metadata_MO_TIT,
            MO_TIT_ChangedAt,
            MO_TIT_Module_Title
        FROM
            _tmp_MO_TIT_Module_Title
        WHERE
            MO_TIT_Version = currentVersion
        AND
            MO_TIT_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_MO_TIT_Module_Title;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaMO_TIT_Module_Title
AFTER INSERT ON dbo.MO_TIT_Module_Title
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaMO_TIT_Module_Title();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbMO_CRE_Module_CreatedAt ON dbo.MO_CRE_Module_CreatedAt;
-- DROP FUNCTION IF EXISTS dbo.tcbMO_CRE_Module_CreatedAt();
CREATE OR REPLACE FUNCTION dbo.tcbMO_CRE_Module_CreatedAt() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_MO_CRE_Module_CreatedAt (
            MO_CRE_MO_ID int not null,
            Metadata_MO_CRE int not null,
            MO_CRE_Module_CreatedAt timestamp not null,
            MO_CRE_Version bigint not null,
            MO_CRE_StatementType char(1) not null,
            primary key(
                MO_CRE_Version,
                MO_CRE_MO_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbMO_CRE_Module_CreatedAt
BEFORE INSERT ON dbo.MO_CRE_Module_CreatedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbMO_CRE_Module_CreatedAt();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciMO_CRE_Module_CreatedAt ON dbo.MO_CRE_Module_CreatedAt;
-- DROP FUNCTION IF EXISTS dbo.tciMO_CRE_Module_CreatedAt();
CREATE OR REPLACE FUNCTION dbo.tciMO_CRE_Module_CreatedAt() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_MO_CRE_Module_CreatedAt
        SELECT
            NEW.MO_CRE_MO_ID,
            NEW.Metadata_MO_CRE,
            NEW.MO_CRE_Module_CreatedAt,
            1,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciMO_CRE_Module_CreatedAt
INSTEAD OF INSERT ON dbo.MO_CRE_Module_CreatedAt
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciMO_CRE_Module_CreatedAt();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaMO_CRE_Module_CreatedAt ON dbo.MO_CRE_Module_CreatedAt;
-- DROP FUNCTION IF EXISTS dbo.tcaMO_CRE_Module_CreatedAt();
CREATE OR REPLACE FUNCTION dbo.tcaMO_CRE_Module_CreatedAt() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find max version
    SELECT
        MAX(MO_CRE_Version) INTO maxVersion
    FROM
        _tmp_MO_CRE_Module_CreatedAt;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_MO_CRE_Module_CreatedAt
        SET
            MO_CRE_StatementType =
                CASE
                    WHEN CRE.MO_CRE_MO_ID is not null
                    THEN ''D'' -- duplicate
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_MO_CRE_Module_CreatedAt v
        LEFT JOIN
            dbo._MO_CRE_Module_CreatedAt CRE
        ON
            CRE.MO_CRE_MO_ID = v.MO_CRE_MO_ID
        AND
            CRE.MO_CRE_Module_CreatedAt = v.MO_CRE_Module_CreatedAt
        WHERE
            v.MO_CRE_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._MO_CRE_Module_CreatedAt (
            MO_CRE_MO_ID,
            Metadata_MO_CRE,
            MO_CRE_Module_CreatedAt
        )
        SELECT
            MO_CRE_MO_ID,
            Metadata_MO_CRE,
            MO_CRE_Module_CreatedAt
        FROM
            _tmp_MO_CRE_Module_CreatedAt
        WHERE
            MO_CRE_Version = currentVersion
        AND
            MO_CRE_StatementType in (''N'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_MO_CRE_Module_CreatedAt;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaMO_CRE_Module_CreatedAt
AFTER INSERT ON dbo.MO_CRE_Module_CreatedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaMO_CRE_Module_CreatedAt();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbMO_ORD_Module_OrderInStream ON dbo.MO_ORD_Module_OrderInStream;
-- DROP FUNCTION IF EXISTS dbo.tcbMO_ORD_Module_OrderInStream();
CREATE OR REPLACE FUNCTION dbo.tcbMO_ORD_Module_OrderInStream() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_MO_ORD_Module_OrderInStream (
            MO_ORD_MO_ID int not null,
            Metadata_MO_ORD int not null,
            MO_ORD_ChangedAt timestamp not null,
            MO_ORD_Module_OrderInStream int not null,
            MO_ORD_Version bigint not null,
            MO_ORD_StatementType char(1) not null,
            primary key(
                MO_ORD_Version,
                MO_ORD_MO_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbMO_ORD_Module_OrderInStream
BEFORE INSERT ON dbo.MO_ORD_Module_OrderInStream
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbMO_ORD_Module_OrderInStream();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciMO_ORD_Module_OrderInStream ON dbo.MO_ORD_Module_OrderInStream;
-- DROP FUNCTION IF EXISTS dbo.tciMO_ORD_Module_OrderInStream();
CREATE OR REPLACE FUNCTION dbo.tciMO_ORD_Module_OrderInStream() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_MO_ORD_Module_OrderInStream
        SELECT
            NEW.MO_ORD_MO_ID,
            NEW.Metadata_MO_ORD,
            NEW.MO_ORD_ChangedAt,
            NEW.MO_ORD_Module_OrderInStream,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciMO_ORD_Module_OrderInStream
INSTEAD OF INSERT ON dbo.MO_ORD_Module_OrderInStream
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciMO_ORD_Module_OrderInStream();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaMO_ORD_Module_OrderInStream ON dbo.MO_ORD_Module_OrderInStream;
-- DROP FUNCTION IF EXISTS dbo.tcaMO_ORD_Module_OrderInStream();
CREATE OR REPLACE FUNCTION dbo.tcaMO_ORD_Module_OrderInStream() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_MO_ORD_Module_OrderInStream
    SET MO_ORD_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    MO_ORD_MO_ID
                ORDER BY
                    MO_ORD_ChangedAt ASC
            ) AS rank,
            MO_ORD_MO_ID AS pk
        FROM _tmp_MO_ORD_Module_OrderInStream
    ) AS v
    WHERE MO_ORD_MO_ID = v.pk
    AND MO_ORD_Version = 0;
    -- find max version
    SELECT
        MAX(MO_ORD_Version) INTO maxVersion
    FROM
        _tmp_MO_ORD_Module_OrderInStream;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_MO_ORD_Module_OrderInStream
        SET
            MO_ORD_StatementType =
                CASE
                    WHEN ORD.MO_ORD_MO_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfMO_ORD_Module_OrderInStream(
                        v.MO_ORD_MO_ID,
                        v.MO_ORD_Module_OrderInStream,
                        v.MO_ORD_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_MO_ORD_Module_OrderInStream v
        LEFT JOIN
            dbo._MO_ORD_Module_OrderInStream ORD
        ON
            ORD.MO_ORD_MO_ID = v.MO_ORD_MO_ID
        AND
            ORD.MO_ORD_ChangedAt = v.MO_ORD_ChangedAt
        AND
            ORD.MO_ORD_Module_OrderInStream = v.MO_ORD_Module_OrderInStream
        WHERE
            v.MO_ORD_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._MO_ORD_Module_OrderInStream (
            MO_ORD_MO_ID,
            Metadata_MO_ORD,
            MO_ORD_ChangedAt,
            MO_ORD_Module_OrderInStream
        )
        SELECT
            MO_ORD_MO_ID,
            Metadata_MO_ORD,
            MO_ORD_ChangedAt,
            MO_ORD_Module_OrderInStream
        FROM
            _tmp_MO_ORD_Module_OrderInStream
        WHERE
            MO_ORD_Version = currentVersion
        AND
            MO_ORD_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_MO_ORD_Module_OrderInStream;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaMO_ORD_Module_OrderInStream
AFTER INSERT ON dbo.MO_ORD_Module_OrderInStream
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaMO_ORD_Module_OrderInStream();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbMO_DEL_Module_DeletedAt ON dbo.MO_DEL_Module_DeletedAt;
-- DROP FUNCTION IF EXISTS dbo.tcbMO_DEL_Module_DeletedAt();
CREATE OR REPLACE FUNCTION dbo.tcbMO_DEL_Module_DeletedAt() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_MO_DEL_Module_DeletedAt (
            MO_DEL_MO_ID int not null,
            Metadata_MO_DEL int not null,
            MO_DEL_Module_DeletedAt timestamp not null,
            MO_DEL_Version bigint not null,
            MO_DEL_StatementType char(1) not null,
            primary key(
                MO_DEL_Version,
                MO_DEL_MO_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbMO_DEL_Module_DeletedAt
BEFORE INSERT ON dbo.MO_DEL_Module_DeletedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbMO_DEL_Module_DeletedAt();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciMO_DEL_Module_DeletedAt ON dbo.MO_DEL_Module_DeletedAt;
-- DROP FUNCTION IF EXISTS dbo.tciMO_DEL_Module_DeletedAt();
CREATE OR REPLACE FUNCTION dbo.tciMO_DEL_Module_DeletedAt() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_MO_DEL_Module_DeletedAt
        SELECT
            NEW.MO_DEL_MO_ID,
            NEW.Metadata_MO_DEL,
            NEW.MO_DEL_Module_DeletedAt,
            1,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciMO_DEL_Module_DeletedAt
INSTEAD OF INSERT ON dbo.MO_DEL_Module_DeletedAt
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciMO_DEL_Module_DeletedAt();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaMO_DEL_Module_DeletedAt ON dbo.MO_DEL_Module_DeletedAt;
-- DROP FUNCTION IF EXISTS dbo.tcaMO_DEL_Module_DeletedAt();
CREATE OR REPLACE FUNCTION dbo.tcaMO_DEL_Module_DeletedAt() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find max version
    SELECT
        MAX(MO_DEL_Version) INTO maxVersion
    FROM
        _tmp_MO_DEL_Module_DeletedAt;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_MO_DEL_Module_DeletedAt
        SET
            MO_DEL_StatementType =
                CASE
                    WHEN DEL.MO_DEL_MO_ID is not null
                    THEN ''D'' -- duplicate
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_MO_DEL_Module_DeletedAt v
        LEFT JOIN
            dbo._MO_DEL_Module_DeletedAt DEL
        ON
            DEL.MO_DEL_MO_ID = v.MO_DEL_MO_ID
        AND
            DEL.MO_DEL_Module_DeletedAt = v.MO_DEL_Module_DeletedAt
        WHERE
            v.MO_DEL_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._MO_DEL_Module_DeletedAt (
            MO_DEL_MO_ID,
            Metadata_MO_DEL,
            MO_DEL_Module_DeletedAt
        )
        SELECT
            MO_DEL_MO_ID,
            Metadata_MO_DEL,
            MO_DEL_Module_DeletedAt
        FROM
            _tmp_MO_DEL_Module_DeletedAt
        WHERE
            MO_DEL_Version = currentVersion
        AND
            MO_DEL_StatementType in (''N'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_MO_DEL_Module_DeletedAt;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaMO_DEL_Module_DeletedAt
AFTER INSERT ON dbo.MO_DEL_Module_DeletedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaMO_DEL_Module_DeletedAt();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbLE_DEL_Lesson_DeletedAt ON dbo.LE_DEL_Lesson_DeletedAt;
-- DROP FUNCTION IF EXISTS dbo.tcbLE_DEL_Lesson_DeletedAt();
CREATE OR REPLACE FUNCTION dbo.tcbLE_DEL_Lesson_DeletedAt() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_LE_DEL_Lesson_DeletedAt (
            LE_DEL_LE_ID int not null,
            Metadata_LE_DEL int not null,
            LE_DEL_Lesson_DeletedAt timestamp(0) not null,
            LE_DEL_Version bigint not null,
            LE_DEL_StatementType char(1) not null,
            primary key(
                LE_DEL_Version,
                LE_DEL_LE_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbLE_DEL_Lesson_DeletedAt
BEFORE INSERT ON dbo.LE_DEL_Lesson_DeletedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbLE_DEL_Lesson_DeletedAt();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciLE_DEL_Lesson_DeletedAt ON dbo.LE_DEL_Lesson_DeletedAt;
-- DROP FUNCTION IF EXISTS dbo.tciLE_DEL_Lesson_DeletedAt();
CREATE OR REPLACE FUNCTION dbo.tciLE_DEL_Lesson_DeletedAt() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_LE_DEL_Lesson_DeletedAt
        SELECT
            NEW.LE_DEL_LE_ID,
            NEW.Metadata_LE_DEL,
            NEW.LE_DEL_Lesson_DeletedAt,
            1,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciLE_DEL_Lesson_DeletedAt
INSTEAD OF INSERT ON dbo.LE_DEL_Lesson_DeletedAt
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciLE_DEL_Lesson_DeletedAt();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaLE_DEL_Lesson_DeletedAt ON dbo.LE_DEL_Lesson_DeletedAt;
-- DROP FUNCTION IF EXISTS dbo.tcaLE_DEL_Lesson_DeletedAt();
CREATE OR REPLACE FUNCTION dbo.tcaLE_DEL_Lesson_DeletedAt() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find max version
    SELECT
        MAX(LE_DEL_Version) INTO maxVersion
    FROM
        _tmp_LE_DEL_Lesson_DeletedAt;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_LE_DEL_Lesson_DeletedAt
        SET
            LE_DEL_StatementType =
                CASE
                    WHEN DEL.LE_DEL_LE_ID is not null
                    THEN ''D'' -- duplicate
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_LE_DEL_Lesson_DeletedAt v
        LEFT JOIN
            dbo._LE_DEL_Lesson_DeletedAt DEL
        ON
            DEL.LE_DEL_LE_ID = v.LE_DEL_LE_ID
        AND
            DEL.LE_DEL_Lesson_DeletedAt = v.LE_DEL_Lesson_DeletedAt
        WHERE
            v.LE_DEL_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._LE_DEL_Lesson_DeletedAt (
            LE_DEL_LE_ID,
            Metadata_LE_DEL,
            LE_DEL_Lesson_DeletedAt
        )
        SELECT
            LE_DEL_LE_ID,
            Metadata_LE_DEL,
            LE_DEL_Lesson_DeletedAt
        FROM
            _tmp_LE_DEL_Lesson_DeletedAt
        WHERE
            LE_DEL_Version = currentVersion
        AND
            LE_DEL_StatementType in (''N'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_LE_DEL_Lesson_DeletedAt;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaLE_DEL_Lesson_DeletedAt
AFTER INSERT ON dbo.LE_DEL_Lesson_DeletedAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaLE_DEL_Lesson_DeletedAt();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbLE_JOI_Lesson_OnlineLessonJoinUrl ON dbo.LE_JOI_Lesson_OnlineLessonJoinUrl;
-- DROP FUNCTION IF EXISTS dbo.tcbLE_JOI_Lesson_OnlineLessonJoinUrl();
CREATE OR REPLACE FUNCTION dbo.tcbLE_JOI_Lesson_OnlineLessonJoinUrl() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_LE_JOI_Lesson_OnlineLessonJoinUrl (
            LE_JOI_LE_ID int not null,
            Metadata_LE_JOI int not null,
            LE_JOI_ChangedAt timestamp not null,
            LE_JOI_Lesson_OnlineLessonJoinUrl varchar(255) not null,
            LE_JOI_Version bigint not null,
            LE_JOI_StatementType char(1) not null,
            primary key(
                LE_JOI_Version,
                LE_JOI_LE_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbLE_JOI_Lesson_OnlineLessonJoinUrl
BEFORE INSERT ON dbo.LE_JOI_Lesson_OnlineLessonJoinUrl
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbLE_JOI_Lesson_OnlineLessonJoinUrl();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciLE_JOI_Lesson_OnlineLessonJoinUrl ON dbo.LE_JOI_Lesson_OnlineLessonJoinUrl;
-- DROP FUNCTION IF EXISTS dbo.tciLE_JOI_Lesson_OnlineLessonJoinUrl();
CREATE OR REPLACE FUNCTION dbo.tciLE_JOI_Lesson_OnlineLessonJoinUrl() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_LE_JOI_Lesson_OnlineLessonJoinUrl
        SELECT
            NEW.LE_JOI_LE_ID,
            NEW.Metadata_LE_JOI,
            NEW.LE_JOI_ChangedAt,
            NEW.LE_JOI_Lesson_OnlineLessonJoinUrl,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciLE_JOI_Lesson_OnlineLessonJoinUrl
INSTEAD OF INSERT ON dbo.LE_JOI_Lesson_OnlineLessonJoinUrl
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciLE_JOI_Lesson_OnlineLessonJoinUrl();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaLE_JOI_Lesson_OnlineLessonJoinUrl ON dbo.LE_JOI_Lesson_OnlineLessonJoinUrl;
-- DROP FUNCTION IF EXISTS dbo.tcaLE_JOI_Lesson_OnlineLessonJoinUrl();
CREATE OR REPLACE FUNCTION dbo.tcaLE_JOI_Lesson_OnlineLessonJoinUrl() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_LE_JOI_Lesson_OnlineLessonJoinUrl
    SET LE_JOI_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    LE_JOI_LE_ID
                ORDER BY
                    LE_JOI_ChangedAt ASC
            ) AS rank,
            LE_JOI_LE_ID AS pk
        FROM _tmp_LE_JOI_Lesson_OnlineLessonJoinUrl
    ) AS v
    WHERE LE_JOI_LE_ID = v.pk
    AND LE_JOI_Version = 0;
    -- find max version
    SELECT
        MAX(LE_JOI_Version) INTO maxVersion
    FROM
        _tmp_LE_JOI_Lesson_OnlineLessonJoinUrl;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_LE_JOI_Lesson_OnlineLessonJoinUrl
        SET
            LE_JOI_StatementType =
                CASE
                    WHEN JOI.LE_JOI_LE_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfLE_JOI_Lesson_OnlineLessonJoinUrl(
                        v.LE_JOI_LE_ID,
                        v.LE_JOI_Lesson_OnlineLessonJoinUrl,
                        v.LE_JOI_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_LE_JOI_Lesson_OnlineLessonJoinUrl v
        LEFT JOIN
            dbo._LE_JOI_Lesson_OnlineLessonJoinUrl JOI
        ON
            JOI.LE_JOI_LE_ID = v.LE_JOI_LE_ID
        AND
            JOI.LE_JOI_ChangedAt = v.LE_JOI_ChangedAt
        AND
            JOI.LE_JOI_Lesson_OnlineLessonJoinUrl = v.LE_JOI_Lesson_OnlineLessonJoinUrl
        WHERE
            v.LE_JOI_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._LE_JOI_Lesson_OnlineLessonJoinUrl (
            LE_JOI_LE_ID,
            Metadata_LE_JOI,
            LE_JOI_ChangedAt,
            LE_JOI_Lesson_OnlineLessonJoinUrl
        )
        SELECT
            LE_JOI_LE_ID,
            Metadata_LE_JOI,
            LE_JOI_ChangedAt,
            LE_JOI_Lesson_OnlineLessonJoinUrl
        FROM
            _tmp_LE_JOI_Lesson_OnlineLessonJoinUrl
        WHERE
            LE_JOI_Version = currentVersion
        AND
            LE_JOI_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_LE_JOI_Lesson_OnlineLessonJoinUrl;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaLE_JOI_Lesson_OnlineLessonJoinUrl
AFTER INSERT ON dbo.LE_JOI_Lesson_OnlineLessonJoinUrl
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaLE_JOI_Lesson_OnlineLessonJoinUrl();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbLE_REC_Lesson_OnlineLessonRecordingUrl ON dbo.LE_REC_Lesson_OnlineLessonRecordingUrl;
-- DROP FUNCTION IF EXISTS dbo.tcbLE_REC_Lesson_OnlineLessonRecordingUrl();
CREATE OR REPLACE FUNCTION dbo.tcbLE_REC_Lesson_OnlineLessonRecordingUrl() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_LE_REC_Lesson_OnlineLessonRecordingUrl (
            LE_REC_LE_ID int not null,
            Metadata_LE_REC int not null,
            LE_REC_ChangedAt timestamp not null,
            LE_REC_Lesson_OnlineLessonRecordingUrl varchar(255) not null,
            LE_REC_Version bigint not null,
            LE_REC_StatementType char(1) not null,
            primary key(
                LE_REC_Version,
                LE_REC_LE_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbLE_REC_Lesson_OnlineLessonRecordingUrl
BEFORE INSERT ON dbo.LE_REC_Lesson_OnlineLessonRecordingUrl
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbLE_REC_Lesson_OnlineLessonRecordingUrl();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciLE_REC_Lesson_OnlineLessonRecordingUrl ON dbo.LE_REC_Lesson_OnlineLessonRecordingUrl;
-- DROP FUNCTION IF EXISTS dbo.tciLE_REC_Lesson_OnlineLessonRecordingUrl();
CREATE OR REPLACE FUNCTION dbo.tciLE_REC_Lesson_OnlineLessonRecordingUrl() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_LE_REC_Lesson_OnlineLessonRecordingUrl
        SELECT
            NEW.LE_REC_LE_ID,
            NEW.Metadata_LE_REC,
            NEW.LE_REC_ChangedAt,
            NEW.LE_REC_Lesson_OnlineLessonRecordingUrl,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciLE_REC_Lesson_OnlineLessonRecordingUrl
INSTEAD OF INSERT ON dbo.LE_REC_Lesson_OnlineLessonRecordingUrl
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciLE_REC_Lesson_OnlineLessonRecordingUrl();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaLE_REC_Lesson_OnlineLessonRecordingUrl ON dbo.LE_REC_Lesson_OnlineLessonRecordingUrl;
-- DROP FUNCTION IF EXISTS dbo.tcaLE_REC_Lesson_OnlineLessonRecordingUrl();
CREATE OR REPLACE FUNCTION dbo.tcaLE_REC_Lesson_OnlineLessonRecordingUrl() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_LE_REC_Lesson_OnlineLessonRecordingUrl
    SET LE_REC_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    LE_REC_LE_ID
                ORDER BY
                    LE_REC_ChangedAt ASC
            ) AS rank,
            LE_REC_LE_ID AS pk
        FROM _tmp_LE_REC_Lesson_OnlineLessonRecordingUrl
    ) AS v
    WHERE LE_REC_LE_ID = v.pk
    AND LE_REC_Version = 0;
    -- find max version
    SELECT
        MAX(LE_REC_Version) INTO maxVersion
    FROM
        _tmp_LE_REC_Lesson_OnlineLessonRecordingUrl;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_LE_REC_Lesson_OnlineLessonRecordingUrl
        SET
            LE_REC_StatementType =
                CASE
                    WHEN REC.LE_REC_LE_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfLE_REC_Lesson_OnlineLessonRecordingUrl(
                        v.LE_REC_LE_ID,
                        v.LE_REC_Lesson_OnlineLessonRecordingUrl,
                        v.LE_REC_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_LE_REC_Lesson_OnlineLessonRecordingUrl v
        LEFT JOIN
            dbo._LE_REC_Lesson_OnlineLessonRecordingUrl REC
        ON
            REC.LE_REC_LE_ID = v.LE_REC_LE_ID
        AND
            REC.LE_REC_ChangedAt = v.LE_REC_ChangedAt
        AND
            REC.LE_REC_Lesson_OnlineLessonRecordingUrl = v.LE_REC_Lesson_OnlineLessonRecordingUrl
        WHERE
            v.LE_REC_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._LE_REC_Lesson_OnlineLessonRecordingUrl (
            LE_REC_LE_ID,
            Metadata_LE_REC,
            LE_REC_ChangedAt,
            LE_REC_Lesson_OnlineLessonRecordingUrl
        )
        SELECT
            LE_REC_LE_ID,
            Metadata_LE_REC,
            LE_REC_ChangedAt,
            LE_REC_Lesson_OnlineLessonRecordingUrl
        FROM
            _tmp_LE_REC_Lesson_OnlineLessonRecordingUrl
        WHERE
            LE_REC_Version = currentVersion
        AND
            LE_REC_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_LE_REC_Lesson_OnlineLessonRecordingUrl;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaLE_REC_Lesson_OnlineLessonRecordingUrl
AFTER INSERT ON dbo.LE_REC_Lesson_OnlineLessonRecordingUrl
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaLE_REC_Lesson_OnlineLessonRecordingUrl();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbLE_TIT_Lesson_Title ON dbo.LE_TIT_Lesson_Title;
-- DROP FUNCTION IF EXISTS dbo.tcbLE_TIT_Lesson_Title();
CREATE OR REPLACE FUNCTION dbo.tcbLE_TIT_Lesson_Title() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_LE_TIT_Lesson_Title (
            LE_TIT_LE_ID int not null,
            Metadata_LE_TIT int not null,
            LE_TIT_ChangedAt timestamp not null,
            LE_TIT_Lesson_Title varchar(255) not null,
            LE_TIT_Version bigint not null,
            LE_TIT_StatementType char(1) not null,
            primary key(
                LE_TIT_Version,
                LE_TIT_LE_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbLE_TIT_Lesson_Title
BEFORE INSERT ON dbo.LE_TIT_Lesson_Title
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbLE_TIT_Lesson_Title();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciLE_TIT_Lesson_Title ON dbo.LE_TIT_Lesson_Title;
-- DROP FUNCTION IF EXISTS dbo.tciLE_TIT_Lesson_Title();
CREATE OR REPLACE FUNCTION dbo.tciLE_TIT_Lesson_Title() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_LE_TIT_Lesson_Title
        SELECT
            NEW.LE_TIT_LE_ID,
            NEW.Metadata_LE_TIT,
            NEW.LE_TIT_ChangedAt,
            NEW.LE_TIT_Lesson_Title,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciLE_TIT_Lesson_Title
INSTEAD OF INSERT ON dbo.LE_TIT_Lesson_Title
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciLE_TIT_Lesson_Title();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaLE_TIT_Lesson_Title ON dbo.LE_TIT_Lesson_Title;
-- DROP FUNCTION IF EXISTS dbo.tcaLE_TIT_Lesson_Title();
CREATE OR REPLACE FUNCTION dbo.tcaLE_TIT_Lesson_Title() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_LE_TIT_Lesson_Title
    SET LE_TIT_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    LE_TIT_LE_ID
                ORDER BY
                    LE_TIT_ChangedAt ASC
            ) AS rank,
            LE_TIT_LE_ID AS pk
        FROM _tmp_LE_TIT_Lesson_Title
    ) AS v
    WHERE LE_TIT_LE_ID = v.pk
    AND LE_TIT_Version = 0;
    -- find max version
    SELECT
        MAX(LE_TIT_Version) INTO maxVersion
    FROM
        _tmp_LE_TIT_Lesson_Title;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_LE_TIT_Lesson_Title
        SET
            LE_TIT_StatementType =
                CASE
                    WHEN TIT.LE_TIT_LE_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfLE_TIT_Lesson_Title(
                        v.LE_TIT_LE_ID,
                        v.LE_TIT_Lesson_Title,
                        v.LE_TIT_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_LE_TIT_Lesson_Title v
        LEFT JOIN
            dbo._LE_TIT_Lesson_Title TIT
        ON
            TIT.LE_TIT_LE_ID = v.LE_TIT_LE_ID
        AND
            TIT.LE_TIT_ChangedAt = v.LE_TIT_ChangedAt
        AND
            TIT.LE_TIT_Lesson_Title = v.LE_TIT_Lesson_Title
        WHERE
            v.LE_TIT_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._LE_TIT_Lesson_Title (
            LE_TIT_LE_ID,
            Metadata_LE_TIT,
            LE_TIT_ChangedAt,
            LE_TIT_Lesson_Title
        )
        SELECT
            LE_TIT_LE_ID,
            Metadata_LE_TIT,
            LE_TIT_ChangedAt,
            LE_TIT_Lesson_Title
        FROM
            _tmp_LE_TIT_Lesson_Title
        WHERE
            LE_TIT_Version = currentVersion
        AND
            LE_TIT_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_LE_TIT_Lesson_Title;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaLE_TIT_Lesson_Title
AFTER INSERT ON dbo.LE_TIT_Lesson_Title
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaLE_TIT_Lesson_Title();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbLE_EDT_Lesson_EndAt ON dbo.LE_EDT_Lesson_EndAt;
-- DROP FUNCTION IF EXISTS dbo.tcbLE_EDT_Lesson_EndAt();
CREATE OR REPLACE FUNCTION dbo.tcbLE_EDT_Lesson_EndAt() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_LE_EDT_Lesson_EndAt (
            LE_EDT_LE_ID int not null,
            Metadata_LE_EDT int not null,
            LE_EDT_ChangedAt timestamp not null,
            LE_EDT_Lesson_EndAt timestamp not null,
            LE_EDT_Version bigint not null,
            LE_EDT_StatementType char(1) not null,
            primary key(
                LE_EDT_Version,
                LE_EDT_LE_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbLE_EDT_Lesson_EndAt
BEFORE INSERT ON dbo.LE_EDT_Lesson_EndAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbLE_EDT_Lesson_EndAt();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciLE_EDT_Lesson_EndAt ON dbo.LE_EDT_Lesson_EndAt;
-- DROP FUNCTION IF EXISTS dbo.tciLE_EDT_Lesson_EndAt();
CREATE OR REPLACE FUNCTION dbo.tciLE_EDT_Lesson_EndAt() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_LE_EDT_Lesson_EndAt
        SELECT
            NEW.LE_EDT_LE_ID,
            NEW.Metadata_LE_EDT,
            NEW.LE_EDT_ChangedAt,
            NEW.LE_EDT_Lesson_EndAt,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciLE_EDT_Lesson_EndAt
INSTEAD OF INSERT ON dbo.LE_EDT_Lesson_EndAt
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciLE_EDT_Lesson_EndAt();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaLE_EDT_Lesson_EndAt ON dbo.LE_EDT_Lesson_EndAt;
-- DROP FUNCTION IF EXISTS dbo.tcaLE_EDT_Lesson_EndAt();
CREATE OR REPLACE FUNCTION dbo.tcaLE_EDT_Lesson_EndAt() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_LE_EDT_Lesson_EndAt
    SET LE_EDT_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    LE_EDT_LE_ID
                ORDER BY
                    LE_EDT_ChangedAt ASC
            ) AS rank,
            LE_EDT_LE_ID AS pk
        FROM _tmp_LE_EDT_Lesson_EndAt
    ) AS v
    WHERE LE_EDT_LE_ID = v.pk
    AND LE_EDT_Version = 0;
    -- find max version
    SELECT
        MAX(LE_EDT_Version) INTO maxVersion
    FROM
        _tmp_LE_EDT_Lesson_EndAt;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_LE_EDT_Lesson_EndAt
        SET
            LE_EDT_StatementType =
                CASE
                    WHEN EDT.LE_EDT_LE_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfLE_EDT_Lesson_EndAt(
                        v.LE_EDT_LE_ID,
                        v.LE_EDT_Lesson_EndAt,
                        v.LE_EDT_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_LE_EDT_Lesson_EndAt v
        LEFT JOIN
            dbo._LE_EDT_Lesson_EndAt EDT
        ON
            EDT.LE_EDT_LE_ID = v.LE_EDT_LE_ID
        AND
            EDT.LE_EDT_ChangedAt = v.LE_EDT_ChangedAt
        AND
            EDT.LE_EDT_Lesson_EndAt = v.LE_EDT_Lesson_EndAt
        WHERE
            v.LE_EDT_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._LE_EDT_Lesson_EndAt (
            LE_EDT_LE_ID,
            Metadata_LE_EDT,
            LE_EDT_ChangedAt,
            LE_EDT_Lesson_EndAt
        )
        SELECT
            LE_EDT_LE_ID,
            Metadata_LE_EDT,
            LE_EDT_ChangedAt,
            LE_EDT_Lesson_EndAt
        FROM
            _tmp_LE_EDT_Lesson_EndAt
        WHERE
            LE_EDT_Version = currentVersion
        AND
            LE_EDT_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_LE_EDT_Lesson_EndAt;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaLE_EDT_Lesson_EndAt
AFTER INSERT ON dbo.LE_EDT_Lesson_EndAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaLE_EDT_Lesson_EndAt();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbLE_HOM_Lesson_HomeWorkUrl ON dbo.LE_HOM_Lesson_HomeWorkUrl;
-- DROP FUNCTION IF EXISTS dbo.tcbLE_HOM_Lesson_HomeWorkUrl();
CREATE OR REPLACE FUNCTION dbo.tcbLE_HOM_Lesson_HomeWorkUrl() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_LE_HOM_Lesson_HomeWorkUrl (
            LE_HOM_LE_ID int not null,
            Metadata_LE_HOM int not null,
            LE_HOM_ChangedAt timestamp not null,
            LE_HOM_Lesson_HomeWorkUrl varchar(500) not null,
            LE_HOM_Version bigint not null,
            LE_HOM_StatementType char(1) not null,
            primary key(
                LE_HOM_Version,
                LE_HOM_LE_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbLE_HOM_Lesson_HomeWorkUrl
BEFORE INSERT ON dbo.LE_HOM_Lesson_HomeWorkUrl
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbLE_HOM_Lesson_HomeWorkUrl();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciLE_HOM_Lesson_HomeWorkUrl ON dbo.LE_HOM_Lesson_HomeWorkUrl;
-- DROP FUNCTION IF EXISTS dbo.tciLE_HOM_Lesson_HomeWorkUrl();
CREATE OR REPLACE FUNCTION dbo.tciLE_HOM_Lesson_HomeWorkUrl() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_LE_HOM_Lesson_HomeWorkUrl
        SELECT
            NEW.LE_HOM_LE_ID,
            NEW.Metadata_LE_HOM,
            NEW.LE_HOM_ChangedAt,
            NEW.LE_HOM_Lesson_HomeWorkUrl,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciLE_HOM_Lesson_HomeWorkUrl
INSTEAD OF INSERT ON dbo.LE_HOM_Lesson_HomeWorkUrl
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciLE_HOM_Lesson_HomeWorkUrl();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaLE_HOM_Lesson_HomeWorkUrl ON dbo.LE_HOM_Lesson_HomeWorkUrl;
-- DROP FUNCTION IF EXISTS dbo.tcaLE_HOM_Lesson_HomeWorkUrl();
CREATE OR REPLACE FUNCTION dbo.tcaLE_HOM_Lesson_HomeWorkUrl() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_LE_HOM_Lesson_HomeWorkUrl
    SET LE_HOM_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    LE_HOM_LE_ID
                ORDER BY
                    LE_HOM_ChangedAt ASC
            ) AS rank,
            LE_HOM_LE_ID AS pk
        FROM _tmp_LE_HOM_Lesson_HomeWorkUrl
    ) AS v
    WHERE LE_HOM_LE_ID = v.pk
    AND LE_HOM_Version = 0;
    -- find max version
    SELECT
        MAX(LE_HOM_Version) INTO maxVersion
    FROM
        _tmp_LE_HOM_Lesson_HomeWorkUrl;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_LE_HOM_Lesson_HomeWorkUrl
        SET
            LE_HOM_StatementType =
                CASE
                    WHEN HOM.LE_HOM_LE_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfLE_HOM_Lesson_HomeWorkUrl(
                        v.LE_HOM_LE_ID,
                        v.LE_HOM_Lesson_HomeWorkUrl,
                        v.LE_HOM_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_LE_HOM_Lesson_HomeWorkUrl v
        LEFT JOIN
            dbo._LE_HOM_Lesson_HomeWorkUrl HOM
        ON
            HOM.LE_HOM_LE_ID = v.LE_HOM_LE_ID
        AND
            HOM.LE_HOM_ChangedAt = v.LE_HOM_ChangedAt
        AND
            HOM.LE_HOM_Lesson_HomeWorkUrl = v.LE_HOM_Lesson_HomeWorkUrl
        WHERE
            v.LE_HOM_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._LE_HOM_Lesson_HomeWorkUrl (
            LE_HOM_LE_ID,
            Metadata_LE_HOM,
            LE_HOM_ChangedAt,
            LE_HOM_Lesson_HomeWorkUrl
        )
        SELECT
            LE_HOM_LE_ID,
            Metadata_LE_HOM,
            LE_HOM_ChangedAt,
            LE_HOM_Lesson_HomeWorkUrl
        FROM
            _tmp_LE_HOM_Lesson_HomeWorkUrl
        WHERE
            LE_HOM_Version = currentVersion
        AND
            LE_HOM_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_LE_HOM_Lesson_HomeWorkUrl;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaLE_HOM_Lesson_HomeWorkUrl
AFTER INSERT ON dbo.LE_HOM_Lesson_HomeWorkUrl
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaLE_HOM_Lesson_HomeWorkUrl();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbLE_DES_Lesson_Description ON dbo.LE_DES_Lesson_Description;
-- DROP FUNCTION IF EXISTS dbo.tcbLE_DES_Lesson_Description();
CREATE OR REPLACE FUNCTION dbo.tcbLE_DES_Lesson_Description() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_LE_DES_Lesson_Description (
            LE_DES_LE_ID int not null,
            Metadata_LE_DES int not null,
            LE_DES_ChangedAt timestamp not null,
            LE_DES_Lesson_Description text not null,
            LE_DES_Version bigint not null,
            LE_DES_StatementType char(1) not null,
            primary key(
                LE_DES_Version,
                LE_DES_LE_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbLE_DES_Lesson_Description
BEFORE INSERT ON dbo.LE_DES_Lesson_Description
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbLE_DES_Lesson_Description();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciLE_DES_Lesson_Description ON dbo.LE_DES_Lesson_Description;
-- DROP FUNCTION IF EXISTS dbo.tciLE_DES_Lesson_Description();
CREATE OR REPLACE FUNCTION dbo.tciLE_DES_Lesson_Description() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_LE_DES_Lesson_Description
        SELECT
            NEW.LE_DES_LE_ID,
            NEW.Metadata_LE_DES,
            NEW.LE_DES_ChangedAt,
            NEW.LE_DES_Lesson_Description,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciLE_DES_Lesson_Description
INSTEAD OF INSERT ON dbo.LE_DES_Lesson_Description
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciLE_DES_Lesson_Description();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaLE_DES_Lesson_Description ON dbo.LE_DES_Lesson_Description;
-- DROP FUNCTION IF EXISTS dbo.tcaLE_DES_Lesson_Description();
CREATE OR REPLACE FUNCTION dbo.tcaLE_DES_Lesson_Description() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_LE_DES_Lesson_Description
    SET LE_DES_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    LE_DES_LE_ID
                ORDER BY
                    LE_DES_ChangedAt ASC
            ) AS rank,
            LE_DES_LE_ID AS pk
        FROM _tmp_LE_DES_Lesson_Description
    ) AS v
    WHERE LE_DES_LE_ID = v.pk
    AND LE_DES_Version = 0;
    -- find max version
    SELECT
        MAX(LE_DES_Version) INTO maxVersion
    FROM
        _tmp_LE_DES_Lesson_Description;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_LE_DES_Lesson_Description
        SET
            LE_DES_StatementType =
                CASE
                    WHEN DES.LE_DES_LE_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfLE_DES_Lesson_Description(
                        v.LE_DES_LE_ID,
                        v.LE_DES_Lesson_Description,
                        v.LE_DES_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_LE_DES_Lesson_Description v
        LEFT JOIN
            dbo._LE_DES_Lesson_Description DES
        ON
            DES.LE_DES_LE_ID = v.LE_DES_LE_ID
        AND
            DES.LE_DES_ChangedAt = v.LE_DES_ChangedAt
        AND
            DES.LE_DES_Lesson_Description = v.LE_DES_Lesson_Description
        WHERE
            v.LE_DES_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._LE_DES_Lesson_Description (
            LE_DES_LE_ID,
            Metadata_LE_DES,
            LE_DES_ChangedAt,
            LE_DES_Lesson_Description
        )
        SELECT
            LE_DES_LE_ID,
            Metadata_LE_DES,
            LE_DES_ChangedAt,
            LE_DES_Lesson_Description
        FROM
            _tmp_LE_DES_Lesson_Description
        WHERE
            LE_DES_Version = currentVersion
        AND
            LE_DES_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_LE_DES_Lesson_Description;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaLE_DES_Lesson_Description
AFTER INSERT ON dbo.LE_DES_Lesson_Description
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaLE_DES_Lesson_Description();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbLE_STA_Lesson_StartAt ON dbo.LE_STA_Lesson_StartAt;
-- DROP FUNCTION IF EXISTS dbo.tcbLE_STA_Lesson_StartAt();
CREATE OR REPLACE FUNCTION dbo.tcbLE_STA_Lesson_StartAt() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_LE_STA_Lesson_StartAt (
            LE_STA_LE_ID int not null,
            Metadata_LE_STA int not null,
            LE_STA_ChangedAt timestamp not null,
            LE_STA_Lesson_StartAt timestamp not null,
            LE_STA_Version bigint not null,
            LE_STA_StatementType char(1) not null,
            primary key(
                LE_STA_Version,
                LE_STA_LE_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbLE_STA_Lesson_StartAt
BEFORE INSERT ON dbo.LE_STA_Lesson_StartAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcbLE_STA_Lesson_StartAt();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciLE_STA_Lesson_StartAt ON dbo.LE_STA_Lesson_StartAt;
-- DROP FUNCTION IF EXISTS dbo.tciLE_STA_Lesson_StartAt();
CREATE OR REPLACE FUNCTION dbo.tciLE_STA_Lesson_StartAt() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_LE_STA_Lesson_StartAt
        SELECT
            NEW.LE_STA_LE_ID,
            NEW.Metadata_LE_STA,
            NEW.LE_STA_ChangedAt,
            NEW.LE_STA_Lesson_StartAt,
            0,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciLE_STA_Lesson_StartAt
INSTEAD OF INSERT ON dbo.LE_STA_Lesson_StartAt
FOR EACH ROW
EXECUTE PROCEDURE dbo.tciLE_STA_Lesson_StartAt();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaLE_STA_Lesson_StartAt ON dbo.LE_STA_Lesson_StartAt;
-- DROP FUNCTION IF EXISTS dbo.tcaLE_STA_Lesson_StartAt();
CREATE OR REPLACE FUNCTION dbo.tcaLE_STA_Lesson_StartAt() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find ranks for inserted data (using self join)
    UPDATE _tmp_LE_STA_Lesson_StartAt
    SET LE_STA_Version = v.rank
    FROM (
        SELECT
            DENSE_RANK() OVER (
                PARTITION BY
                    LE_STA_LE_ID
                ORDER BY
                    LE_STA_ChangedAt ASC
            ) AS rank,
            LE_STA_LE_ID AS pk
        FROM _tmp_LE_STA_Lesson_StartAt
    ) AS v
    WHERE LE_STA_LE_ID = v.pk
    AND LE_STA_Version = 0;
    -- find max version
    SELECT
        MAX(LE_STA_Version) INTO maxVersion
    FROM
        _tmp_LE_STA_Lesson_StartAt;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_LE_STA_Lesson_StartAt
        SET
            LE_STA_StatementType =
                CASE
                    WHEN STA.LE_STA_LE_ID is not null
                    THEN ''D'' -- duplicate
                    WHEN dbo.rfLE_STA_Lesson_StartAt(
                        v.LE_STA_LE_ID,
                        v.LE_STA_Lesson_StartAt,
                        v.LE_STA_ChangedAt
                    ) = 1
                    THEN ''R'' -- restatement
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_LE_STA_Lesson_StartAt v
        LEFT JOIN
            dbo._LE_STA_Lesson_StartAt STA
        ON
            STA.LE_STA_LE_ID = v.LE_STA_LE_ID
        AND
            STA.LE_STA_ChangedAt = v.LE_STA_ChangedAt
        AND
            STA.LE_STA_Lesson_StartAt = v.LE_STA_Lesson_StartAt
        WHERE
            v.LE_STA_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO dbo._LE_STA_Lesson_StartAt (
            LE_STA_LE_ID,
            Metadata_LE_STA,
            LE_STA_ChangedAt,
            LE_STA_Lesson_StartAt
        )
        SELECT
            LE_STA_LE_ID,
            Metadata_LE_STA,
            LE_STA_ChangedAt,
            LE_STA_Lesson_StartAt
        FROM
            _tmp_LE_STA_Lesson_StartAt
        WHERE
            LE_STA_Version = currentVersion
        AND
            LE_STA_StatementType in (''N'',''R'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_LE_STA_Lesson_StartAt;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaLE_STA_Lesson_StartAt
AFTER INSERT ON dbo.LE_STA_Lesson_StartAt
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.tcaLE_STA_Lesson_StartAt();
-- ANCHOR TRIGGERS ---------------------------------------------------------------------------------------------------
--
-- The following triggers on the latest view make it behave like a table.
-- There are three different 'instead of' triggers: insert, update, and delete.
-- They will ensure that such operations are propagated to the underlying tables
-- in a consistent way. Default values are used for some columns if not provided
-- by the corresponding SQL statements.
--
-- For idempotent attributes, only changes that represent a value different from
-- the previous or following value are stored. Others are silently ignored in
-- order to avoid unnecessary temporal duplicates.
--
-- BEFORE INSERT trigger --------------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS itb_lCO_Course ON dbo.lCO_Course;
--DROP FUNCTION IF EXISTS dbo.itb_lCO_Course();
CREATE OR REPLACE FUNCTION dbo.itb_lCO_Course() RETURNS trigger AS '
    BEGIN
        -- create temporary table to keep inserted rows in
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_it_CO_Course (
            CO_ID int not null,
            Metadata_CO int not null,
            CO_TIT_CO_ID int null,
            Metadata_CO_TIT int null,
            CO_TIT_ChangedAt timestamp null,
            CO_TIT_Course_Title varchar(255) null,
            CO_ICO_CO_ID int null,
            Metadata_CO_ICO int null,
            CO_ICO_ChangedAt timestamp null,
            CO_ICO_Course_IconUrl varchar(255) null,
            CO_DEL_CO_ID int null,
            Metadata_CO_DEL int null,
            CO_DEL_Course_Deleted timestamp(0) null,
            CO_CRE_CO_ID int null,
            Metadata_CO_CRE int null,
            CO_CRE_Course_Created timestamp null,
            CO_AUT_CO_ID int null,
            Metadata_CO_AUT int null,
            CO_AUT_AUT_IsAutoCourseEnrole bool null,
            CO_AUT_Metadata_AUT int null,
            CO_AUT_AUT_ID int null,
            CO_DEM_CO_ID int null,
            Metadata_CO_DEM int null,
            CO_DEM_DEM_IsDemoEnrole bool null,
            CO_DEM_Metadata_DEM int null,
            CO_DEM_DEM_ID int null,
            CO_UPD_CO_ID int null,
            Metadata_CO_UPD int null,
            CO_UPD_ChangedAt timestamp null,
            CO_UPD_Course_UpdatedAt timestamp null
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER itb_lCO_Course
BEFORE INSERT ON dbo.lCO_Course
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.itb_lCO_Course(); 
-- INSTEAD OF INSERT trigger ----------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS iti_lCO_Course ON dbo.lCO_Course;
--DROP FUNCTION IF EXISTS dbo.iti_lCO_Course();
CREATE OR REPLACE FUNCTION dbo.iti_lCO_Course() RETURNS trigger AS '
    BEGIN
        -- generate anchor ID (if not provided)
        IF (NEW.CO_ID IS NULL) THEN 
            INSERT INTO dbo.CO_Course (
                Metadata_CO 
            ) VALUES (
                NEW.Metadata_CO 
            );
            SELECT
                lastval() 
            INTO NEW.CO_ID;
        -- if anchor ID is provided then let''s insert it into the anchor table
        -- but only if that ID is not present in the anchor table
        ELSE
            INSERT INTO dbo.CO_Course (
                Metadata_CO,
                CO_ID
            )
            SELECT
                NEW.Metadata_CO,
                NEW.CO_ID
            WHERE NOT EXISTS(
	            SELECT
	                CO_ID 
	            FROM dbo.CO_Course
	            WHERE CO_ID = NEW.CO_ID
	            LIMIT 1
            );
        END IF;
        -- insert row into temporary table
    	INSERT INTO _tmp_it_CO_Course (
            CO_ID,
            Metadata_CO,
            CO_TIT_CO_ID,
            Metadata_CO_TIT,
            CO_TIT_ChangedAt,
            CO_TIT_Course_Title,
            CO_ICO_CO_ID,
            Metadata_CO_ICO,
            CO_ICO_ChangedAt,
            CO_ICO_Course_IconUrl,
            CO_DEL_CO_ID,
            Metadata_CO_DEL,
            CO_DEL_Course_Deleted,
            CO_CRE_CO_ID,
            Metadata_CO_CRE,
            CO_CRE_Course_Created,
            CO_AUT_CO_ID,
            Metadata_CO_AUT,
            CO_AUT_AUT_IsAutoCourseEnrole,
            CO_AUT_Metadata_AUT,
            CO_AUT_AUT_ID,
            CO_DEM_CO_ID,
            Metadata_CO_DEM,
            CO_DEM_DEM_IsDemoEnrole,
            CO_DEM_Metadata_DEM,
            CO_DEM_DEM_ID,
            CO_UPD_CO_ID,
            Metadata_CO_UPD,
            CO_UPD_ChangedAt,
            CO_UPD_Course_UpdatedAt
    	) VALUES (
    	    NEW.CO_ID,
            NEW.Metadata_CO,
            COALESCE(NEW.CO_TIT_CO_ID, NEW.CO_ID),
            COALESCE(NEW.Metadata_CO_TIT, NEW.Metadata_CO),
            COALESCE(NEW.CO_TIT_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.CO_TIT_Course_Title,
            COALESCE(NEW.CO_ICO_CO_ID, NEW.CO_ID),
            COALESCE(NEW.Metadata_CO_ICO, NEW.Metadata_CO),
            COALESCE(NEW.CO_ICO_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.CO_ICO_Course_IconUrl,
            COALESCE(NEW.CO_DEL_CO_ID, NEW.CO_ID),
            COALESCE(NEW.Metadata_CO_DEL, NEW.Metadata_CO),
            NEW.CO_DEL_Course_Deleted,
            COALESCE(NEW.CO_CRE_CO_ID, NEW.CO_ID),
            COALESCE(NEW.Metadata_CO_CRE, NEW.Metadata_CO),
            NEW.CO_CRE_Course_Created,
            COALESCE(NEW.CO_AUT_CO_ID, NEW.CO_ID),
            COALESCE(NEW.Metadata_CO_AUT, NEW.Metadata_CO),
            NEW.CO_AUT_AUT_IsAutoCourseEnrole,
            COALESCE(NEW.CO_AUT_Metadata_AUT, NEW.Metadata_CO),
            NEW.CO_AUT_AUT_ID,
            COALESCE(NEW.CO_DEM_CO_ID, NEW.CO_ID),
            COALESCE(NEW.Metadata_CO_DEM, NEW.Metadata_CO),
            NEW.CO_DEM_DEM_IsDemoEnrole,
            COALESCE(NEW.CO_DEM_Metadata_DEM, NEW.Metadata_CO),
            NEW.CO_DEM_DEM_ID,
            COALESCE(NEW.CO_UPD_CO_ID, NEW.CO_ID),
            COALESCE(NEW.Metadata_CO_UPD, NEW.Metadata_CO),
            COALESCE(NEW.CO_UPD_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.CO_UPD_Course_UpdatedAt
    	);
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER iti_lCO_Course
INSTEAD OF INSERT ON dbo.lCO_Course
FOR EACH ROW
EXECUTE PROCEDURE dbo.iti_lCO_Course();
-- AFTER INSERT trigger ---------------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS ita_lCO_Course ON dbo.lCO_Course;
--DROP FUNCTION IF EXISTS dbo.ita_lCO_Course();
CREATE OR REPLACE FUNCTION dbo.ita_lCO_Course() RETURNS trigger AS '
    BEGIN
        INSERT INTO dbo.CO_TIT_Course_Title (
            CO_TIT_CO_ID,
            Metadata_CO_TIT,
            CO_TIT_ChangedAt,
            CO_TIT_Course_Title
        )
        SELECT
            i.CO_TIT_CO_ID,
            i.Metadata_CO_TIT,
            i.CO_TIT_ChangedAt,
            i.CO_TIT_Course_Title
        FROM
            _tmp_it_CO_Course i
        WHERE
            i.CO_TIT_Course_Title is not null;
        INSERT INTO dbo.CO_ICO_Course_IconUrl (
            CO_ICO_CO_ID,
            Metadata_CO_ICO,
            CO_ICO_ChangedAt,
            CO_ICO_Course_IconUrl
        )
        SELECT
            i.CO_ICO_CO_ID,
            i.Metadata_CO_ICO,
            i.CO_ICO_ChangedAt,
            i.CO_ICO_Course_IconUrl
        FROM
            _tmp_it_CO_Course i
        WHERE
            i.CO_ICO_Course_IconUrl is not null;
        INSERT INTO dbo.CO_DEL_Course_Deleted (
            CO_DEL_CO_ID,
            Metadata_CO_DEL,
            CO_DEL_Course_Deleted
        )
        SELECT
            i.CO_DEL_CO_ID,
            i.Metadata_CO_DEL,
            i.CO_DEL_Course_Deleted
        FROM
            _tmp_it_CO_Course i
        WHERE
            i.CO_DEL_Course_Deleted is not null;
        INSERT INTO dbo.CO_CRE_Course_Created (
            CO_CRE_CO_ID,
            Metadata_CO_CRE,
            CO_CRE_Course_Created
        )
        SELECT
            i.CO_CRE_CO_ID,
            i.Metadata_CO_CRE,
            i.CO_CRE_Course_Created
        FROM
            _tmp_it_CO_Course i
        WHERE
            i.CO_CRE_Course_Created is not null;
        INSERT INTO dbo.CO_AUT_Course_IsAutoCourseEnrole (
            CO_AUT_CO_ID,
            Metadata_CO_AUT,
            CO_AUT_AUT_ID
        )
        SELECT
            i.CO_AUT_CO_ID,
            i.Metadata_CO_AUT,
            COALESCE(i.CO_AUT_AUT_ID, kAUT.AUT_ID) 
        FROM
            _tmp_it_CO_Course i
        LEFT JOIN
            dbo.AUT_IsAutoCourseEnrole kAUT
        ON
            kAUT.AUT_IsAutoCourseEnrole = i.CO_AUT_AUT_IsAutoCourseEnrole
        WHERE
            COALESCE(i.CO_AUT_AUT_ID, kAUT.AUT_ID) is not null;
        INSERT INTO dbo.CO_DEM_Course_IsDemoEnrole (
            CO_DEM_CO_ID,
            Metadata_CO_DEM,
            CO_DEM_DEM_ID
        )
        SELECT
            i.CO_DEM_CO_ID,
            i.Metadata_CO_DEM,
            COALESCE(i.CO_DEM_DEM_ID, kDEM.DEM_ID) 
        FROM
            _tmp_it_CO_Course i
        LEFT JOIN
            dbo.DEM_IsDemoEnrole kDEM
        ON
            kDEM.DEM_IsDemoEnrole = i.CO_DEM_DEM_IsDemoEnrole
        WHERE
            COALESCE(i.CO_DEM_DEM_ID, kDEM.DEM_ID) is not null;
        INSERT INTO dbo.CO_UPD_Course_UpdatedAt (
            CO_UPD_CO_ID,
            Metadata_CO_UPD,
            CO_UPD_ChangedAt,
            CO_UPD_Course_UpdatedAt
        )
        SELECT
            i.CO_UPD_CO_ID,
            i.Metadata_CO_UPD,
            i.CO_UPD_ChangedAt,
            i.CO_UPD_Course_UpdatedAt
        FROM
            _tmp_it_CO_Course i
        WHERE
            i.CO_UPD_Course_UpdatedAt is not null;
        DROP TABLE IF EXISTS _tmp_it_CO_Course;
        RETURN NULL;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER ita_lCO_Course
AFTER INSERT ON dbo.lCO_Course
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.ita_lCO_Course();
-- BEFORE INSERT trigger --------------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS itb_lST_Stream ON dbo.lST_Stream;
--DROP FUNCTION IF EXISTS dbo.itb_lST_Stream();
CREATE OR REPLACE FUNCTION dbo.itb_lST_Stream() RETURNS trigger AS '
    BEGIN
        -- create temporary table to keep inserted rows in
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_it_ST_Stream (
            ST_ID int not null,
            Metadata_ST int not null,
            ST_DEL_ST_ID int null,
            Metadata_ST_DEL int null,
            ST_DEL_Stream_DeletedAt timestamp null,
            ST_CRE_ST_ID int null,
            Metadata_ST_CRE int null,
            ST_CRE_Stream_CreatedAt timestamp null,
            ST_ENA_ST_ID int null,
            Metadata_ST_ENA int null,
            ST_ENA_ChangedAt timestamp null,
            ST_ENA_Stream_EndAt timestamp null,
            ST_STA_ST_ID int null,
            Metadata_ST_STA int null,
            ST_STA_ChangedAt timestamp null,
            ST_STA_Stream_StartAt timestamp null,
            ST_HOM_ST_ID int null,
            Metadata_ST_HOM int null,
            ST_HOM_ChangedAt timestamp null,
            ST_HOM_Stream_HomeworkDeadlineDays int null,
            ST_OPE_ST_ID int null,
            Metadata_ST_OPE int null,
            ST_OPE_OPE_IsOpen bool null,
            ST_OPE_Metadata_OPE int null,
            ST_OPE_OPE_ID int null,
            ST_NAM_ST_ID int null,
            Metadata_ST_NAM int null,
            ST_NAM_ChangedAt timestamp null,
            ST_NAM_Stream_Name varchar(255) null,
            ST_UPD_ST_ID int null,
            Metadata_ST_UPD int null,
            ST_UPD_ChangedAt timestamp null,
            ST_UPD_Stream_UpdatedAt timestamp null
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER itb_lST_Stream
BEFORE INSERT ON dbo.lST_Stream
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.itb_lST_Stream(); 
-- INSTEAD OF INSERT trigger ----------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS iti_lST_Stream ON dbo.lST_Stream;
--DROP FUNCTION IF EXISTS dbo.iti_lST_Stream();
CREATE OR REPLACE FUNCTION dbo.iti_lST_Stream() RETURNS trigger AS '
    BEGIN
        -- generate anchor ID (if not provided)
        IF (NEW.ST_ID IS NULL) THEN 
            INSERT INTO dbo.ST_Stream (
                Metadata_ST 
            ) VALUES (
                NEW.Metadata_ST 
            );
            SELECT
                lastval() 
            INTO NEW.ST_ID;
        -- if anchor ID is provided then let''s insert it into the anchor table
        -- but only if that ID is not present in the anchor table
        ELSE
            INSERT INTO dbo.ST_Stream (
                Metadata_ST,
                ST_ID
            )
            SELECT
                NEW.Metadata_ST,
                NEW.ST_ID
            WHERE NOT EXISTS(
	            SELECT
	                ST_ID 
	            FROM dbo.ST_Stream
	            WHERE ST_ID = NEW.ST_ID
	            LIMIT 1
            );
        END IF;
        -- insert row into temporary table
    	INSERT INTO _tmp_it_ST_Stream (
            ST_ID,
            Metadata_ST,
            ST_DEL_ST_ID,
            Metadata_ST_DEL,
            ST_DEL_Stream_DeletedAt,
            ST_CRE_ST_ID,
            Metadata_ST_CRE,
            ST_CRE_Stream_CreatedAt,
            ST_ENA_ST_ID,
            Metadata_ST_ENA,
            ST_ENA_ChangedAt,
            ST_ENA_Stream_EndAt,
            ST_STA_ST_ID,
            Metadata_ST_STA,
            ST_STA_ChangedAt,
            ST_STA_Stream_StartAt,
            ST_HOM_ST_ID,
            Metadata_ST_HOM,
            ST_HOM_ChangedAt,
            ST_HOM_Stream_HomeworkDeadlineDays,
            ST_OPE_ST_ID,
            Metadata_ST_OPE,
            ST_OPE_OPE_IsOpen,
            ST_OPE_Metadata_OPE,
            ST_OPE_OPE_ID,
            ST_NAM_ST_ID,
            Metadata_ST_NAM,
            ST_NAM_ChangedAt,
            ST_NAM_Stream_Name,
            ST_UPD_ST_ID,
            Metadata_ST_UPD,
            ST_UPD_ChangedAt,
            ST_UPD_Stream_UpdatedAt
    	) VALUES (
    	    NEW.ST_ID,
            NEW.Metadata_ST,
            COALESCE(NEW.ST_DEL_ST_ID, NEW.ST_ID),
            COALESCE(NEW.Metadata_ST_DEL, NEW.Metadata_ST),
            NEW.ST_DEL_Stream_DeletedAt,
            COALESCE(NEW.ST_CRE_ST_ID, NEW.ST_ID),
            COALESCE(NEW.Metadata_ST_CRE, NEW.Metadata_ST),
            NEW.ST_CRE_Stream_CreatedAt,
            COALESCE(NEW.ST_ENA_ST_ID, NEW.ST_ID),
            COALESCE(NEW.Metadata_ST_ENA, NEW.Metadata_ST),
            COALESCE(NEW.ST_ENA_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.ST_ENA_Stream_EndAt,
            COALESCE(NEW.ST_STA_ST_ID, NEW.ST_ID),
            COALESCE(NEW.Metadata_ST_STA, NEW.Metadata_ST),
            COALESCE(NEW.ST_STA_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.ST_STA_Stream_StartAt,
            COALESCE(NEW.ST_HOM_ST_ID, NEW.ST_ID),
            COALESCE(NEW.Metadata_ST_HOM, NEW.Metadata_ST),
            COALESCE(NEW.ST_HOM_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.ST_HOM_Stream_HomeworkDeadlineDays,
            COALESCE(NEW.ST_OPE_ST_ID, NEW.ST_ID),
            COALESCE(NEW.Metadata_ST_OPE, NEW.Metadata_ST),
            NEW.ST_OPE_OPE_IsOpen,
            COALESCE(NEW.ST_OPE_Metadata_OPE, NEW.Metadata_ST),
            NEW.ST_OPE_OPE_ID,
            COALESCE(NEW.ST_NAM_ST_ID, NEW.ST_ID),
            COALESCE(NEW.Metadata_ST_NAM, NEW.Metadata_ST),
            COALESCE(NEW.ST_NAM_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.ST_NAM_Stream_Name,
            COALESCE(NEW.ST_UPD_ST_ID, NEW.ST_ID),
            COALESCE(NEW.Metadata_ST_UPD, NEW.Metadata_ST),
            COALESCE(NEW.ST_UPD_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.ST_UPD_Stream_UpdatedAt
    	);
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER iti_lST_Stream
INSTEAD OF INSERT ON dbo.lST_Stream
FOR EACH ROW
EXECUTE PROCEDURE dbo.iti_lST_Stream();
-- AFTER INSERT trigger ---------------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS ita_lST_Stream ON dbo.lST_Stream;
--DROP FUNCTION IF EXISTS dbo.ita_lST_Stream();
CREATE OR REPLACE FUNCTION dbo.ita_lST_Stream() RETURNS trigger AS '
    BEGIN
        INSERT INTO dbo.ST_DEL_Stream_DeletedAt (
            ST_DEL_ST_ID,
            Metadata_ST_DEL,
            ST_DEL_Stream_DeletedAt
        )
        SELECT
            i.ST_DEL_ST_ID,
            i.Metadata_ST_DEL,
            i.ST_DEL_Stream_DeletedAt
        FROM
            _tmp_it_ST_Stream i
        WHERE
            i.ST_DEL_Stream_DeletedAt is not null;
        INSERT INTO dbo.ST_CRE_Stream_CreatedAt (
            ST_CRE_ST_ID,
            Metadata_ST_CRE,
            ST_CRE_Stream_CreatedAt
        )
        SELECT
            i.ST_CRE_ST_ID,
            i.Metadata_ST_CRE,
            i.ST_CRE_Stream_CreatedAt
        FROM
            _tmp_it_ST_Stream i
        WHERE
            i.ST_CRE_Stream_CreatedAt is not null;
        INSERT INTO dbo.ST_ENA_Stream_EndAt (
            ST_ENA_ST_ID,
            Metadata_ST_ENA,
            ST_ENA_ChangedAt,
            ST_ENA_Stream_EndAt
        )
        SELECT
            i.ST_ENA_ST_ID,
            i.Metadata_ST_ENA,
            i.ST_ENA_ChangedAt,
            i.ST_ENA_Stream_EndAt
        FROM
            _tmp_it_ST_Stream i
        WHERE
            i.ST_ENA_Stream_EndAt is not null;
        INSERT INTO dbo.ST_STA_Stream_StartAt (
            ST_STA_ST_ID,
            Metadata_ST_STA,
            ST_STA_ChangedAt,
            ST_STA_Stream_StartAt
        )
        SELECT
            i.ST_STA_ST_ID,
            i.Metadata_ST_STA,
            i.ST_STA_ChangedAt,
            i.ST_STA_Stream_StartAt
        FROM
            _tmp_it_ST_Stream i
        WHERE
            i.ST_STA_Stream_StartAt is not null;
        INSERT INTO dbo.ST_HOM_Stream_HomeworkDeadlineDays (
            ST_HOM_ST_ID,
            Metadata_ST_HOM,
            ST_HOM_ChangedAt,
            ST_HOM_Stream_HomeworkDeadlineDays
        )
        SELECT
            i.ST_HOM_ST_ID,
            i.Metadata_ST_HOM,
            i.ST_HOM_ChangedAt,
            i.ST_HOM_Stream_HomeworkDeadlineDays
        FROM
            _tmp_it_ST_Stream i
        WHERE
            i.ST_HOM_Stream_HomeworkDeadlineDays is not null;
        INSERT INTO dbo.ST_OPE_Stream_IsOpen (
            ST_OPE_ST_ID,
            Metadata_ST_OPE,
            ST_OPE_OPE_ID
        )
        SELECT
            i.ST_OPE_ST_ID,
            i.Metadata_ST_OPE,
            COALESCE(i.ST_OPE_OPE_ID, kOPE.OPE_ID) 
        FROM
            _tmp_it_ST_Stream i
        LEFT JOIN
            dbo.OPE_IsOpen kOPE
        ON
            kOPE.OPE_IsOpen = i.ST_OPE_OPE_IsOpen
        WHERE
            COALESCE(i.ST_OPE_OPE_ID, kOPE.OPE_ID) is not null;
        INSERT INTO dbo.ST_NAM_Stream_Name (
            ST_NAM_ST_ID,
            Metadata_ST_NAM,
            ST_NAM_ChangedAt,
            ST_NAM_Stream_Name
        )
        SELECT
            i.ST_NAM_ST_ID,
            i.Metadata_ST_NAM,
            i.ST_NAM_ChangedAt,
            i.ST_NAM_Stream_Name
        FROM
            _tmp_it_ST_Stream i
        WHERE
            i.ST_NAM_Stream_Name is not null;
        INSERT INTO dbo.ST_UPD_Stream_UpdatedAt (
            ST_UPD_ST_ID,
            Metadata_ST_UPD,
            ST_UPD_ChangedAt,
            ST_UPD_Stream_UpdatedAt
        )
        SELECT
            i.ST_UPD_ST_ID,
            i.Metadata_ST_UPD,
            i.ST_UPD_ChangedAt,
            i.ST_UPD_Stream_UpdatedAt
        FROM
            _tmp_it_ST_Stream i
        WHERE
            i.ST_UPD_Stream_UpdatedAt is not null;
        DROP TABLE IF EXISTS _tmp_it_ST_Stream;
        RETURN NULL;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER ita_lST_Stream
AFTER INSERT ON dbo.lST_Stream
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.ita_lST_Stream();
-- BEFORE INSERT trigger --------------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS itb_lMO_Module ON dbo.lMO_Module;
--DROP FUNCTION IF EXISTS dbo.itb_lMO_Module();
CREATE OR REPLACE FUNCTION dbo.itb_lMO_Module() RETURNS trigger AS '
    BEGIN
        -- create temporary table to keep inserted rows in
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_it_MO_Module (
            MO_ID int not null,
            Metadata_MO int not null,
            MO_UPD_MO_ID int null,
            Metadata_MO_UPD int null,
            MO_UPD_ChangedAt timestamp null,
            MO_UPD_Module_UpdatedAt timestamp null,
            MO_TIT_MO_ID int null,
            Metadata_MO_TIT int null,
            MO_TIT_ChangedAt timestamp null,
            MO_TIT_Module_Title varchar(255) null,
            MO_CRE_MO_ID int null,
            Metadata_MO_CRE int null,
            MO_CRE_Module_CreatedAt timestamp null,
            MO_ORD_MO_ID int null,
            Metadata_MO_ORD int null,
            MO_ORD_ChangedAt timestamp null,
            MO_ORD_Module_OrderInStream int null,
            MO_DEL_MO_ID int null,
            Metadata_MO_DEL int null,
            MO_DEL_Module_DeletedAt timestamp null
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER itb_lMO_Module
BEFORE INSERT ON dbo.lMO_Module
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.itb_lMO_Module(); 
-- INSTEAD OF INSERT trigger ----------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS iti_lMO_Module ON dbo.lMO_Module;
--DROP FUNCTION IF EXISTS dbo.iti_lMO_Module();
CREATE OR REPLACE FUNCTION dbo.iti_lMO_Module() RETURNS trigger AS '
    BEGIN
        -- generate anchor ID (if not provided)
        IF (NEW.MO_ID IS NULL) THEN 
            INSERT INTO dbo.MO_Module (
                Metadata_MO 
            ) VALUES (
                NEW.Metadata_MO 
            );
            SELECT
                lastval() 
            INTO NEW.MO_ID;
        -- if anchor ID is provided then let''s insert it into the anchor table
        -- but only if that ID is not present in the anchor table
        ELSE
            INSERT INTO dbo.MO_Module (
                Metadata_MO,
                MO_ID
            )
            SELECT
                NEW.Metadata_MO,
                NEW.MO_ID
            WHERE NOT EXISTS(
	            SELECT
	                MO_ID 
	            FROM dbo.MO_Module
	            WHERE MO_ID = NEW.MO_ID
	            LIMIT 1
            );
        END IF;
        -- insert row into temporary table
    	INSERT INTO _tmp_it_MO_Module (
            MO_ID,
            Metadata_MO,
            MO_UPD_MO_ID,
            Metadata_MO_UPD,
            MO_UPD_ChangedAt,
            MO_UPD_Module_UpdatedAt,
            MO_TIT_MO_ID,
            Metadata_MO_TIT,
            MO_TIT_ChangedAt,
            MO_TIT_Module_Title,
            MO_CRE_MO_ID,
            Metadata_MO_CRE,
            MO_CRE_Module_CreatedAt,
            MO_ORD_MO_ID,
            Metadata_MO_ORD,
            MO_ORD_ChangedAt,
            MO_ORD_Module_OrderInStream,
            MO_DEL_MO_ID,
            Metadata_MO_DEL,
            MO_DEL_Module_DeletedAt
    	) VALUES (
    	    NEW.MO_ID,
            NEW.Metadata_MO,
            COALESCE(NEW.MO_UPD_MO_ID, NEW.MO_ID),
            COALESCE(NEW.Metadata_MO_UPD, NEW.Metadata_MO),
            COALESCE(NEW.MO_UPD_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.MO_UPD_Module_UpdatedAt,
            COALESCE(NEW.MO_TIT_MO_ID, NEW.MO_ID),
            COALESCE(NEW.Metadata_MO_TIT, NEW.Metadata_MO),
            COALESCE(NEW.MO_TIT_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.MO_TIT_Module_Title,
            COALESCE(NEW.MO_CRE_MO_ID, NEW.MO_ID),
            COALESCE(NEW.Metadata_MO_CRE, NEW.Metadata_MO),
            NEW.MO_CRE_Module_CreatedAt,
            COALESCE(NEW.MO_ORD_MO_ID, NEW.MO_ID),
            COALESCE(NEW.Metadata_MO_ORD, NEW.Metadata_MO),
            COALESCE(NEW.MO_ORD_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.MO_ORD_Module_OrderInStream,
            COALESCE(NEW.MO_DEL_MO_ID, NEW.MO_ID),
            COALESCE(NEW.Metadata_MO_DEL, NEW.Metadata_MO),
            NEW.MO_DEL_Module_DeletedAt
    	);
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER iti_lMO_Module
INSTEAD OF INSERT ON dbo.lMO_Module
FOR EACH ROW
EXECUTE PROCEDURE dbo.iti_lMO_Module();
-- AFTER INSERT trigger ---------------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS ita_lMO_Module ON dbo.lMO_Module;
--DROP FUNCTION IF EXISTS dbo.ita_lMO_Module();
CREATE OR REPLACE FUNCTION dbo.ita_lMO_Module() RETURNS trigger AS '
    BEGIN
        INSERT INTO dbo.MO_UPD_Module_UpdatedAt (
            MO_UPD_MO_ID,
            Metadata_MO_UPD,
            MO_UPD_ChangedAt,
            MO_UPD_Module_UpdatedAt
        )
        SELECT
            i.MO_UPD_MO_ID,
            i.Metadata_MO_UPD,
            i.MO_UPD_ChangedAt,
            i.MO_UPD_Module_UpdatedAt
        FROM
            _tmp_it_MO_Module i
        WHERE
            i.MO_UPD_Module_UpdatedAt is not null;
        INSERT INTO dbo.MO_TIT_Module_Title (
            MO_TIT_MO_ID,
            Metadata_MO_TIT,
            MO_TIT_ChangedAt,
            MO_TIT_Module_Title
        )
        SELECT
            i.MO_TIT_MO_ID,
            i.Metadata_MO_TIT,
            i.MO_TIT_ChangedAt,
            i.MO_TIT_Module_Title
        FROM
            _tmp_it_MO_Module i
        WHERE
            i.MO_TIT_Module_Title is not null;
        INSERT INTO dbo.MO_CRE_Module_CreatedAt (
            MO_CRE_MO_ID,
            Metadata_MO_CRE,
            MO_CRE_Module_CreatedAt
        )
        SELECT
            i.MO_CRE_MO_ID,
            i.Metadata_MO_CRE,
            i.MO_CRE_Module_CreatedAt
        FROM
            _tmp_it_MO_Module i
        WHERE
            i.MO_CRE_Module_CreatedAt is not null;
        INSERT INTO dbo.MO_ORD_Module_OrderInStream (
            MO_ORD_MO_ID,
            Metadata_MO_ORD,
            MO_ORD_ChangedAt,
            MO_ORD_Module_OrderInStream
        )
        SELECT
            i.MO_ORD_MO_ID,
            i.Metadata_MO_ORD,
            i.MO_ORD_ChangedAt,
            i.MO_ORD_Module_OrderInStream
        FROM
            _tmp_it_MO_Module i
        WHERE
            i.MO_ORD_Module_OrderInStream is not null;
        INSERT INTO dbo.MO_DEL_Module_DeletedAt (
            MO_DEL_MO_ID,
            Metadata_MO_DEL,
            MO_DEL_Module_DeletedAt
        )
        SELECT
            i.MO_DEL_MO_ID,
            i.Metadata_MO_DEL,
            i.MO_DEL_Module_DeletedAt
        FROM
            _tmp_it_MO_Module i
        WHERE
            i.MO_DEL_Module_DeletedAt is not null;
        DROP TABLE IF EXISTS _tmp_it_MO_Module;
        RETURN NULL;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER ita_lMO_Module
AFTER INSERT ON dbo.lMO_Module
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.ita_lMO_Module();
-- BEFORE INSERT trigger --------------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS itb_lLE_Lesson ON dbo.lLE_Lesson;
--DROP FUNCTION IF EXISTS dbo.itb_lLE_Lesson();
CREATE OR REPLACE FUNCTION dbo.itb_lLE_Lesson() RETURNS trigger AS '
    BEGIN
        -- create temporary table to keep inserted rows in
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_it_LE_Lesson (
            LE_ID int not null,
            Metadata_LE int not null,
            LE_DEL_LE_ID int null,
            Metadata_LE_DEL int null,
            LE_DEL_Lesson_DeletedAt timestamp(0) null,
            LE_JOI_LE_ID int null,
            Metadata_LE_JOI int null,
            LE_JOI_ChangedAt timestamp null,
            LE_JOI_Lesson_OnlineLessonJoinUrl varchar(255) null,
            LE_REC_LE_ID int null,
            Metadata_LE_REC int null,
            LE_REC_ChangedAt timestamp null,
            LE_REC_Lesson_OnlineLessonRecordingUrl varchar(255) null,
            LE_TIT_LE_ID int null,
            Metadata_LE_TIT int null,
            LE_TIT_ChangedAt timestamp null,
            LE_TIT_Lesson_Title varchar(255) null,
            LE_EDT_LE_ID int null,
            Metadata_LE_EDT int null,
            LE_EDT_ChangedAt timestamp null,
            LE_EDT_Lesson_EndAt timestamp null,
            LE_HOM_LE_ID int null,
            Metadata_LE_HOM int null,
            LE_HOM_ChangedAt timestamp null,
            LE_HOM_Lesson_HomeWorkUrl varchar(500) null,
            LE_DES_LE_ID int null,
            Metadata_LE_DES int null,
            LE_DES_ChangedAt timestamp null,
            LE_DES_Lesson_Description text null,
            LE_STA_LE_ID int null,
            Metadata_LE_STA int null,
            LE_STA_ChangedAt timestamp null,
            LE_STA_Lesson_StartAt timestamp null
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER itb_lLE_Lesson
BEFORE INSERT ON dbo.lLE_Lesson
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.itb_lLE_Lesson(); 
-- INSTEAD OF INSERT trigger ----------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS iti_lLE_Lesson ON dbo.lLE_Lesson;
--DROP FUNCTION IF EXISTS dbo.iti_lLE_Lesson();
CREATE OR REPLACE FUNCTION dbo.iti_lLE_Lesson() RETURNS trigger AS '
    BEGIN
        -- generate anchor ID (if not provided)
        IF (NEW.LE_ID IS NULL) THEN 
            INSERT INTO dbo.LE_Lesson (
                Metadata_LE 
            ) VALUES (
                NEW.Metadata_LE 
            );
            SELECT
                lastval() 
            INTO NEW.LE_ID;
        -- if anchor ID is provided then let''s insert it into the anchor table
        -- but only if that ID is not present in the anchor table
        ELSE
            INSERT INTO dbo.LE_Lesson (
                Metadata_LE,
                LE_ID
            )
            SELECT
                NEW.Metadata_LE,
                NEW.LE_ID
            WHERE NOT EXISTS(
	            SELECT
	                LE_ID 
	            FROM dbo.LE_Lesson
	            WHERE LE_ID = NEW.LE_ID
	            LIMIT 1
            );
        END IF;
        -- insert row into temporary table
    	INSERT INTO _tmp_it_LE_Lesson (
            LE_ID,
            Metadata_LE,
            LE_DEL_LE_ID,
            Metadata_LE_DEL,
            LE_DEL_Lesson_DeletedAt,
            LE_JOI_LE_ID,
            Metadata_LE_JOI,
            LE_JOI_ChangedAt,
            LE_JOI_Lesson_OnlineLessonJoinUrl,
            LE_REC_LE_ID,
            Metadata_LE_REC,
            LE_REC_ChangedAt,
            LE_REC_Lesson_OnlineLessonRecordingUrl,
            LE_TIT_LE_ID,
            Metadata_LE_TIT,
            LE_TIT_ChangedAt,
            LE_TIT_Lesson_Title,
            LE_EDT_LE_ID,
            Metadata_LE_EDT,
            LE_EDT_ChangedAt,
            LE_EDT_Lesson_EndAt,
            LE_HOM_LE_ID,
            Metadata_LE_HOM,
            LE_HOM_ChangedAt,
            LE_HOM_Lesson_HomeWorkUrl,
            LE_DES_LE_ID,
            Metadata_LE_DES,
            LE_DES_ChangedAt,
            LE_DES_Lesson_Description,
            LE_STA_LE_ID,
            Metadata_LE_STA,
            LE_STA_ChangedAt,
            LE_STA_Lesson_StartAt
    	) VALUES (
    	    NEW.LE_ID,
            NEW.Metadata_LE,
            COALESCE(NEW.LE_DEL_LE_ID, NEW.LE_ID),
            COALESCE(NEW.Metadata_LE_DEL, NEW.Metadata_LE),
            NEW.LE_DEL_Lesson_DeletedAt,
            COALESCE(NEW.LE_JOI_LE_ID, NEW.LE_ID),
            COALESCE(NEW.Metadata_LE_JOI, NEW.Metadata_LE),
            COALESCE(NEW.LE_JOI_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.LE_JOI_Lesson_OnlineLessonJoinUrl,
            COALESCE(NEW.LE_REC_LE_ID, NEW.LE_ID),
            COALESCE(NEW.Metadata_LE_REC, NEW.Metadata_LE),
            COALESCE(NEW.LE_REC_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.LE_REC_Lesson_OnlineLessonRecordingUrl,
            COALESCE(NEW.LE_TIT_LE_ID, NEW.LE_ID),
            COALESCE(NEW.Metadata_LE_TIT, NEW.Metadata_LE),
            COALESCE(NEW.LE_TIT_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.LE_TIT_Lesson_Title,
            COALESCE(NEW.LE_EDT_LE_ID, NEW.LE_ID),
            COALESCE(NEW.Metadata_LE_EDT, NEW.Metadata_LE),
            COALESCE(NEW.LE_EDT_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.LE_EDT_Lesson_EndAt,
            COALESCE(NEW.LE_HOM_LE_ID, NEW.LE_ID),
            COALESCE(NEW.Metadata_LE_HOM, NEW.Metadata_LE),
            COALESCE(NEW.LE_HOM_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.LE_HOM_Lesson_HomeWorkUrl,
            COALESCE(NEW.LE_DES_LE_ID, NEW.LE_ID),
            COALESCE(NEW.Metadata_LE_DES, NEW.Metadata_LE),
            COALESCE(NEW.LE_DES_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.LE_DES_Lesson_Description,
            COALESCE(NEW.LE_STA_LE_ID, NEW.LE_ID),
            COALESCE(NEW.Metadata_LE_STA, NEW.Metadata_LE),
            COALESCE(NEW.LE_STA_ChangedAt, CAST(LOCALTIMESTAMP AS timestamp)),
            NEW.LE_STA_Lesson_StartAt
    	);
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER iti_lLE_Lesson
INSTEAD OF INSERT ON dbo.lLE_Lesson
FOR EACH ROW
EXECUTE PROCEDURE dbo.iti_lLE_Lesson();
-- AFTER INSERT trigger ---------------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS ita_lLE_Lesson ON dbo.lLE_Lesson;
--DROP FUNCTION IF EXISTS dbo.ita_lLE_Lesson();
CREATE OR REPLACE FUNCTION dbo.ita_lLE_Lesson() RETURNS trigger AS '
    BEGIN
        INSERT INTO dbo.LE_DEL_Lesson_DeletedAt (
            LE_DEL_LE_ID,
            Metadata_LE_DEL,
            LE_DEL_Lesson_DeletedAt
        )
        SELECT
            i.LE_DEL_LE_ID,
            i.Metadata_LE_DEL,
            i.LE_DEL_Lesson_DeletedAt
        FROM
            _tmp_it_LE_Lesson i
        WHERE
            i.LE_DEL_Lesson_DeletedAt is not null;
        INSERT INTO dbo.LE_JOI_Lesson_OnlineLessonJoinUrl (
            LE_JOI_LE_ID,
            Metadata_LE_JOI,
            LE_JOI_ChangedAt,
            LE_JOI_Lesson_OnlineLessonJoinUrl
        )
        SELECT
            i.LE_JOI_LE_ID,
            i.Metadata_LE_JOI,
            i.LE_JOI_ChangedAt,
            i.LE_JOI_Lesson_OnlineLessonJoinUrl
        FROM
            _tmp_it_LE_Lesson i
        WHERE
            i.LE_JOI_Lesson_OnlineLessonJoinUrl is not null;
        INSERT INTO dbo.LE_REC_Lesson_OnlineLessonRecordingUrl (
            LE_REC_LE_ID,
            Metadata_LE_REC,
            LE_REC_ChangedAt,
            LE_REC_Lesson_OnlineLessonRecordingUrl
        )
        SELECT
            i.LE_REC_LE_ID,
            i.Metadata_LE_REC,
            i.LE_REC_ChangedAt,
            i.LE_REC_Lesson_OnlineLessonRecordingUrl
        FROM
            _tmp_it_LE_Lesson i
        WHERE
            i.LE_REC_Lesson_OnlineLessonRecordingUrl is not null;
        INSERT INTO dbo.LE_TIT_Lesson_Title (
            LE_TIT_LE_ID,
            Metadata_LE_TIT,
            LE_TIT_ChangedAt,
            LE_TIT_Lesson_Title
        )
        SELECT
            i.LE_TIT_LE_ID,
            i.Metadata_LE_TIT,
            i.LE_TIT_ChangedAt,
            i.LE_TIT_Lesson_Title
        FROM
            _tmp_it_LE_Lesson i
        WHERE
            i.LE_TIT_Lesson_Title is not null;
        INSERT INTO dbo.LE_EDT_Lesson_EndAt (
            LE_EDT_LE_ID,
            Metadata_LE_EDT,
            LE_EDT_ChangedAt,
            LE_EDT_Lesson_EndAt
        )
        SELECT
            i.LE_EDT_LE_ID,
            i.Metadata_LE_EDT,
            i.LE_EDT_ChangedAt,
            i.LE_EDT_Lesson_EndAt
        FROM
            _tmp_it_LE_Lesson i
        WHERE
            i.LE_EDT_Lesson_EndAt is not null;
        INSERT INTO dbo.LE_HOM_Lesson_HomeWorkUrl (
            LE_HOM_LE_ID,
            Metadata_LE_HOM,
            LE_HOM_ChangedAt,
            LE_HOM_Lesson_HomeWorkUrl
        )
        SELECT
            i.LE_HOM_LE_ID,
            i.Metadata_LE_HOM,
            i.LE_HOM_ChangedAt,
            i.LE_HOM_Lesson_HomeWorkUrl
        FROM
            _tmp_it_LE_Lesson i
        WHERE
            i.LE_HOM_Lesson_HomeWorkUrl is not null;
        INSERT INTO dbo.LE_DES_Lesson_Description (
            LE_DES_LE_ID,
            Metadata_LE_DES,
            LE_DES_ChangedAt,
            LE_DES_Lesson_Description
        )
        SELECT
            i.LE_DES_LE_ID,
            i.Metadata_LE_DES,
            i.LE_DES_ChangedAt,
            i.LE_DES_Lesson_Description
        FROM
            _tmp_it_LE_Lesson i
        WHERE
            i.LE_DES_Lesson_Description is not null;
        INSERT INTO dbo.LE_STA_Lesson_StartAt (
            LE_STA_LE_ID,
            Metadata_LE_STA,
            LE_STA_ChangedAt,
            LE_STA_Lesson_StartAt
        )
        SELECT
            i.LE_STA_LE_ID,
            i.Metadata_LE_STA,
            i.LE_STA_ChangedAt,
            i.LE_STA_Lesson_StartAt
        FROM
            _tmp_it_LE_Lesson i
        WHERE
            i.LE_STA_Lesson_StartAt is not null;
        DROP TABLE IF EXISTS _tmp_it_LE_Lesson;
        RETURN NULL;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER ita_lLE_Lesson
AFTER INSERT ON dbo.lLE_Lesson
FOR EACH STATEMENT
EXECUTE PROCEDURE dbo.ita_lLE_Lesson();
-- TIE RESTATEMENT CONSTRAINTS ----------------------------------------------------------------------------------------
--
-- Ties may be prevented from storing restatements.
-- A restatement is when the same (non-key) values occurs for two adjacent points
-- in changing time.
--
-- returns 1 for one or two equal surrounding values, 0 for different surrounding values
--
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfCO_learned_ST_on restatement finder
--
-- CO_ID_learned non-key value
-- ST_ID_on primary key component 
-- changed the point in time from which this value shall represent a change
--
-- rcCO_learned_ST_on restatement constraint (available only in ties that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.rfCO_learned_ST_on (
    _CO_ID_learned int, 
    _ST_ID_on int, 
    changed timestamp
) RETURNS smallint AS '
    BEGIN RETURN (
        SELECT
            COUNT(*)
        FROM (
            (SELECT
                pre.CO_ID_learned
            FROM
                dbo.CO_learned_ST_on pre
            WHERE
                pre.ST_ID_on = _ST_ID_on
            AND
                pre.CO_learned_ST_on_ChangedAt < changed
            ORDER BY
                pre.CO_learned_ST_on_ChangedAt DESC
            LIMIT 1)
            UNION
            (SELECT
                fol.CO_ID_learned
            FROM
                dbo.CO_learned_ST_on fol
            WHERE
                fol.ST_ID_on = _ST_ID_on
            AND
                fol.CO_learned_ST_on_ChangedAt > changed
            ORDER BY
                fol.CO_learned_ST_on_ChangedAt ASC
            LIMIT 1)
        ) s
        WHERE
            s.CO_ID_learned = _CO_ID_learned
    );
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfST_consists_MO_of restatement finder
--
-- ST_ID_consists non-key value
-- MO_ID_of primary key component 
-- changed the point in time from which this value shall represent a change
--
-- rcST_consists_MO_of restatement constraint (available only in ties that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.rfST_consists_MO_of (
    _ST_ID_consists int, 
    _MO_ID_of int, 
    changed timestamp
) RETURNS smallint AS '
    BEGIN RETURN (
        SELECT
            COUNT(*)
        FROM (
            (SELECT
                pre.ST_ID_consists
            FROM
                dbo.ST_consists_MO_of pre
            WHERE
                pre.MO_ID_of = _MO_ID_of
            AND
                pre.ST_consists_MO_of_ChangedAt < changed
            ORDER BY
                pre.ST_consists_MO_of_ChangedAt DESC
            LIMIT 1)
            UNION
            (SELECT
                fol.ST_ID_consists
            FROM
                dbo.ST_consists_MO_of fol
            WHERE
                fol.MO_ID_of = _MO_ID_of
            AND
                fol.ST_consists_MO_of_ChangedAt > changed
            ORDER BY
                fol.ST_consists_MO_of_ChangedAt ASC
            LIMIT 1)
        ) s
        WHERE
            s.ST_ID_consists = _ST_ID_consists
    );
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfMO_consist_LE_of restatement finder
--
-- MO_ID_consist non-key value
-- LE_ID_of primary key component 
-- changed the point in time from which this value shall represent a change
--
-- rcMO_consist_LE_of restatement constraint (available only in ties that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.rfMO_consist_LE_of (
    _MO_ID_consist int, 
    _LE_ID_of int, 
    changed timestamp
) RETURNS smallint AS '
    BEGIN RETURN (
        SELECT
            COUNT(*)
        FROM (
            (SELECT
                pre.MO_ID_consist
            FROM
                dbo.MO_consist_LE_of pre
            WHERE
                pre.LE_ID_of = _LE_ID_of
            AND
                pre.MO_consist_LE_of_ChangedAt < changed
            ORDER BY
                pre.MO_consist_LE_of_ChangedAt DESC
            LIMIT 1)
            UNION
            (SELECT
                fol.MO_ID_consist
            FROM
                dbo.MO_consist_LE_of fol
            WHERE
                fol.LE_ID_of = _LE_ID_of
            AND
                fol.MO_consist_LE_of_ChangedAt > changed
            ORDER BY
                fol.MO_consist_LE_of_ChangedAt ASC
            LIMIT 1)
        ) s
        WHERE
            s.MO_ID_consist = _MO_ID_consist
    );
    END;
' LANGUAGE plpgsql;
-- Restatement Finder Function and Constraint -------------------------------------------------------------------------
-- rfLE_tought_TE_by restatement finder
--
-- LE_ID_tought primary key component 
-- TE_ID_by non-key value
-- changed the point in time from which this value shall represent a change
--
-- rcLE_tought_TE_by restatement constraint (available only in ties that cannot have restatements)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.rfLE_tought_TE_by (
    _LE_ID_tought int, 
    _TE_ID_by int, 
    changed timestamp
) RETURNS smallint AS '
    BEGIN RETURN (
        SELECT
            COUNT(*)
        FROM (
            (SELECT
                pre.TE_ID_by
            FROM
                dbo.LE_tought_TE_by pre
            WHERE
                pre.LE_ID_tought = _LE_ID_tought
            AND
                pre.LE_tought_TE_by_ChangedAt < changed
            ORDER BY
                pre.LE_tought_TE_by_ChangedAt DESC
            LIMIT 1)
            UNION
            (SELECT
                fol.TE_ID_by
            FROM
                dbo.LE_tought_TE_by fol
            WHERE
                fol.LE_ID_tought = _LE_ID_tought
            AND
                fol.LE_tought_TE_by_ChangedAt > changed
            ORDER BY
                fol.LE_tought_TE_by_ChangedAt ASC
            LIMIT 1)
        ) s
        WHERE
            s.TE_ID_by = _TE_ID_by
    );
    END;
' LANGUAGE plpgsql;
-- TIE TEMPORAL PERSPECTIVES ------------------------------------------------------------------------------------------
--
-- These table valued functions simplify temporal querying by providing a temporal
-- perspective of each tie. There are four types of perspectives: latest,
-- point-in-time, difference, and now.
--
-- The latest perspective shows the latest available information for each tie.
-- The now perspective shows the information as it is right now.
-- The point-in-time perspective lets you travel through the information to the given timepoint.
--
-- changingTimepoint the point in changing time to travel to
--
-- The difference perspective shows changes between the two given timepoints.
--
-- intervalStart the start of the interval for finding changes
-- intervalEnd the end of the interval for finding changes
--
-- Under equivalence all these views default to equivalent = 0, however, corresponding
-- prepended-e perspectives are provided in order to select a specific equivalent.
--
-- equivalent the equivalent for which to retrieve data
--
-- DROP TIE TEMPORAL PERSPECTIVES ----------------------------------------------------------------------------------
/*
DROP FUNCTION IF EXISTS dbo.dCO_learned_ST_on(
    timestamp without time zone,
    timestamp without time zone
);
DROP VIEW IF EXISTS dbo.nCO_learned_ST_on;
DROP FUNCTION IF EXISTS dbo.pCO_learned_ST_on(
    timestamp without time zone
);
DROP VIEW IF EXISTS dbo.lCO_learned_ST_on;
DROP FUNCTION IF EXISTS dbo.dST_consists_MO_of(
    timestamp without time zone,
    timestamp without time zone
);
DROP VIEW IF EXISTS dbo.nST_consists_MO_of;
DROP FUNCTION IF EXISTS dbo.pST_consists_MO_of(
    timestamp without time zone
);
DROP VIEW IF EXISTS dbo.lST_consists_MO_of;
DROP FUNCTION IF EXISTS dbo.dMO_consist_LE_of(
    timestamp without time zone,
    timestamp without time zone
);
DROP VIEW IF EXISTS dbo.nMO_consist_LE_of;
DROP FUNCTION IF EXISTS dbo.pMO_consist_LE_of(
    timestamp without time zone
);
DROP VIEW IF EXISTS dbo.lMO_consist_LE_of;
DROP FUNCTION IF EXISTS dbo.dLE_tought_TE_by(
    timestamp without time zone,
    timestamp without time zone
);
DROP VIEW IF EXISTS dbo.nLE_tought_TE_by;
DROP FUNCTION IF EXISTS dbo.pLE_tought_TE_by(
    timestamp without time zone
);
DROP VIEW IF EXISTS dbo.lLE_tought_TE_by;
*/
-- Latest perspective -------------------------------------------------------------------------------------------------
-- lCO_learned_ST_on viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.lCO_learned_ST_on AS
SELECT
    tie.Metadata_CO_learned_ST_on,
    tie.CO_learned_ST_on_ChangedAt,
    tie.CO_ID_learned,
    tie.ST_ID_on
FROM
    dbo.CO_learned_ST_on tie
WHERE
    tie.CO_learned_ST_on_ChangedAt = (
        SELECT
            max(sub.CO_learned_ST_on_ChangedAt)
        FROM
            dbo.CO_learned_ST_on sub
        WHERE
            sub.ST_ID_on = tie.ST_ID_on
   );
-- Latest perspective -------------------------------------------------------------------------------------------------
-- lST_consists_MO_of viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.lST_consists_MO_of AS
SELECT
    tie.Metadata_ST_consists_MO_of,
    tie.ST_consists_MO_of_ChangedAt,
    tie.ST_ID_consists,
    tie.MO_ID_of
FROM
    dbo.ST_consists_MO_of tie
WHERE
    tie.ST_consists_MO_of_ChangedAt = (
        SELECT
            max(sub.ST_consists_MO_of_ChangedAt)
        FROM
            dbo.ST_consists_MO_of sub
        WHERE
            sub.MO_ID_of = tie.MO_ID_of
   );
-- Latest perspective -------------------------------------------------------------------------------------------------
-- lMO_consist_LE_of viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.lMO_consist_LE_of AS
SELECT
    tie.Metadata_MO_consist_LE_of,
    tie.MO_consist_LE_of_ChangedAt,
    tie.MO_ID_consist,
    tie.LE_ID_of
FROM
    dbo.MO_consist_LE_of tie
WHERE
    tie.MO_consist_LE_of_ChangedAt = (
        SELECT
            max(sub.MO_consist_LE_of_ChangedAt)
        FROM
            dbo.MO_consist_LE_of sub
        WHERE
            sub.LE_ID_of = tie.LE_ID_of
   );
-- Latest perspective -------------------------------------------------------------------------------------------------
-- lLE_tought_TE_by viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.lLE_tought_TE_by AS
SELECT
    tie.Metadata_LE_tought_TE_by,
    tie.LE_tought_TE_by_ChangedAt,
    tie.LE_ID_tought,
    tie.TE_ID_by
FROM
    dbo.LE_tought_TE_by tie
WHERE
    tie.LE_tought_TE_by_ChangedAt = (
        SELECT
            max(sub.LE_tought_TE_by_ChangedAt)
        FROM
            dbo.LE_tought_TE_by sub
        WHERE
            sub.LE_ID_tought = tie.LE_ID_tought
   );
-- Point-in-time perspective ------------------------------------------------------------------------------------------
-- pCO_learned_ST_on viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.pCO_learned_ST_on (
    changingTimepoint timestamp without time zone
)
RETURNS TABLE (
    Metadata_CO_learned_ST_on int,
    CO_learned_ST_on_ChangedAt timestamp,
    CO_ID_learned int,
    ST_ID_on int
) AS '
SELECT
    tie.Metadata_CO_learned_ST_on,
    tie.CO_learned_ST_on_ChangedAt,
    tie.CO_ID_learned,
    tie.ST_ID_on
FROM
    dbo.CO_learned_ST_on tie
WHERE
    tie.CO_learned_ST_on_ChangedAt = (
        SELECT
            max(sub.CO_learned_ST_on_ChangedAt)
        FROM
            dbo.CO_learned_ST_on sub
        WHERE
            sub.ST_ID_on = tie.ST_ID_on
        AND
            sub.CO_learned_ST_on_ChangedAt <= changingTimepoint
   );
' LANGUAGE SQL;
-- Point-in-time perspective ------------------------------------------------------------------------------------------
-- pST_consists_MO_of viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.pST_consists_MO_of (
    changingTimepoint timestamp without time zone
)
RETURNS TABLE (
    Metadata_ST_consists_MO_of int,
    ST_consists_MO_of_ChangedAt timestamp,
    ST_ID_consists int,
    MO_ID_of int
) AS '
SELECT
    tie.Metadata_ST_consists_MO_of,
    tie.ST_consists_MO_of_ChangedAt,
    tie.ST_ID_consists,
    tie.MO_ID_of
FROM
    dbo.ST_consists_MO_of tie
WHERE
    tie.ST_consists_MO_of_ChangedAt = (
        SELECT
            max(sub.ST_consists_MO_of_ChangedAt)
        FROM
            dbo.ST_consists_MO_of sub
        WHERE
            sub.MO_ID_of = tie.MO_ID_of
        AND
            sub.ST_consists_MO_of_ChangedAt <= changingTimepoint
   );
' LANGUAGE SQL;
-- Point-in-time perspective ------------------------------------------------------------------------------------------
-- pMO_consist_LE_of viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.pMO_consist_LE_of (
    changingTimepoint timestamp without time zone
)
RETURNS TABLE (
    Metadata_MO_consist_LE_of int,
    MO_consist_LE_of_ChangedAt timestamp,
    MO_ID_consist int,
    LE_ID_of int
) AS '
SELECT
    tie.Metadata_MO_consist_LE_of,
    tie.MO_consist_LE_of_ChangedAt,
    tie.MO_ID_consist,
    tie.LE_ID_of
FROM
    dbo.MO_consist_LE_of tie
WHERE
    tie.MO_consist_LE_of_ChangedAt = (
        SELECT
            max(sub.MO_consist_LE_of_ChangedAt)
        FROM
            dbo.MO_consist_LE_of sub
        WHERE
            sub.LE_ID_of = tie.LE_ID_of
        AND
            sub.MO_consist_LE_of_ChangedAt <= changingTimepoint
   );
' LANGUAGE SQL;
-- Point-in-time perspective ------------------------------------------------------------------------------------------
-- pLE_tought_TE_by viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.pLE_tought_TE_by (
    changingTimepoint timestamp without time zone
)
RETURNS TABLE (
    Metadata_LE_tought_TE_by int,
    LE_tought_TE_by_ChangedAt timestamp,
    LE_ID_tought int,
    TE_ID_by int
) AS '
SELECT
    tie.Metadata_LE_tought_TE_by,
    tie.LE_tought_TE_by_ChangedAt,
    tie.LE_ID_tought,
    tie.TE_ID_by
FROM
    dbo.LE_tought_TE_by tie
WHERE
    tie.LE_tought_TE_by_ChangedAt = (
        SELECT
            max(sub.LE_tought_TE_by_ChangedAt)
        FROM
            dbo.LE_tought_TE_by sub
        WHERE
            sub.LE_ID_tought = tie.LE_ID_tought
        AND
            sub.LE_tought_TE_by_ChangedAt <= changingTimepoint
   );
' LANGUAGE SQL;
-- Now perspective ----------------------------------------------------------------------------------------------------
-- nCO_learned_ST_on viewed as it currently is (cannot include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.nCO_learned_ST_on AS
SELECT
    *
FROM
    dbo.pCO_learned_ST_on(LOCALTIMESTAMP);
-- Now perspective ----------------------------------------------------------------------------------------------------
-- nST_consists_MO_of viewed as it currently is (cannot include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.nST_consists_MO_of AS
SELECT
    *
FROM
    dbo.pST_consists_MO_of(LOCALTIMESTAMP);
-- Now perspective ----------------------------------------------------------------------------------------------------
-- nMO_consist_LE_of viewed as it currently is (cannot include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.nMO_consist_LE_of AS
SELECT
    *
FROM
    dbo.pMO_consist_LE_of(LOCALTIMESTAMP);
-- Now perspective ----------------------------------------------------------------------------------------------------
-- nLE_tought_TE_by viewed as it currently is (cannot include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.nLE_tought_TE_by AS
SELECT
    *
FROM
    dbo.pLE_tought_TE_by(LOCALTIMESTAMP);
-- Difference perspective ---------------------------------------------------------------------------------------------
-- dCO_learned_ST_on showing all differences between the given timepoints
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.dCO_learned_ST_on (
    intervalStart timestamp without time zone,
    intervalEnd timestamp without time zone
)
RETURNS TABLE (
    Metadata_CO_learned_ST_on int,
    CO_learned_ST_on_ChangedAt timestamp,
    CO_ID_learned int,
    ST_ID_on int
) AS '
SELECT
    tie.Metadata_CO_learned_ST_on,
    tie.CO_learned_ST_on_ChangedAt,
    tie.CO_ID_learned,
    tie.ST_ID_on
FROM
    dbo.CO_learned_ST_on tie
WHERE
    tie.CO_learned_ST_on_ChangedAt BETWEEN intervalStart AND intervalEnd;
' LANGUAGE SQL;
-- Difference perspective ---------------------------------------------------------------------------------------------
-- dST_consists_MO_of showing all differences between the given timepoints
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.dST_consists_MO_of (
    intervalStart timestamp without time zone,
    intervalEnd timestamp without time zone
)
RETURNS TABLE (
    Metadata_ST_consists_MO_of int,
    ST_consists_MO_of_ChangedAt timestamp,
    ST_ID_consists int,
    MO_ID_of int
) AS '
SELECT
    tie.Metadata_ST_consists_MO_of,
    tie.ST_consists_MO_of_ChangedAt,
    tie.ST_ID_consists,
    tie.MO_ID_of
FROM
    dbo.ST_consists_MO_of tie
WHERE
    tie.ST_consists_MO_of_ChangedAt BETWEEN intervalStart AND intervalEnd;
' LANGUAGE SQL;
-- Difference perspective ---------------------------------------------------------------------------------------------
-- dMO_consist_LE_of showing all differences between the given timepoints
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.dMO_consist_LE_of (
    intervalStart timestamp without time zone,
    intervalEnd timestamp without time zone
)
RETURNS TABLE (
    Metadata_MO_consist_LE_of int,
    MO_consist_LE_of_ChangedAt timestamp,
    MO_ID_consist int,
    LE_ID_of int
) AS '
SELECT
    tie.Metadata_MO_consist_LE_of,
    tie.MO_consist_LE_of_ChangedAt,
    tie.MO_ID_consist,
    tie.LE_ID_of
FROM
    dbo.MO_consist_LE_of tie
WHERE
    tie.MO_consist_LE_of_ChangedAt BETWEEN intervalStart AND intervalEnd;
' LANGUAGE SQL;
-- Difference perspective ---------------------------------------------------------------------------------------------
-- dLE_tought_TE_by showing all differences between the given timepoints
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.dLE_tought_TE_by (
    intervalStart timestamp without time zone,
    intervalEnd timestamp without time zone
)
RETURNS TABLE (
    Metadata_LE_tought_TE_by int,
    LE_tought_TE_by_ChangedAt timestamp,
    LE_ID_tought int,
    TE_ID_by int
) AS '
SELECT
    tie.Metadata_LE_tought_TE_by,
    tie.LE_tought_TE_by_ChangedAt,
    tie.LE_ID_tought,
    tie.TE_ID_by
FROM
    dbo.LE_tought_TE_by tie
WHERE
    tie.LE_tought_TE_by_ChangedAt BETWEEN intervalStart AND intervalEnd;
' LANGUAGE SQL;
-- DESCRIPTIONS -------------------------------------------------------------------------------------------------------
