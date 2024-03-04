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
-- OB_Object table (with 1 attributes)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS tes._OB_Object;
CREATE TABLE IF NOT EXISTS tes._OB_Object (
    OB_ID serial not null, 
    Metadata_OB int not null, 
    constraint pkOB_Object primary key (
        OB_ID
    )
);
ALTER TABLE IF EXISTS ONLY tes._OB_Object CLUSTER ON pkOB_Object;
-- DROP VIEW IF EXISTS tes.OB_Object;
CREATE OR REPLACE VIEW tes.OB_Object AS SELECT 
    OB_ID,
    Metadata_OB 
FROM tes._OB_Object;
-- Static attribute table ---------------------------------------------------------------------------------------------
-- OB_ATR_Object_ObjectAttribute table (on OB_Object)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS tes._OB_ATR_Object_ObjectAttribute;
CREATE TABLE IF NOT EXISTS tes._OB_ATR_Object_ObjectAttribute (
    OB_ATR_OB_ID int not null,
    OB_ATR_Object_ObjectAttribute varchar(255) not null,
    Metadata_OB_ATR int not null,
    constraint fkOB_ATR_Object_ObjectAttribute foreign key (
        OB_ATR_OB_ID
    ) references tes._OB_Object(OB_ID),
    constraint pkOB_ATR_Object_ObjectAttribute primary key (
        OB_ATR_OB_ID
    )
);
ALTER TABLE IF EXISTS ONLY tes._OB_ATR_Object_ObjectAttribute CLUSTER ON pkOB_ATR_Object_ObjectAttribute;
-- DROP VIEW IF EXISTS tes.OB_ATR_Object_ObjectAttribute;
CREATE OR REPLACE VIEW tes.OB_ATR_Object_ObjectAttribute AS SELECT
    OB_ATR_OB_ID,
    OB_ATR_Object_ObjectAttribute,
    Metadata_OB_ATR
FROM tes._OB_ATR_Object_ObjectAttribute;
-- Anchor table -------------------------------------------------------------------------------------------------------
-- PA_Part table (with 1 attributes)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS tes._PA_Part;
CREATE TABLE IF NOT EXISTS tes._PA_Part (
    PA_ID serial not null, 
    Metadata_PA int not null, 
    constraint pkPA_Part primary key (
        PA_ID
    )
);
ALTER TABLE IF EXISTS ONLY tes._PA_Part CLUSTER ON pkPA_Part;
-- DROP VIEW IF EXISTS tes.PA_Part;
CREATE OR REPLACE VIEW tes.PA_Part AS SELECT 
    PA_ID,
    Metadata_PA 
FROM tes._PA_Part;
-- Static attribute table ---------------------------------------------------------------------------------------------
-- PA_PAR_Part_PartAttribute table (on PA_Part)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS tes._PA_PAR_Part_PartAttribute;
CREATE TABLE IF NOT EXISTS tes._PA_PAR_Part_PartAttribute (
    PA_PAR_PA_ID int not null,
    PA_PAR_Part_PartAttribute varchar(255) not null,
    Metadata_PA_PAR int not null,
    constraint fkPA_PAR_Part_PartAttribute foreign key (
        PA_PAR_PA_ID
    ) references tes._PA_Part(PA_ID),
    constraint pkPA_PAR_Part_PartAttribute primary key (
        PA_PAR_PA_ID
    )
);
ALTER TABLE IF EXISTS ONLY tes._PA_PAR_Part_PartAttribute CLUSTER ON pkPA_PAR_Part_PartAttribute;
-- DROP VIEW IF EXISTS tes.PA_PAR_Part_PartAttribute;
CREATE OR REPLACE VIEW tes.PA_PAR_Part_PartAttribute AS SELECT
    PA_PAR_PA_ID,
    PA_PAR_Part_PartAttribute,
    Metadata_PA_PAR
FROM tes._PA_PAR_Part_PartAttribute;
-- TIES ---------------------------------------------------------------------------------------------------------------
--
-- Ties are used to represent relationships between entities.
-- They come in four flavors: static, historized, knotted static, and knotted historized.
-- Ties have cardinality, constraining how members may participate in the relationship.
-- Every entity that is a member in a tie has a specified role in the relationship.
-- Ties must have at least two anchor roles and zero or more knot roles.
--
-- Static tie table ---------------------------------------------------------------------------------------------------
-- OB_consissts_PA_of table (having 2 roles)
-----------------------------------------------------------------------------------------------------------------------
-- DROP TABLE IF EXISTS dbo._OB_consissts_PA_of;
CREATE TABLE IF NOT EXISTS dbo._OB_consissts_PA_of (
    OB_ID_consissts int not null, 
    PA_ID_of int not null, 
    Metadata_OB_consissts_PA_of int not null,
    constraint OB_consissts_PA_of_fkOB_consissts foreign key (
        OB_ID_consissts
    ) references tes._OB_Object(OB_ID), 
    constraint OB_consissts_PA_of_fkPA_of foreign key (
        PA_ID_of
    ) references tes._PA_Part(PA_ID), 
    constraint pkOB_consissts_PA_of primary key (
        PA_ID_of
    )
);
ALTER TABLE IF EXISTS ONLY dbo._OB_consissts_PA_of CLUSTER ON pkOB_consissts_PA_of;
-- DROP VIEW IF EXISTS dbo.OB_consissts_PA_of;
CREATE OR REPLACE VIEW dbo.OB_consissts_PA_of AS SELECT
    OB_ID_consissts,
    PA_ID_of,
    Metadata_OB_consissts_PA_of
FROM dbo._OB_consissts_PA_of;
-- KEY GENERATORS -----------------------------------------------------------------------------------------------------
--
-- These stored procedures can be used to generate identities of entities.
-- Corresponding anchors must have an incrementing identity column.
--
-- Key Generation Stored Procedure ------------------------------------------------------------------------------------
-- kOB_Object identity by surrogate key generation stored procedure
-----------------------------------------------------------------------------------------------------------------------
--DROP FUNCTION IF EXISTS tes.kOB_Object(
-- bigint,
-- int
--);
CREATE OR REPLACE FUNCTION tes.kOB_Object(
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
            INSERT INTO tes.OB_Object (
                Metadata_OB
            )
            SELECT
                metadata
            FROM
                idGenerator;
        END IF;
    END;
' LANGUAGE plpgsql;
-- Key Generation Stored Procedure ------------------------------------------------------------------------------------
-- kPA_Part identity by surrogate key generation stored procedure
-----------------------------------------------------------------------------------------------------------------------
--DROP FUNCTION IF EXISTS tes.kPA_Part(
-- bigint,
-- int
--);
CREATE OR REPLACE FUNCTION tes.kPA_Part(
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
            INSERT INTO tes.PA_Part (
                Metadata_PA
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
DROP FUNCTION IF EXISTS tes.dOB_Object(
    timestamp without time zone, 
    timestamp without time zone, 
    text
);
DROP VIEW IF EXISTS tes.nOB_Object;
DROP FUNCTION IF EXISTS tes.pOB_Object(
    timestamp without time zone
);
DROP VIEW IF EXISTS tes.lOB_Object;
DROP FUNCTION IF EXISTS tes.dPA_Part(
    timestamp without time zone, 
    timestamp without time zone, 
    text
);
DROP VIEW IF EXISTS tes.nPA_Part;
DROP FUNCTION IF EXISTS tes.pPA_Part(
    timestamp without time zone
);
DROP VIEW IF EXISTS tes.lPA_Part;
*/
-- Latest perspective -------------------------------------------------------------------------------------------------
-- lOB_Object viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW tes.lOB_Object AS
SELECT
    OB.OB_ID,
    OB.Metadata_OB,
    ATR.OB_ATR_OB_ID,
    ATR.Metadata_OB_ATR,
    ATR.OB_ATR_Object_ObjectAttribute
FROM
    tes.OB_Object OB
LEFT JOIN
    tes.OB_ATR_Object_ObjectAttribute ATR
ON
    ATR.OB_ATR_OB_ID = OB.OB_ID;
-- Latest perspective -------------------------------------------------------------------------------------------------
-- lPA_Part viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW tes.lPA_Part AS
SELECT
    PA.PA_ID,
    PA.Metadata_PA,
    PAR.PA_PAR_PA_ID,
    PAR.Metadata_PA_PAR,
    PAR.PA_PAR_Part_PartAttribute
FROM
    tes.PA_Part PA
LEFT JOIN
    tes.PA_PAR_Part_PartAttribute PAR
ON
    PAR.PA_PAR_PA_ID = PA.PA_ID;
-- Point-in-time perspective ------------------------------------------------------------------------------------------
-- pOB_Object viewed as it was on the given timepoint
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION tes.pOB_Object (
    changingTimepoint timestamp without time zone
)
RETURNS TABLE (
    OB_ID int,
    Metadata_OB int,
    OB_ATR_OB_ID int,
    Metadata_OB_ATR int,
    OB_ATR_Object_ObjectAttribute varchar(255)
) AS '
SELECT
    OB.OB_ID,
    OB.Metadata_OB,
    ATR.OB_ATR_OB_ID,
    ATR.Metadata_OB_ATR,
    ATR.OB_ATR_Object_ObjectAttribute
FROM
    tes.OB_Object OB
LEFT JOIN
    tes.OB_ATR_Object_ObjectAttribute ATR
ON
    ATR.OB_ATR_OB_ID = OB.OB_ID;
' LANGUAGE SQL;
-- Point-in-time perspective ------------------------------------------------------------------------------------------
-- pPA_Part viewed as it was on the given timepoint
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION tes.pPA_Part (
    changingTimepoint timestamp without time zone
)
RETURNS TABLE (
    PA_ID int,
    Metadata_PA int,
    PA_PAR_PA_ID int,
    Metadata_PA_PAR int,
    PA_PAR_Part_PartAttribute varchar(255)
) AS '
SELECT
    PA.PA_ID,
    PA.Metadata_PA,
    PAR.PA_PAR_PA_ID,
    PAR.Metadata_PA_PAR,
    PAR.PA_PAR_Part_PartAttribute
FROM
    tes.PA_Part PA
LEFT JOIN
    tes.PA_PAR_Part_PartAttribute PAR
ON
    PAR.PA_PAR_PA_ID = PA.PA_ID;
' LANGUAGE SQL;
-- Now perspective ----------------------------------------------------------------------------------------------------
-- nOB_Object viewed as it currently is (cannot include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW tes.nOB_Object AS
SELECT
    *
FROM
    tes.pOB_Object(LOCALTIMESTAMP);
-- Now perspective ----------------------------------------------------------------------------------------------------
-- nPA_Part viewed as it currently is (cannot include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW tes.nPA_Part AS
SELECT
    *
FROM
    tes.pPA_Part(LOCALTIMESTAMP);
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
-- DROP TRIGGER IF EXISTS tcbOB_ATR_Object_ObjectAttribute ON tes.OB_ATR_Object_ObjectAttribute;
-- DROP FUNCTION IF EXISTS tes.tcbOB_ATR_Object_ObjectAttribute();
CREATE OR REPLACE FUNCTION tes.tcbOB_ATR_Object_ObjectAttribute() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_OB_ATR_Object_ObjectAttribute (
            OB_ATR_OB_ID int not null,
            Metadata_OB_ATR int not null,
            OB_ATR_Object_ObjectAttribute varchar(255) not null,
            OB_ATR_Version bigint not null,
            OB_ATR_StatementType char(1) not null,
            primary key(
                OB_ATR_Version,
                OB_ATR_OB_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbOB_ATR_Object_ObjectAttribute
BEFORE INSERT ON tes.OB_ATR_Object_ObjectAttribute
FOR EACH STATEMENT
EXECUTE PROCEDURE tes.tcbOB_ATR_Object_ObjectAttribute();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciOB_ATR_Object_ObjectAttribute ON tes.OB_ATR_Object_ObjectAttribute;
-- DROP FUNCTION IF EXISTS tes.tciOB_ATR_Object_ObjectAttribute();
CREATE OR REPLACE FUNCTION tes.tciOB_ATR_Object_ObjectAttribute() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_OB_ATR_Object_ObjectAttribute
        SELECT
            NEW.OB_ATR_OB_ID,
            NEW.Metadata_OB_ATR,
            NEW.OB_ATR_Object_ObjectAttribute,
            1,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciOB_ATR_Object_ObjectAttribute
INSTEAD OF INSERT ON tes.OB_ATR_Object_ObjectAttribute
FOR EACH ROW
EXECUTE PROCEDURE tes.tciOB_ATR_Object_ObjectAttribute();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaOB_ATR_Object_ObjectAttribute ON tes.OB_ATR_Object_ObjectAttribute;
-- DROP FUNCTION IF EXISTS tes.tcaOB_ATR_Object_ObjectAttribute();
CREATE OR REPLACE FUNCTION tes.tcaOB_ATR_Object_ObjectAttribute() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find max version
    SELECT
        MAX(OB_ATR_Version) INTO maxVersion
    FROM
        _tmp_OB_ATR_Object_ObjectAttribute;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_OB_ATR_Object_ObjectAttribute
        SET
            OB_ATR_StatementType =
                CASE
                    WHEN ATR.OB_ATR_OB_ID is not null
                    THEN ''D'' -- duplicate
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_OB_ATR_Object_ObjectAttribute v
        LEFT JOIN
            tes._OB_ATR_Object_ObjectAttribute ATR
        ON
            ATR.OB_ATR_OB_ID = v.OB_ATR_OB_ID
        AND
            ATR.OB_ATR_Object_ObjectAttribute = v.OB_ATR_Object_ObjectAttribute
        WHERE
            v.OB_ATR_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO tes._OB_ATR_Object_ObjectAttribute (
            OB_ATR_OB_ID,
            Metadata_OB_ATR,
            OB_ATR_Object_ObjectAttribute
        )
        SELECT
            OB_ATR_OB_ID,
            Metadata_OB_ATR,
            OB_ATR_Object_ObjectAttribute
        FROM
            _tmp_OB_ATR_Object_ObjectAttribute
        WHERE
            OB_ATR_Version = currentVersion
        AND
            OB_ATR_StatementType in (''N'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_OB_ATR_Object_ObjectAttribute;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaOB_ATR_Object_ObjectAttribute
AFTER INSERT ON tes.OB_ATR_Object_ObjectAttribute
FOR EACH STATEMENT
EXECUTE PROCEDURE tes.tcaOB_ATR_Object_ObjectAttribute();
-- BEFORE INSERT trigger ----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcbPA_PAR_Part_PartAttribute ON tes.PA_PAR_Part_PartAttribute;
-- DROP FUNCTION IF EXISTS tes.tcbPA_PAR_Part_PartAttribute();
CREATE OR REPLACE FUNCTION tes.tcbPA_PAR_Part_PartAttribute() RETURNS trigger AS '
    BEGIN
        -- temporary table is used to create an insert order 
        -- (so that rows are inserted in order with respect to temporality)
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_PA_PAR_Part_PartAttribute (
            PA_PAR_PA_ID int not null,
            Metadata_PA_PAR int not null,
            PA_PAR_Part_PartAttribute varchar(255) not null,
            PA_PAR_Version bigint not null,
            PA_PAR_StatementType char(1) not null,
            primary key(
                PA_PAR_Version,
                PA_PAR_PA_ID
            )
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcbPA_PAR_Part_PartAttribute
BEFORE INSERT ON tes.PA_PAR_Part_PartAttribute
FOR EACH STATEMENT
EXECUTE PROCEDURE tes.tcbPA_PAR_Part_PartAttribute();
-- INSTEAD OF INSERT trigger ------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tciPA_PAR_Part_PartAttribute ON tes.PA_PAR_Part_PartAttribute;
-- DROP FUNCTION IF EXISTS tes.tciPA_PAR_Part_PartAttribute();
CREATE OR REPLACE FUNCTION tes.tciPA_PAR_Part_PartAttribute() RETURNS trigger AS '
    BEGIN
        -- insert rows into the temporary table
        INSERT INTO _tmp_PA_PAR_Part_PartAttribute
        SELECT
            NEW.PA_PAR_PA_ID,
            NEW.Metadata_PA_PAR,
            NEW.PA_PAR_Part_PartAttribute,
            1,
            ''X'';
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER tciPA_PAR_Part_PartAttribute
INSTEAD OF INSERT ON tes.PA_PAR_Part_PartAttribute
FOR EACH ROW
EXECUTE PROCEDURE tes.tciPA_PAR_Part_PartAttribute();
-- AFTER INSERT trigger -----------------------------------------------------------------------------------------------
-- DROP TRIGGER IF EXISTS tcaPA_PAR_Part_PartAttribute ON tes.PA_PAR_Part_PartAttribute;
-- DROP FUNCTION IF EXISTS tes.tcaPA_PAR_Part_PartAttribute();
CREATE OR REPLACE FUNCTION tes.tcaPA_PAR_Part_PartAttribute() RETURNS trigger AS '
    DECLARE maxVersion int;
    DECLARE currentVersion int = 0;
BEGIN
    -- find max version
    SELECT
        MAX(PA_PAR_Version) INTO maxVersion
    FROM
        _tmp_PA_PAR_Part_PartAttribute;
    -- is max version NULL?
    IF (maxVersion is null) THEN
        RETURN NULL;
    END IF;
    -- loop over versions
    LOOP
        currentVersion := currentVersion + 1;
        -- set statement types
        UPDATE _tmp_PA_PAR_Part_PartAttribute
        SET
            PA_PAR_StatementType =
                CASE
                    WHEN PAR.PA_PAR_PA_ID is not null
                    THEN ''D'' -- duplicate
                    ELSE ''N'' -- new statement
                END
        FROM
            _tmp_PA_PAR_Part_PartAttribute v
        LEFT JOIN
            tes._PA_PAR_Part_PartAttribute PAR
        ON
            PAR.PA_PAR_PA_ID = v.PA_PAR_PA_ID
        AND
            PAR.PA_PAR_Part_PartAttribute = v.PA_PAR_Part_PartAttribute
        WHERE
            v.PA_PAR_Version = currentVersion;
        -- insert data into attribute table
        INSERT INTO tes._PA_PAR_Part_PartAttribute (
            PA_PAR_PA_ID,
            Metadata_PA_PAR,
            PA_PAR_Part_PartAttribute
        )
        SELECT
            PA_PAR_PA_ID,
            Metadata_PA_PAR,
            PA_PAR_Part_PartAttribute
        FROM
            _tmp_PA_PAR_Part_PartAttribute
        WHERE
            PA_PAR_Version = currentVersion
        AND
            PA_PAR_StatementType in (''N'');
        EXIT WHEN currentVersion >= maxVersion;
    END LOOP;
    DROP TABLE IF EXISTS _tmp_PA_PAR_Part_PartAttribute;
    RETURN NULL;
END;
' LANGUAGE plpgsql;
CREATE TRIGGER tcaPA_PAR_Part_PartAttribute
AFTER INSERT ON tes.PA_PAR_Part_PartAttribute
FOR EACH STATEMENT
EXECUTE PROCEDURE tes.tcaPA_PAR_Part_PartAttribute();
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
--DROP TRIGGER IF EXISTS itb_lOB_Object ON tes.lOB_Object;
--DROP FUNCTION IF EXISTS tes.itb_lOB_Object();
CREATE OR REPLACE FUNCTION tes.itb_lOB_Object() RETURNS trigger AS '
    BEGIN
        -- create temporary table to keep inserted rows in
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_it_OB_Object (
            OB_ID int not null,
            Metadata_OB int not null,
            OB_ATR_OB_ID int null,
            Metadata_OB_ATR int null,
            OB_ATR_Object_ObjectAttribute varchar(255) null
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER itb_lOB_Object
BEFORE INSERT ON tes.lOB_Object
FOR EACH STATEMENT
EXECUTE PROCEDURE tes.itb_lOB_Object(); 
-- INSTEAD OF INSERT trigger ----------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS iti_lOB_Object ON tes.lOB_Object;
--DROP FUNCTION IF EXISTS tes.iti_lOB_Object();
CREATE OR REPLACE FUNCTION tes.iti_lOB_Object() RETURNS trigger AS '
    BEGIN
        -- generate anchor ID (if not provided)
        IF (NEW.OB_ID IS NULL) THEN 
            INSERT INTO tes.OB_Object (
                Metadata_OB 
            ) VALUES (
                NEW.Metadata_OB 
            );
            SELECT
                lastval() 
            INTO NEW.OB_ID;
        -- if anchor ID is provided then let''s insert it into the anchor table
        -- but only if that ID is not present in the anchor table
        ELSE
            INSERT INTO tes.OB_Object (
                Metadata_OB,
                OB_ID
            )
            SELECT
                NEW.Metadata_OB,
                NEW.OB_ID
            WHERE NOT EXISTS(
	            SELECT
	                OB_ID 
	            FROM tes.OB_Object
	            WHERE OB_ID = NEW.OB_ID
	            LIMIT 1
            );
        END IF;
        -- insert row into temporary table
    	INSERT INTO _tmp_it_OB_Object (
            OB_ID,
            Metadata_OB,
            OB_ATR_OB_ID,
            Metadata_OB_ATR,
            OB_ATR_Object_ObjectAttribute
    	) VALUES (
    	    NEW.OB_ID,
            NEW.Metadata_OB,
            COALESCE(NEW.OB_ATR_OB_ID, NEW.OB_ID),
            COALESCE(NEW.Metadata_OB_ATR, NEW.Metadata_OB),
            NEW.OB_ATR_Object_ObjectAttribute
    	);
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER iti_lOB_Object
INSTEAD OF INSERT ON tes.lOB_Object
FOR EACH ROW
EXECUTE PROCEDURE tes.iti_lOB_Object();
-- AFTER INSERT trigger ---------------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS ita_lOB_Object ON tes.lOB_Object;
--DROP FUNCTION IF EXISTS tes.ita_lOB_Object();
CREATE OR REPLACE FUNCTION tes.ita_lOB_Object() RETURNS trigger AS '
    BEGIN
        INSERT INTO tes.OB_ATR_Object_ObjectAttribute (
            OB_ATR_OB_ID,
            Metadata_OB_ATR,
            OB_ATR_Object_ObjectAttribute
        )
        SELECT
            i.OB_ATR_OB_ID,
            i.Metadata_OB_ATR,
            i.OB_ATR_Object_ObjectAttribute
        FROM
            _tmp_it_OB_Object i
        WHERE
            i.OB_ATR_Object_ObjectAttribute is not null;
        DROP TABLE IF EXISTS _tmp_it_OB_Object;
        RETURN NULL;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER ita_lOB_Object
AFTER INSERT ON tes.lOB_Object
FOR EACH STATEMENT
EXECUTE PROCEDURE tes.ita_lOB_Object();
-- BEFORE INSERT trigger --------------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS itb_lPA_Part ON tes.lPA_Part;
--DROP FUNCTION IF EXISTS tes.itb_lPA_Part();
CREATE OR REPLACE FUNCTION tes.itb_lPA_Part() RETURNS trigger AS '
    BEGIN
        -- create temporary table to keep inserted rows in
        CREATE TEMPORARY TABLE IF NOT EXISTS _tmp_it_PA_Part (
            PA_ID int not null,
            Metadata_PA int not null,
            PA_PAR_PA_ID int null,
            Metadata_PA_PAR int null,
            PA_PAR_Part_PartAttribute varchar(255) null
        ) ON COMMIT DROP;
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER itb_lPA_Part
BEFORE INSERT ON tes.lPA_Part
FOR EACH STATEMENT
EXECUTE PROCEDURE tes.itb_lPA_Part(); 
-- INSTEAD OF INSERT trigger ----------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS iti_lPA_Part ON tes.lPA_Part;
--DROP FUNCTION IF EXISTS tes.iti_lPA_Part();
CREATE OR REPLACE FUNCTION tes.iti_lPA_Part() RETURNS trigger AS '
    BEGIN
        -- generate anchor ID (if not provided)
        IF (NEW.PA_ID IS NULL) THEN 
            INSERT INTO tes.PA_Part (
                Metadata_PA 
            ) VALUES (
                NEW.Metadata_PA 
            );
            SELECT
                lastval() 
            INTO NEW.PA_ID;
        -- if anchor ID is provided then let''s insert it into the anchor table
        -- but only if that ID is not present in the anchor table
        ELSE
            INSERT INTO tes.PA_Part (
                Metadata_PA,
                PA_ID
            )
            SELECT
                NEW.Metadata_PA,
                NEW.PA_ID
            WHERE NOT EXISTS(
	            SELECT
	                PA_ID 
	            FROM tes.PA_Part
	            WHERE PA_ID = NEW.PA_ID
	            LIMIT 1
            );
        END IF;
        -- insert row into temporary table
    	INSERT INTO _tmp_it_PA_Part (
            PA_ID,
            Metadata_PA,
            PA_PAR_PA_ID,
            Metadata_PA_PAR,
            PA_PAR_Part_PartAttribute
    	) VALUES (
    	    NEW.PA_ID,
            NEW.Metadata_PA,
            COALESCE(NEW.PA_PAR_PA_ID, NEW.PA_ID),
            COALESCE(NEW.Metadata_PA_PAR, NEW.Metadata_PA),
            NEW.PA_PAR_Part_PartAttribute
    	);
        RETURN NEW;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER iti_lPA_Part
INSTEAD OF INSERT ON tes.lPA_Part
FOR EACH ROW
EXECUTE PROCEDURE tes.iti_lPA_Part();
-- AFTER INSERT trigger ---------------------------------------------------------------------------------------------------------
--DROP TRIGGER IF EXISTS ita_lPA_Part ON tes.lPA_Part;
--DROP FUNCTION IF EXISTS tes.ita_lPA_Part();
CREATE OR REPLACE FUNCTION tes.ita_lPA_Part() RETURNS trigger AS '
    BEGIN
        INSERT INTO tes.PA_PAR_Part_PartAttribute (
            PA_PAR_PA_ID,
            Metadata_PA_PAR,
            PA_PAR_Part_PartAttribute
        )
        SELECT
            i.PA_PAR_PA_ID,
            i.Metadata_PA_PAR,
            i.PA_PAR_Part_PartAttribute
        FROM
            _tmp_it_PA_Part i
        WHERE
            i.PA_PAR_Part_PartAttribute is not null;
        DROP TABLE IF EXISTS _tmp_it_PA_Part;
        RETURN NULL;
    END;
' LANGUAGE plpgsql;
CREATE TRIGGER ita_lPA_Part
AFTER INSERT ON tes.lPA_Part
FOR EACH STATEMENT
EXECUTE PROCEDURE tes.ita_lPA_Part();
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
DROP VIEW IF EXISTS dbo.nOB_consissts_PA_of;
DROP FUNCTION IF EXISTS dbo.pOB_consissts_PA_of(
    timestamp without time zone
);
DROP VIEW IF EXISTS dbo.lOB_consissts_PA_of;
*/
-- Latest perspective -------------------------------------------------------------------------------------------------
-- lOB_consissts_PA_of viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.lOB_consissts_PA_of AS
SELECT
    tie.Metadata_OB_consissts_PA_of,
    tie.OB_ID_consissts,
    tie.PA_ID_of
FROM
    dbo.OB_consissts_PA_of tie;
-- Point-in-time perspective ------------------------------------------------------------------------------------------
-- pOB_consissts_PA_of viewed by the latest available information (may include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION dbo.pOB_consissts_PA_of (
    changingTimepoint timestamp without time zone
)
RETURNS TABLE (
    Metadata_OB_consissts_PA_of int,
    OB_ID_consissts int,
    PA_ID_of int
) AS '
SELECT
    tie.Metadata_OB_consissts_PA_of,
    tie.OB_ID_consissts,
    tie.PA_ID_of
FROM
    dbo.OB_consissts_PA_of tie;
' LANGUAGE SQL;
-- Now perspective ----------------------------------------------------------------------------------------------------
-- nOB_consissts_PA_of viewed as it currently is (cannot include future versions)
-----------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE VIEW dbo.nOB_consissts_PA_of AS
SELECT
    *
FROM
    dbo.pOB_consissts_PA_of(LOCALTIMESTAMP);
-- DESCRIPTIONS -------------------------------------------------------------------------------------------------------
