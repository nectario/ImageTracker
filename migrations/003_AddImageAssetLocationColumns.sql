ALTER TABLE `ImageAsset`
    ADD COLUMN `LocationDisplayName` VARCHAR(512) NULL AFTER `DescriptionUpdatedAtUtc`,
    ADD COLUMN `StreetAddress` VARCHAR(512) NULL AFTER `LocationDisplayName`,
    ADD COLUMN `Neighborhood` VARCHAR(255) NULL AFTER `StreetAddress`,
    ADD COLUMN `City` VARCHAR(255) NULL AFTER `Neighborhood`,
    ADD COLUMN `County` VARCHAR(255) NULL AFTER `City`,
    ADD COLUMN `State` VARCHAR(255) NULL AFTER `County`,
    ADD COLUMN `PostalCode` VARCHAR(32) NULL AFTER `State`,
    ADD COLUMN `Country` VARCHAR(255) NULL AFTER `PostalCode`,
    ADD COLUMN `CountryCode` VARCHAR(8) NULL AFTER `Country`,
    ADD COLUMN `LocationProvider` VARCHAR(64) NULL AFTER `CountryCode`,
    ADD COLUMN `LocationUpdatedAtUtc` DATETIME NULL AFTER `LocationProvider`;
