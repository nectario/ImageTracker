ALTER TABLE `ImageAsset`
    CHANGE COLUMN `ShortDescription` `Description` TEXT NULL;

ALTER TABLE `ImageAsset`
    CHANGE COLUMN `ShortDescriptionModel` `DescriptionModel` VARCHAR(128) NULL;

ALTER TABLE `ImageAsset`
    CHANGE COLUMN `ShortDescriptionUpdatedAtUtc` `DescriptionUpdatedAtUtc` DATETIME NULL;
