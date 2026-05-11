ALTER TABLE `ImageAsset`
    ADD COLUMN `DateTime` DATETIME NULL AFTER `FileName`,
    ADD COLUMN `Date` DATE NULL AFTER `DateTime`,
    ADD COLUMN `Time` TIME NULL AFTER `Date`,
    ADD COLUMN `TimeZone` VARCHAR(64) NULL AFTER `Time`,
    ADD COLUMN `UtcOffsetMinutes` SMALLINT NULL AFTER `TimeZone`,
    ADD COLUMN `DateTimeUtc` DATETIME NULL AFTER `UtcOffsetMinutes`,
    ADD COLUMN `CreatedAt` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP AFTER `UpdatedAtUtc`,
    ADD COLUMN `ModifiedAt` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER `CreatedAt`;

UPDATE `ImageAsset`
SET
    `DateTimeUtc` = COALESCE(`DateTimeUtc`, `TakenDateTimeUtc`),
    `DateTime` = COALESCE(`DateTime`, `TakenDateTimeUtc`),
    `Date` = COALESCE(`Date`, DATE(COALESCE(`DateTime`, `TakenDateTimeUtc`))),
    `Time` = COALESCE(`Time`, TIME(COALESCE(`DateTime`, `TakenDateTimeUtc`))),
    `TimeZone` = COALESCE(`TimeZone`, 'UTC'),
    `UtcOffsetMinutes` = COALESCE(`UtcOffsetMinutes`, 0),
    `CreatedAt` = COALESCE(`InsertedAtUtc`, `CreatedAt`),
    `ModifiedAt` = COALESCE(`UpdatedAtUtc`, `ModifiedAt`);
