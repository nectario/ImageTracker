CREATE TABLE IF NOT EXISTS `ImageAsset` (
    `Id` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `Source` VARCHAR(32) NOT NULL DEFAULT 'OneDrive',
    `DriveItemId` VARCHAR(128) NOT NULL,
    `FileName` VARCHAR(512) NOT NULL,
    `TakenDateTimeUtc` DATETIME NULL,
    `Latitude` DOUBLE NULL,
    `Longitude` DOUBLE NULL,
    `Altitude` DOUBLE NULL,
    `ShortDescription` TEXT NULL,
    `ShortDescriptionModel` VARCHAR(128) NULL,
    `ShortDescriptionUpdatedAtUtc` DATETIME NULL,
    `IsDeleted` TINYINT(1) NOT NULL DEFAULT 0,
    `DeletedAtUtc` DATETIME NULL,
    `RawGraphJson` JSON NULL,
    `InsertedAtUtc` DATETIME NOT NULL,
    `UpdatedAtUtc` DATETIME NOT NULL,
    UNIQUE KEY `Ux_ImageAsset_Source_DriveItemId` (`Source`, `DriveItemId`),
    KEY `Ix_ImageAsset_TakenDateTimeUtc` (`TakenDateTimeUtc`),
    KEY `Ix_ImageAsset_LatLon` (`Latitude`, `Longitude`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `OneDriveSyncState` (
    `Id` INT PRIMARY KEY,
    `FolderDriveItemId` VARCHAR(128) NOT NULL,
    `FolderPath` VARCHAR(1024) NOT NULL,
    `DeltaLink` TEXT NULL,
    `LastRunAtUtc` DATETIME NULL,
    `LastSuccessAtUtc` DATETIME NULL,
    `LastError` TEXT NULL,
    `UpdatedAtUtc` DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `OneDriveTokenCache` (
    `Id` INT PRIMARY KEY,
    `CacheJson` LONGTEXT NOT NULL,
    `UpdatedAtUtc` DATETIME NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
