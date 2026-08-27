ALTER TABLE `LegacyImageAssetMap`
    DROP FOREIGN KEY `Fk_LegacyImageAssetMap_MediaAsset`,
    DROP FOREIGN KEY `Fk_LegacyImageAssetMap_MediaOccurrence`,
    DROP INDEX `Fk_LegacyImageAssetMap_MediaAsset`,
    ADD KEY `Ix_LegacyImageAssetMap_User_MediaAsset` (`UserId`, `MediaAssetId`),
    ADD KEY `Ix_LegacyImageAssetMap_User_MediaOccurrence` (`UserId`, `MediaOccurrenceId`),
    ADD CONSTRAINT `Fk_LegacyImageAssetMap_User_MediaAsset`
        FOREIGN KEY (`UserId`, `MediaAssetId`) REFERENCES `MediaAsset` (`UserId`, `Id`) ON DELETE RESTRICT,
    ADD CONSTRAINT `Fk_LegacyImageAssetMap_User_MediaOccurrence`
        FOREIGN KEY (`UserId`, `MediaOccurrenceId`) REFERENCES `MediaOccurrence` (`UserId`, `Id`) ON DELETE RESTRICT,
    ALGORITHM=COPY;
