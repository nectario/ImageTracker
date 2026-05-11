ALTER TABLE `ImageAsset`
    ADD COLUMN `Category` VARCHAR(255) NULL AFTER `LocationUpdatedAtUtc`,
    ADD COLUMN `CategorySource` VARCHAR(64) NULL AFTER `Category`,
    ADD KEY `Ix_ImageAsset_Category` (`Category`(191)),
    ADD KEY `Ix_ImageAsset_CategorySource` (`CategorySource`),
    ADD KEY `Ix_ImageAsset_Category_Date` (`Category`(191), `Date`),
    ADD KEY `Ix_ImageAsset_StreetAddress` (`StreetAddress`(191)),
    ADD KEY `Ix_ImageAsset_StreetAddress_Category` (`StreetAddress`(191), `Category`(191)),
    ADD KEY `Ix_ImageAsset_Date` (`Date`),
    ADD KEY `Ix_ImageAsset_TimeZone_Date` (`TimeZone`, `Date`),
    ADD KEY `Ix_ImageAsset_LatLon_Date` (`Latitude`, `Longitude`, `Date`);
