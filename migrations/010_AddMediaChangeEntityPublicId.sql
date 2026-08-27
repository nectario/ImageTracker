ALTER TABLE `MediaChange`
    ADD COLUMN `EntityPublicId` CHAR(36) CHARACTER SET ascii COLLATE ascii_bin NOT NULL AFTER `EntityId`,
    ADD KEY `Ix_MediaChange_User_EntityPublicId` (`UserId`, `EntityPublicId`),
    ALGORITHM=INPLACE,
    LOCK=NONE;
