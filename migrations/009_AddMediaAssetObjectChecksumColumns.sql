ALTER TABLE `MediaAsset`
    ADD COLUMN `ContentHashSource` VARCHAR(32) NOT NULL DEFAULT 'ClientDeclared' AFTER `ContentSha256`,
    ADD COLUMN `ContentHashVerifiedAtUtc` DATETIME(6) NULL AFTER `ContentHashSource`,
    ADD COLUMN `OriginalS3ChecksumAlgorithm` VARCHAR(16) NULL AFTER `OriginalS3ETag`,
    ADD COLUMN `OriginalS3ChecksumType` VARCHAR(16) NULL AFTER `OriginalS3ChecksumAlgorithm`,
    ADD COLUMN `OriginalS3ChecksumValue` VARCHAR(255) NULL AFTER `OriginalS3ChecksumType`,
    ADD COLUMN `PreviewS3ChecksumAlgorithm` VARCHAR(16) NULL AFTER `PreviewS3ObjectKey`,
    ADD COLUMN `PreviewS3ChecksumType` VARCHAR(16) NULL AFTER `PreviewS3ChecksumAlgorithm`,
    ADD COLUMN `PreviewS3ChecksumValue` VARCHAR(255) NULL AFTER `PreviewS3ChecksumType`,
    ALGORITHM=INSTANT;
