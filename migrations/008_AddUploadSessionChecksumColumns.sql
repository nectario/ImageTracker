ALTER TABLE `UploadSession`
    ADD COLUMN `S3ChecksumAlgorithm` VARCHAR(16) NOT NULL AFTER `ChecksumSha256`,
    ADD COLUMN `S3ChecksumType` VARCHAR(16) NOT NULL AFTER `S3ChecksumAlgorithm`,
    ADD COLUMN `S3ChecksumValue` VARCHAR(255) NULL AFTER `S3ChecksumType`,
    ALGORITHM=INSTANT;
