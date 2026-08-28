ALTER TABLE `ProviderUsageMonth`
    ADD COLUMN `CircuitState` VARCHAR(16) NOT NULL DEFAULT 'Closed' AFTER `HardLimitUnits`,
    ADD COLUMN `CircuitOpenedAtUtc` DATETIME(6) NULL AFTER `CircuitState`,
    ADD COLUMN `CircuitFailureCode` VARCHAR(64) NULL AFTER `CircuitOpenedAtUtc`,
    ALGORITHM=INSTANT;
