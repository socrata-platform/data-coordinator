DO $$BEGIN

DROP TRIGGER IF EXISTS update_dataset_log_trigger ON copy_map;
DROP FUNCTION IF EXISTS update_backup_log();
DROP TRIGGER IF EXISTS create_dataset_log_trigger ON dataset_map;
DROP FUNCTION IF EXISTS add_to_backup_log();
DROP INDEX IF EXISTS backup_log_order;
DROP TABLE IF EXISTS backup_log;
DROP INDEX IF EXISTS secondary_manifest_order;
DROP INDEX IF EXISTS secondary_manifest_dataset_system_id;
DROP TABLE IF EXISTS secondary_manifest;
DROP TABLE IF EXISTS pending_table_drops;
DROP TABLE IF EXISTS column_map;
DROP TABLE IF EXISTS copy_map;
DROP TABLE IF EXISTS dataset_map;
DROP TABLE IF EXISTS secondary_stores_config;
DROP TABLE IF EXISTS truth_manifest;
DROP TYPE IF EXISTS unit;
DROP TYPE IF EXISTS dataset_lifecycle_stage;

END$$;
