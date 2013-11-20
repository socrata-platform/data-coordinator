DO $$BEGIN

IF (SELECT NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'dataset_lifecycle_stage')) THEN
  CREATE TYPE dataset_lifecycle_stage AS ENUM('Unpublished', 'Published', 'Snapshotted', 'Discarded');
END IF;

IF (SELECT NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'unit')) THEN
  CREATE TYPE unit AS ENUM('Unit');
END IF;

-- This is a separate table from dataset_map so it can continue to exist
-- even if the dataset_map entry goes away.
CREATE TABLE IF NOT EXISTS truth_manifest (
  dataset_system_id    BIGINT NOT NULL PRIMARY KEY,
  published_version    BIGINT NOT NULL, -- The last (data log) version set to "published" (0 if never)
  latest_version       BIGINT NOT NULL  -- The last (data log) version
);

CREATE TABLE IF NOT EXISTS secondary_stores_config (
  store_id            VARCHAR(%STORE_ID_LEN%)  NOT NULL PRIMARY KEY,
  next_run_time       TIMESTAMP WITH TIME ZONE NOT NULL,
  interval_in_seconds INT                      NOT NULL
);

CREATE TABLE IF NOT EXISTS dataset_map (
  -- Note: when flipping a backup to primary, the system_id sequence object must be set since
  -- playing back logs doesn't access the object.
  system_id          BIGSERIAL                    NOT NULL PRIMARY KEY,
  next_counter_value BIGINT                       NOT NULL,
  locale_name        VARCHAR(%LOCALE_NAME_LEN%)   NOT NULL,
  obfuscation_key    BYTEA                        NOT NULL
);

CREATE TABLE IF NOT EXISTS copy_map (
  system_id             BIGSERIAL                  NOT NULL PRIMARY KEY,
  dataset_system_id     BIGINT                     NOT NULL REFERENCES dataset_map(system_id),
  copy_number           BIGINT                     NOT NULL, -- this gets incremented per copy made.
  lifecycle_stage       dataset_lifecycle_stage    NOT NULL,
  data_version          BIGINT                     NOT NULL, -- this refers to the log's version
  UNIQUE (dataset_system_id, copy_number)
);

CREATE TABLE IF NOT EXISTS column_map (
  system_id                 BIGINT                        NOT NULL,
  copy_system_id            BIGINT                        NOT NULL REFERENCES copy_map(system_id),
  user_column_id            VARCHAR(%USER_COLUMN_ID_LEN%) NOT NULL,
  type_name                 VARCHAR(%TYPE_NAME_LEN%)      NOT NULL,
  -- all your physical columns are belong to us
  physical_column_base_base VARCHAR(%PHYSCOL_BASE_LEN%)   NOT NULL, -- the true PCB is p_c_b_b + "_" + system_id
  is_system_primary_key     unit                          NULL, -- evil "unique" hack
  is_user_primary_key       unit                          NULL, -- evil "unique" hack
  is_version                unit                          NULL, -- evil "unique" hack
  -- Making a copy preserves the system_id of columns.  Therefore, we need a two-part primary key
  -- in order to uniquely identify a column.
  -- It's in the order table-id-then-column-id so the implied index can (I think!) be used for
  -- "give me all the columns of this dataset version".
  PRIMARY KEY (copy_system_id, system_id),
  UNIQUE (copy_system_id, user_column_id),
  UNIQUE (copy_system_id, is_system_primary_key), -- hack hack hack
  UNIQUE (copy_system_id, is_user_primary_key), -- hack hack hack
  UNIQUE (copy_system_id, is_version) -- hack hack hack
);

CREATE TABLE IF NOT EXISTS pending_table_drops (
  id         BIGSERIAL                 NOT NULL PRIMARY KEY,
  table_name VARCHAR(%TABLE_NAME_LEN%) NOT NULL,
  queued_at  TIMESTAMP WITH TIME ZONE  NOT NULL
);

CREATE TABLE IF NOT EXISTS secondary_manifest (
  store_id                         VARCHAR(64) NOT NULL,
  dataset_system_id                BIGINT NOT NULL, -- doesn't "reference" because this has to be able to refer to deleted datasets
  latest_secondary_data_version    BIGINT NOT NULL DEFAULT 0,
  latest_secondary_lifecycle_stage dataset_lifecycle_stage NOT NULL DEFAULT 'Unpublished',
  latest_data_version              BIGINT NOT NULL,
  went_out_of_sync_at              TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(), -- used to order processing
  cookie                           TEXT NULL,
  broken_at                        TIMESTAMP WITH TIME ZONE NULL,
  PRIMARY KEY (store_id, dataset_system_id)
);

DROP INDEX IF EXISTS secondary_manifest_dataset_system_id;
CREATE INDEX secondary_manifest_dataset_system_id ON secondary_manifest(dataset_system_id);

DROP INDEX IF EXISTS secondary_manifest_order;
CREATE INDEX secondary_manifest_order ON secondary_manifest (store_id, (broken_at IS NULL), (latest_data_version > latest_secondary_data_version), went_out_of_sync_at);

CREATE TABLE IF NOT EXISTS backup_log (
  dataset_system_id          BIGINT                   NOT NULL UNIQUE, -- doesn't "reference" because this has to be able to refer to deleted datasets
  latest_backup_data_version BIGINT                   NOT NULL DEFAULT 0,
  latest_data_version        BIGINT                   NOT NULL DEFAULT 0,
  went_out_of_sync_at        TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now() -- used to order processing
);

DROP INDEX IF EXISTS backup_log_order;
CREATE INDEX backup_log_order ON backup_log ((latest_data_version > latest_backup_data_version), went_out_of_sync_at);

CREATE OR REPLACE FUNCTION add_to_backup_log() RETURNS trigger AS $add_to_backup_log$
  BEGIN
    INSERT INTO backup_log (dataset_system_id) VALUES (NEW.system_id);
    RETURN NEW;
  END;
$add_to_backup_log$ LANGUAGE PLPGSQL;

DROP TRIGGER IF EXISTS create_dataset_log_trigger ON dataset_map;
CREATE TRIGGER create_dataset_log_trigger AFTER INSERT ON dataset_map FOR EACH ROW EXECUTE PROCEDURE add_to_backup_log();

CREATE OR REPLACE FUNCTION update_backup_log() RETURNS trigger as $update_backup_log$
  DECLARE
    last_data_version BIGINT;
  BEGIN
    -- First, we update the timestamp only if this is the first change since the last time
    -- the backup was run.  This is to keep things fair, otherwise a fast-changing dataset
    -- could keep knocking itself down the queue.
    UPDATE backup_log
      SET went_out_of_sync_at = CURRENT_TIMESTAMP
      WHERE dataset_system_id = NEW.dataset_system_id AND latest_data_version = latest_backup_data_version;
    -- Then unconditionally bump the latest data version.
    UPDATE backup_log SET latest_data_version = NEW.data_version WHERE dataset_system_id = NEW.dataset_system_id;

    -- Same thing for all secondaries to which this is attached.
    UPDATE secondary_manifest
      SET went_out_of_sync_at = CURRENT_TIMESTAMP
      WHERE dataset_system_id = NEW.dataset_system_id AND latest_data_version = latest_secondary_data_version;
    UPDATE secondary_manifest
      SET latest_data_version = NEW.data_version
      WHERE secondary_manifest.dataset_system_id = NEW.dataset_system_id;
    RETURN NEW;
  END;
$update_backup_log$ LANGUAGE PLPGSQL;

DROP TRIGGER IF EXISTS update_dataset_log_trigger ON copy_map;
CREATE TRIGGER update_dataset_log_trigger AFTER UPDATE ON copy_map FOR EACH ROW EXECUTE PROCEDURE update_backup_log();

END$$;
