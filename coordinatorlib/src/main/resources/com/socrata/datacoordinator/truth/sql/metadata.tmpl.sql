DO $$BEGIN

IF (SELECT NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'dataset_lifecycle_stage')) THEN
  CREATE TYPE dataset_lifecycle_stage AS ENUM('Unpublished', 'Published', 'Snapshotted', 'Discarded');
END IF;

IF (SELECT NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'unit')) THEN
  CREATE TYPE unit AS ENUM('Unit');
END IF;

CREATE TABLE IF NOT EXISTS global_log (
  id                BIGINT                   NOT NULL PRIMARY KEY, -- guaranteed to be contiguous and strictly increasing
  dataset_system_id BIGINT                   NOT NULL, -- Not REFERENCES because datasets can be deleted
  version           BIGINT                   NOT NULL,
  updated_at        TIMESTAMP WITH TIME ZONE NOT NULL,
  updated_by        VARCHAR(%USER_UID_LEN%)  NOT NULL
);

CREATE TABLE IF NOT EXISTS last_id_sent_to_backup (
  id                  BIGINT NOT NULL REFERENCES global_log(id),
  single_row_enforcer unit   NOT NULL DEFAULT 'Unit'
);

CREATE TABLE IF NOT EXISTS last_id_processed_for_secondaries (
  id                  BIGINT NOT NULL REFERENCES global_log(id),
  single_row_enforcer unit   NOT NULL DEFAULT 'Unit'
);

-- This is a separate table from dataset_map so it can continue to exist
-- even if the dataset_map entry goes away.
CREATE TABLE IF NOT EXISTS truth_manifest (
  dataset_system_id    BIGINT NOT NULL PRIMARY KEY,
  published_version    BIGINT NOT NULL, -- The last (data log) version set to "published" (0 if never)
  latest_version       BIGINT NOT NULL  -- The last (data log) version
);

CREATE TABLE IF NOT EXISTS secondary_stores (
  system_id         BIGINT                  NOT NULL PRIMARY KEY,
  store_id          VARCHAR(%STORE_ID_LEN%) NOT NULL UNIQUE,
  wants_unpublished BOOLEAN                 NOT NULL
);

CREATE TABLE IF NOT EXISTS dataset_map (
  -- Note that IT IS ASSUMED THAT dataset_id WILL NEVER CHANGE.  In other words, dataset_id should
  -- not have anything in particular to do with SoQL resource names.  It is NOT assumed that they will
  -- not be re-used, however.
  --
  -- Note 2: when flipping a backup to primary, the system_id sequence object must be set since
  -- playing back logs doesn't access the object.
  system_id       BIGSERIAL                   NOT NULL PRIMARY KEY,
  dataset_name    VARCHAR(%DATASET_ID_LEN%)   NOT NULL UNIQUE, -- This probably contains the domain ID in some manner...
  table_base_base VARCHAR(%PHYSTAB_BASE_LEN%) NOT NULL, -- this + version_map's copy_number is used to name per-dataset tables.
  next_row_id     BIGINT                      NOT NULL
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
  system_id                 BIGINT                      NOT NULL,
  copy_system_id            BIGINT                      NOT NULL REFERENCES copy_map(system_id),
  logical_column            VARCHAR(%LOGICAL_NAME_LEN%) NOT NULL, -- "logical column" is roughly "user-visible SoQL name"
  type_name                 VARCHAR(%TYPE_NAME_LEN%)    NOT NULL,
  -- all your physical columns are belong to us
  physical_column_base_base VARCHAR(%PHYSCOL_BASE_LEN%) NOT NULL, -- the true PCB is p_c_b_b + "_" + system_id
  is_system_primary_key     unit                        NULL, -- evil "unique" hack
  is_user_primary_key       unit                        NULL, -- evil "unique" hack
  -- Making a copy preserves the system_id of columns.  Therefore, we need a two-part primary key
  -- in order to uniquely identify a column.
  -- It's in the order table-id-then-column-id so the implied index can (I think!) be used for
  -- "give me all the columns of this dataset version".
  PRIMARY KEY (copy_system_id, system_id),
  UNIQUE (copy_system_id, logical_column),
  UNIQUE (copy_system_id, is_system_primary_key), -- hack hack hack
  UNIQUE (copy_system_id, is_user_primary_key) -- hack hack hack
);

CREATE TABLE IF NOT EXISTS pending_table_drops (
  id         BIGSERIAL                 NOT NULL PRIMARY KEY,
  table_name VARCHAR(%TABLE_NAME_LEN%) NOT NULL,
  queued_at  TIMESTAMP WITH TIME ZONE  NOT NULL
);

CREATE TABLE IF NOT EXISTS secondary_manifest (
  store_id          VARCHAR(64) NOT NULL,
  dataset_system_id BIGINT NOT NULL REFERENCES dataset_map(system_id),
  version           BIGINT NOT NULL, -- data log version.  0 if never fed anything in
  cookie            TEXT NULL,
  PRIMARY KEY (store_id, dataset_system_id)
);

END$$;
