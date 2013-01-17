DO $$BEGIN

IF (SELECT NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'dataset_lifecycle_stage')) THEN
  CREATE TYPE dataset_lifecycle_stage AS ENUM('Unpublished', 'Published', 'Snapshotted');
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

-- This is a separate table from dataset_map so it can continue to exist
-- even if the dataset_map entry goes away.
CREATE TABLE IF NOT EXISTS truth_manifest (
  dataset_system_id    BIGINT NOT NULL PRIMARY KEY,
  published_version    BIGINT NOT NULL, -- The last (data log) version set to "published" (0 if never)
  latest_version       BIGINT NOT NULL  -- The last version
);

CREATE TABLE IF NOT EXISTS secondary_stores (
  system_id         BIGINT                  NOT NULL PRIMARY KEY,
  store_id          VARCHAR(%STORE_ID_LEN%) NOT NULL UNIQUE,
  wants_unpublished BOOLEAN                 NOT NULL
);

CREATE TABLE IF NOT EXISTS secondary_manifest (
  store_system_id   BIGINT NOT NULL REFERENCES secondary_stores(system_id),
  dataset_system_id BIGINT NOT NULL REFERENCES truth_manifest(dataset_system_id),
  version           BIGINT NOT NULL, -- data log version.  0 if never fed anything in
  PRIMARY KEY (store_system_id, dataset_system_id)
);

CREATE TABLE IF NOT EXISTS dataset_map (
  -- Note that IT IS ASSUMED THAT dataset_id WILL NEVER CHANGE.  In other words, dataset_id should
  -- not have anything in particular to do with SoQL resource names.  It is NOT assumed that they will
  -- not be re-used, however.
  --
  -- Note 2: when flipping a backup to primary, the system_id sequence object must be set since
  -- playing back logs doesn't access the object.
  system_id       BIGSERIAL                   NOT NULL PRIMARY KEY,
  dataset_id      VARCHAR(%DATASET_ID_LEN%)   NOT NULL UNIQUE, -- This probably contains the domain ID in some manner...
  table_base_base VARCHAR(%PHYSTAB_BASE_LEN%) NOT NULL -- this + version_map's lifecycle_version is used to name per-dataset tables.
);

CREATE TABLE IF NOT EXISTS version_map (
  system_id             BIGSERIAL                  NOT NULL PRIMARY KEY,
  dataset_system_id     BIGINT                     NOT NULL REFERENCES dataset_map(system_id),
  lifecycle_version     BIGINT                     NOT NULL, -- this gets incremented per copy made.  It has nothing to do with the log's version
  lifecycle_stage       dataset_lifecycle_stage    NOT NULL,
  UNIQUE (dataset_system_id, lifecycle_version)
);

CREATE TABLE IF NOT EXISTS column_map (
  system_id                 BIGINT                      NOT NULL,
  version_system_id         BIGINT                      NOT NULL REFERENCES version_map(system_id),
  logical_column            VARCHAR(%COLUMN_NAME_LEN%)  NOT NULL, -- "logical column" is roughly "user-visible SoQL name"
  type_name                 VARCHAR(%TYPE_NAME_LEN%)    NOT NULL,
  -- all your physical columns are belong to us
  physical_column_base_base VARCHAR(%PHYSCOL_BASE_LEN%) NOT NULL, -- the true PCB is p_c_b_b + "_" + system_id
  is_user_primary_key       unit                        NULL, -- evil "unique" hack
  -- Making a copy preserves the system_id of columns.  Therefore, we need a two-part primary key
  -- in order to uniquely identifiy a column.
  -- It's in the order version-id-then-column-id so the implied index can (I think!) be used for
  -- "give me all the columns of this dataset version".
  PRIMARY KEY (version_system_id, system_id),
  UNIQUE (version_system_id, logical_column),
  UNIQUE (version_system_id, is_user_primary_key) -- hack hack hack
);

END$$;
