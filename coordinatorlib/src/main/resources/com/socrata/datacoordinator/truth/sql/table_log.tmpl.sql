CREATE TABLE IF NOT EXISTS %AUDIT_TABLE_NAME% (
  version    BIGINT                   NOT NULL, -- guaranteed to be contiguous and strictly increasing
  who        VARCHAR(%USER_UID_LEN%)  NOT NULL,
  at_time    TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT now(),
  PRIMARY KEY (version)
) %TABLESPACE%;

ALTER TABLE %AUDIT_TABLE_NAME% OWNER TO %USER%;

DO $$
BEGIN

IF NOT EXISTS (
  SELECT 1
  FROM pg_indexes
  WHERE schemaname = 'public' and indexname = '%AUDIT_TABLE_NAME%_at_time'
) THEN
    CREATE INDEX %AUDIT_TABLE_NAME%_at_time ON %AUDIT_TABLE_NAME% (at_time) %TABLESPACE%;
END IF;

END$$;

CREATE TABLE IF NOT EXISTS %TABLE_NAME% (
  version    BIGINT                   NOT NULL REFERENCES %AUDIT_TABLE_NAME% (version), -- guaranteed to be contiguous and strictly increasing
  subversion BIGINT                   NOT NULL, -- guaranteed to be contiguous and strictly increasing within a version
  what       VARCHAR(%OPERATION_LEN%) NOT NULL,
  aux        BYTEA                    NOT NULL,
  PRIMARY KEY (version, subversion)
) %TABLESPACE%;

ALTER TABLE %TABLE_NAME% OWNER TO %USER%;