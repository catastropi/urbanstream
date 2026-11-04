-- 005: 최소 권한 역할. 로그인 비밀번호는 여기서 만들지 않는다 → 운영자가 ALTER ROLE … PASSWORD 로 지정 (runbook).
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='gold_writer') THEN CREATE ROLE gold_writer NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='api_reader')  THEN CREATE ROLE api_reader  NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='bi_reader')   THEN CREATE ROLE bi_reader   NOLOGIN; END IF;
END $$;

GRANT USAGE ON SCHEMA dim, fact, mart, ops TO gold_writer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dim, fact, ops TO gold_writer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA ops TO gold_writer;
GRANT EXECUTE ON FUNCTION mart.refresh_all() TO gold_writer;

GRANT USAGE ON SCHEMA dim, fact, mart TO api_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA dim, fact, mart TO api_reader;

GRANT USAGE ON SCHEMA mart TO bi_reader;        -- BI 는 mart 만 (fact 직접 접근 없음)
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO bi_reader;
GRANT SELECT ON dim.dim_link, dim.dim_date TO bi_reader;
GRANT USAGE ON SCHEMA dim TO bi_reader;
