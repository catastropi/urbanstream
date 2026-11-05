-- 013: Step 9. API 서비스용 로그인 역할. 결정 근거: docs/adr/0011-step9-api.md (D9-3)
--
-- 새 표·뷰가 없으므로 새 GRANT 도 없다: api_service 는 api_reader 를 상속한다.
--   api_reader = dim/fact/mart 표·뷰 SELECT + ops **뷰만** SELECT (005, 006, 007, 009, 010, 011). 샌드박스에서 ops 원본 표 7개 거부를 확인했다.
-- 비밀번호는 여기 두지 않는다(파일·체크섬·git 에 남는다). scripts/db_bootstrap.py --api 가 .env 의 API_DB_PASSWORD 로 LOGIN 을 켠다.
--   NOLOGIN 으로 만들기 때문에 부트스트랩 전에는 누구도 이 역할로 접속하지 못한다(안전한 쪽으로 실패).
-- 역할 설정은 로그인할 때만 적용된다(SET ROLE 에는 적용되지 않음). API 앱은 같은 값을 연결 옵션으로도 직접 건다.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_service') THEN
    CREATE ROLE api_service NOLOGIN IN ROLE api_reader;
  END IF;
END $$;

ALTER ROLE api_service CONNECTION LIMIT 8;
ALTER ROLE api_service SET statement_timeout = '5s';
ALTER ROLE api_service SET lock_timeout = '1s';
ALTER ROLE api_service SET idle_in_transaction_session_timeout = '10s';
ALTER ROLE api_service SET default_transaction_read_only = on;

COMMENT ON ROLE api_service IS 'Step 9 FastAPI 서비스 로그인 역할(읽기 전용, api_reader 상속). 비밀번호는 db_bootstrap.py --api 가 .env 값으로 설정.';
