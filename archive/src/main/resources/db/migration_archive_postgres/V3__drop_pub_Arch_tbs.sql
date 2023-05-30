DO $$
BEGIN
	IF EXISTS (
	   SELECT 1
	   FROM information_schema.tables
	   WHERE table_schema = 'public'
	   AND table_name = 'workflow_archive'
	)
	THEN
	   IF EXISTS (
	   SELECT 1
	   FROM information_schema.tables
	   WHERE table_schema = 'archive'
	   AND table_name = 'workflow_archive'
	   )
	   THEN
--           RAISE NOTICE 'execute: DROP TABLE archive.workflow_archive;';
	      DROP TABLE archive.workflow_archive;
	   END IF;
--        RAISE NOTICE 'execute: ALTER TABLE public.workflow_archive SET SCHEMA archive;';
	   ALTER TABLE public.workflow_archive SET SCHEMA archive;
	ELSE
	   RAISE NOTICE 'Table public.workflow_archive does not exist.';
	END IF;

	IF EXISTS (
       SELECT 1
       FROM information_schema.tables
       WHERE table_schema = 'public'
       AND table_name = 'task_logs'
    )
    THEN
    	IF EXISTS (
    	SELECT 1
    	FROM information_schema.tables
    	WHERE table_schema = 'archive'
    	AND table_name = 'task_logs'
    	)
    	THEN
--           RAISE NOTICE 'execute: DROP TABLE archive.task_logs;';
    	   DROP TABLE archive.task_logs;
    	END IF;
--         RAISE NOTICE 'execute: ALTER TABLE public.task_logs SET SCHEMA archive;';
    	ALTER TABLE public.task_logs SET SCHEMA archive;
    	ELSE
    	   RAISE NOTICE 'Table public.task_logs does not exist.';
    	END IF;
END $$;

DO $$
BEGIN
    IF EXISTS(SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name = 'flyway_schema_history') THEN
        DELETE FROM public.flyway_schema_history WHERE version IN ('99', '100');
    END IF;
END $$;