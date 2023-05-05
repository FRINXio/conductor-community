DROP TABLE IF EXISTS public.workflow_archive, public.task_logs;

DELETE FROM public.flyway_schema_history WHERE version IN ('99', '100');