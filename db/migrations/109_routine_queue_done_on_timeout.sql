-- 109: задания общей очереди рутин без колбэка-«готово».
--
-- Аудит второго мозга (frame-brain-audit) отвечает много раз — вердикт за вердиктом, кругами по 40,
-- финального «готово» у него нет. Его задание закрывается по истечении timeout_sec как выполненное,
-- а не как сбой: done_on_timeout = true. Остальные рутины (посты, справки) закрываются колбэком.
ALTER TABLE routine_queue ADD COLUMN IF NOT EXISTS done_on_timeout BOOLEAN NOT NULL DEFAULT false;
