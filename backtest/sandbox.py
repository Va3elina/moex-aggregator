"""Песочница для стратегий на Python: отдельный контейнер bt-sandbox БЕЗ сети и БЕЗ секретов.

Зачем: код из редактора исполняется как есть. В контейнере воркера лежит DB_URL суперпользователя Postgres —
там чужой код запускать нельзя. Здесь нет ни сети (network_mode: none), ни переменных с паролями, корневая ФС только
для чтения, кэш свечей смонтирован только для чтения; единственное место записи — папка заданий BT_JOBS.

Обмен с воркером — файлами: воркер кладёт <id>.job.json, песочница отвечает <id>.result.json (запись через
переименование — читатель никогда не видит половину файла). Каждое задание — отдельный процесс с потолком памяти и
процессорного времени: зависший или прожорливый код убивается, цикл живёт дальше.

    python -m backtest.sandbox          # вечный цикл (контейнер)
    python -m backtest.sandbox --once   # обработать очередь и выйти (локальная проверка)
"""
import json, os, pathlib, resource, subprocess, sys, time

JOBS = pathlib.Path(os.environ.get('BT_JOBS', '/app/bt_jobs'))
CPU_SEC, MEM_BYTES, WALL_SEC = 600, 1_200_000_000, 900


def _limits():
    resource.setrlimit(resource.RLIMIT_CPU, (CPU_SEC, CPU_SEC))
    try: resource.setrlimit(resource.RLIMIT_AS, (MEM_BYTES, MEM_BYTES))
    except (ValueError, OSError): pass                       # macOS не даёт понизить AS — локально работаем без потолка
    resource.setrlimit(resource.RLIMIT_NOFILE, (256, 256))


def _write(path, obj):
    tmp = path.with_suffix('.tmp'); tmp.write_text(json.dumps(obj, ensure_ascii=False)); tmp.rename(path)


def run_job(job_file):
    """Исполняется в ДОЧЕРНЕМ процессе."""
    from . import pyengine
    job = json.loads(job_file.read_text()); rid = job['id']

    def progress(i, n): _write(JOBS / f'{rid}.progress.json', {'p': int(100 * i / n)})
    res = pyengine.run_code(job['code'], job.get('params'), job.get('universe'), job.get('since'), job.get('until'), progress)
    _write(JOBS / f'{rid}.result.json', res)


def loop(once=False):
    JOBS.mkdir(parents=True, exist_ok=True)
    print('bt-sandbox: старт, задания —', JOBS, flush=True)
    while True:
        jobs = sorted(JOBS.glob('*.job.json'), key=lambda f: f.stat().st_mtime)
        for f in jobs:
            rid = f.name.split('.')[0]; t0 = time.time()
            try:
                p = subprocess.run([sys.executable, '-m', 'backtest.sandbox', '--job', str(f)], preexec_fn=_limits,
                                   timeout=WALL_SEC, capture_output=True, text=True, env={k: v for k, v in os.environ.items() if k in ('PATH', 'BT_DATA', 'BT_JOBS', 'PYTHONPATH', 'HOME', 'LANG')})
                if not (JOBS / f'{rid}.result.json').exists():
                    why = 'превышен лимит памяти или процессорного времени' if p.returncode in (-9, -24, 137) else (p.stderr or 'процесс завершился без результата')[-1500:]
                    _write(JOBS / f'{rid}.result.json', {'error': why})
            except subprocess.TimeoutExpired:
                _write(JOBS / f'{rid}.result.json', {'error': f'стратегия считалась дольше {WALL_SEC} с — остановлена'})
            f.unlink(missing_ok=True)
            print(f'bt-sandbox: задание {rid} за {time.time() - t0:.1f} с', flush=True)
        if once: return
        time.sleep(1)


if __name__ == '__main__':
    if '--job' in sys.argv: run_job(pathlib.Path(sys.argv[sys.argv.index('--job') + 1]))
    else: loop(once='--once' in sys.argv)
