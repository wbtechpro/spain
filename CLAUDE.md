# spain-map — памятка

Прод: https://dubson.org (VPS 82.202.129.14, Caddy + FastAPI systemd).
Репо: https://github.com/wbtechpro/spain (main защищён, push через PR).

## Правила

- **Прод = `origin/main`.** Никогда не правим файлы на VPS через ssh — следующий деплой сотрёт.
- **Любая работа — через feature-ветку + PR.** Напрямую в main запрещено (branch protection).
- **Merge в main → автодеплой** (GitHub Actions → `ops/deploy.sh` по ssh). ~60 сек до прода.
- **`data/parquet/` в `.gitignore`** — дериватив, пересобирается на деплое из `data/*.json`. Локально после клона: `uv run etl/to_parquet.py`.

## Добавить фичу

```bash
git switch -c feat/<short-name>
# ...правки...
uv run --project server uvicorn server.main:app --reload    # локальная проверка, если фронт/бэк
git commit -am "feat: описание"
git push -u origin feat/<short-name>
gh pr create --fill && gh pr merge --squash
```

## Обновить / добавить данные

```bash
git switch -c data/<описание>
uv run scripts/fetch_<что-то>.py       # → data/<что-то>.json
uv run etl/to_parquet.py               # проверка, что parquet соберётся
git add data/<что-то>.json
git commit -m "data: описание"
git push -u origin data/<описание>
gh pr create --fill && gh pr merge --squash
```

Parquet коммитить НЕ нужно — собирается на VPS автоматически.

## Параллельные сессии

Две Claude-сессии в одном каталоге **одновременно** → чужие правки попадут в твой коммит. Для параллели:

```bash
git worktree add ../spain-<name> <branch-name>   # отдельный рабочий каталог
# другая сессия работает в ../spain-<name>, main-каталог не трогает
git worktree remove ../spain-<name>              # когда закончили
```

## Conventional Commits

- `feat:` — новая фича
- `fix:` — баг
- `data:` — обновление данных
- `chore:` — инфра, зависимости
- `docs:` — только доки
- `refactor:` — рефакторинг без изменения поведения

## Структура

```
frontend не выделен — всё в index.html (будущий рефакторинг)
server/         FastAPI, storage.py работает с parquet через DuckDB
etl/            to_parquet.py — JSON → Parquet (derived, не коммитим)
scripts/        fetch_*.py — источник JSON (WIP, живёт тут исторически)
data/           *.json (source) + parquet/ (derived, gitignored)
ops/            Caddyfile, systemd unit, deploy.sh
.github/workflows/deploy.yml — автодеплой main → VPS
```

## Типичные ошибки

- Вручную правишь на VPS → сотрётся следующим деплоем.
- Забыл ветку, коммитишь в main → `git push` упадёт на branch protection, нужно `git switch -c ...`.
- Две сессии в одном каталоге без worktree → чужие правки смешиваются.
- Коммитишь `data/parquet/` → в `.gitignore`, не пройдёт.
- Force-push в main → запрещён protection, не сломаешь историю случайно.
