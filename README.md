# Bybit Volume Spike Bot

Моніторить USDT-перпетуальні ф'ючерси на Bybit і шле алерти в Telegram при різкому
зростанні об'єму торгів на 1-хвилинному таймфреймі.

## Як це працює

- Бере топ-N пар за абсолютною зміною ціни за 24г через REST API Bybit
  (з фільтром за мінімальним оборотом, щоб відсіяти неліквідні пари).
- Підписується на потік `kline.1.<symbol>` через публічний WebSocket (без ключів API).
- Для кожної закритої 1хв-свічки рахує середній об'єм за попередні N свічок.
- Якщо поточний об'єм перевищує середнє у X разів — шле алерт у Telegram.
- При розриві з'єднання перепідключається автоматично.

## Локальний запуск (для тесту)

```bash
python -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env
# заповнити TELEGRAM_BOT_TOKEN і TELEGRAM_CHAT_ID у .env

export $(cat .env | xargs)   # Linux/Mac; на Windows задати змінні вручну
python bot.py
```

## Як отримати Telegram-токен і chat_id

1. Написати @BotFather → `/newbot` → отримати `TELEGRAM_BOT_TOKEN`.
2. Написати своєму новому боту будь-яке повідомлення.
3. Написати @userinfobot (або зайти на `https://api.telegram.org/bot<TOKEN>/getUpdates`
   після кроку 2) — знайти там свій `chat_id`.

## Деплой на Fly.io (безкоштовний варіант)

```bash
# встановити flyctl: https://fly.io/docs/flyctl/install/
fly launch --no-deploy        # створить fly.toml, відповісти "ні" на автогенерацію
fly secrets set TELEGRAM_BOT_TOKEN=xxx TELEGRAM_CHAT_ID=xxx
fly deploy
```

Fly.io деплоїть безпосередньо з Dockerfile у цій папці — окремо нічого готувати не треба.

## Деплой на Oracle Cloud Free Tier VM (постійно безкоштовний варіант)

1. Створити безкоштовну ARM-інстанцію (Always Free) в Oracle Cloud Console.
2. Підключитись по SSH, встановити Docker.
3. Скопіювати файли проєкту на сервер (`git clone` свого репозиторію).
4. Запустити:
   ```bash
   docker build -t volume-bot .
   docker run -d --restart unless-stopped \
     -e TELEGRAM_BOT_TOKEN=xxx -e TELEGRAM_CHAT_ID=xxx \
     --name volume-bot volume-bot
   ```
   Прапорець `--restart unless-stopped` — це і є автоматичний рестарт при падінні
   чи перезавантаженні сервера.

## Деплой через Railway (найпростіший git-flow)

1. Запушити ці файли у свій GitHub-репозиторій.
2. На railway.app → New Project → Deploy from GitHub repo.
3. Додати змінні середовища `TELEGRAM_BOT_TOKEN` і `TELEGRAM_CHAT_ID` у Settings → Variables.
4. Railway сам збере образ за Dockerfile і задеплоїть; кожен наступний `git push` —
   автоматичний редеплой.

## Налаштування чутливості

Все керується змінними середовища (див. `.env.example`):

- `VOLUME_MULTIPLIER` — у скільки разів об'єм має перевищити середнє (за замовчуванням 5x).
  Менше значення = більше сигналів, у т.ч. хибних.
- `ROLLING_WINDOW` — скільки попередніх хвилин враховувати для середнього (за замовчуванням 10).
- `MIN_VOLUME_USDT` — фільтр за мінімальним оборотом, щоб відсіяти мертві пари.
- `TOP_N_PAIRS` — скільки пар моніторити, обираються за найбільшою зміною ціни за 24г
  (за замовчуванням 50; більше пар = більше WS-з'єднань і навантаження).
- `ALERT_COOLDOWN_SEC` — мінімальний інтервал між повторними алертами по одній парі.

## Обмеження поточної версії

- Немає збереження стану між рестартами (історія об'ємів накопичується заново) —
  для 24/7-хостингу це не проблема, для нестабільних безкоштовних тарифів з частими
  рестартами варто додати Redis.
- Алгоритм детекції простий (проста середня + поріг) — легко замінити на z-score
  чи інший статистичний метод у функції `handle_kline_message`.
