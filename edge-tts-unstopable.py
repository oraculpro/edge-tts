import edge_tts
import asyncio
import os
import random
import time
import sys
import traceback

# ================== НАСТРОЙКИ ==================
VOICE = "ru-RU-SvetlanaNeural"
OUTPUT_DIR = "Вывод_аудио_unstopable"
RATE = "+0%"
FINAL_FILENAME = "Audio_Целиком.mp3"

MAX_CHUNK_SIZE = 800            # маленькие чанки
DELAY_BETWEEN_SUCCESS = 8       # пауза между успешными запросами, сек

# Backoff при ошибке: 1 -> 2 -> 4 -> 8 -> 10 -> 10 ... минут
RETRY_DELAYS_MIN = [1, 2, 4, 8, 10]
RETRY_JITTER_SEC = 15

# Сколько ждать, если упало что-то совсем неожиданное (не сеть, а баг)
FATAL_RETRY_WAIT_SEC = 60
# ===============================================

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ---------- разбиение ----------
def split_text(text, max_size=MAX_CHUNK_SIZE):
    chunks, current = [], ""

    def flush():
        nonlocal current
        if current.strip():
            chunks.append(current.strip())
        current = ""

    for para in text.split('\n\n'):
        para = para.strip()
        if not para:
            continue

        sentences, buf = [], ""
        for ch in para:
            buf += ch
            if ch in ".!?…" and buf.strip():
                sentences.append(buf.strip())
                buf = ""
        if buf.strip():
            sentences.append(buf.strip())

        for sent in sentences:
            if len(sent) > max_size:
                for w in sent.split():
                    if len(current) + len(w) + 1 > max_size:
                        flush()
                    current += w + " "
                continue
            if len(current) + len(sent) + 1 > max_size:
                flush()
            current += sent + " "

    flush()
    return chunks


def backoff_minutes(attempt_index):
    idx = min(attempt_index, len(RETRY_DELAYS_MIN) - 1)
    return RETRY_DELAYS_MIN[idx]


def chunk_is_ready(path):
    return os.path.exists(path) and os.path.getsize(path) > 0


# ---------- генерация одного чанка: бесконечные попытки ----------
async def generate_one_chunk_forever(text, filename):
    """Пытается сгенерировать чанк, пока не получится. Никогда не сдаётся."""
    path = os.path.join(OUTPUT_DIR, filename)
    attempt = 0

    while True:
        attempt += 1
        try:
            print(f"→ {filename} попытка {attempt}...", flush=True)
            communicate = edge_tts.Communicate(text, VOICE, rate=RATE)
            await communicate.save(path)

            if not chunk_is_ready(path):
                raise RuntimeError("файл пустой после генерации")

            print(f"✓ {filename} готов (попытка {attempt}, "
                  f"{os.path.getsize(path)} байт)", flush=True)
            return  # единственный выход — успех

        except Exception as e:
            wait_min = backoff_minutes(attempt - 1)
            wait_sec = max(
                5,
                wait_min * 60 + random.uniform(-RETRY_JITTER_SEC, RETRY_JITTER_SEC)
            )
            print(f"✗ {filename} попытка {attempt} упала: "
                  f"{type(e).__name__}: {e}", flush=True)
            print(f"  жду {wait_sec/60:.1f} мин и повторяю...", flush=True)
            await asyncio.sleep(wait_sec)


# ---------- склейка ----------
def merge_mp3_files(input_dir, output_file, expected_count):
    print(f"\nСклеиваю файлы в {output_file}...", flush=True)
    mp3_files = sorted(
        f for f in os.listdir(input_dir)
        if f.startswith('chunk_') and f.endswith('.mp3')
    )
    if len(mp3_files) != expected_count:
        print(f"⚠ Найдено {len(mp3_files)} из {expected_count} чанков!", flush=True)

    with open(output_file, 'wb') as outfile:
        for mp3_file in mp3_files:
            with open(os.path.join(input_dir, mp3_file), 'rb') as infile:
                outfile.write(infile.read())

    print(f"✓ Файл сохранён: {output_file}", flush=True)
    return len(mp3_files) == expected_count


# ---------- основной цикл с самовосстановлением ----------
async def run_all_chunks(chunks, total):
    for i, chunk in enumerate(chunks, 1):
        filename = f"chunk_{i-1:03d}.mp3"
        path = os.path.join(OUTPUT_DIR, filename)

        if chunk_is_ready(path):
            print(f"[{i}/{total}] {filename} уже готов — пропускаю", flush=True)
            continue

        print(f"\n[{i}/{total}] {filename} — {len(chunk)} символов", flush=True)
        await generate_one_chunk_forever(chunk, filename)

        if i < total:
            await asyncio.sleep(DELAY_BETWEEN_SUCCESS)


async def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_file = os.path.join(script_dir, "text.txt")

    if not os.path.exists(input_file):
        print(f"❌ Файл {input_file} не найден!")
        return

    with open(input_file, 'r', encoding='utf-8') as f:
        full_text = f.read()

    print(f"Прочитан текст: {len(full_text)} символов")
    chunks = split_text(full_text)
    total = len(chunks)
    print(f"Разбито на {total} частей (по ≤{MAX_CHUNK_SIZE} символов)")
    print(f"Последовательный режим, пауза между успехами {DELAY_BETWEEN_SUCCESS}с")
    print(f"Backoff при ошибке: {RETRY_DELAYS_MIN} минут, без верхней границы попыток\n")

    # Внешний слой самовосстановления: даже если run_all_chunks упадёт
    # (например, случайный баг в библиотеке), ждём и продолжаем с того же места.
    while True:
        try:
            await run_all_chunks(chunks, total)
            break  # дошли до конца — выходим
        except KeyboardInterrupt:
            print("\n⏸ Прервано пользователем. Прогресс сохранён на диск.")
            print("  При следующем запуске готовые чанки будут пропущены.")
            return
        except Exception:
            print("\n‼ Непредвиденная ошибка верхнего уровня:", flush=True)
            traceback.print_exc()
            print(f"  жду {FATAL_RETRY_WAIT_SEC}с и продолжаю с того же места...",
                  flush=True)
            await asyncio.sleep(FATAL_RETRY_WAIT_SEC)

    # Все чанки на месте — склеиваем
    final_path = os.path.join(OUTPUT_DIR, FINAL_FILENAME)
    complete = merge_mp3_files(OUTPUT_DIR, final_path, total)

    if complete:
        print("\nУдаляю временные файлы...", flush=True)
        for f in os.listdir(OUTPUT_DIR):
            if f.startswith('chunk_') and f.endswith('.mp3'):
                os.remove(os.path.join(OUTPUT_DIR, f))
        print(f"\n✓ Готово: {final_path}", flush=True)
    else:
        # По логике сюда попасть не должны (цикл не выйдет, пока все чанки
        # не готовы), но на всякий случай не удаляем временные.
        print(f"\n⚠ Склейка неполная: {final_path}", flush=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n⏸ Остановлено пользователем.")
        sys.exit(0)