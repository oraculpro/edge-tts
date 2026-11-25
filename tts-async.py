import edge_tts
import asyncio
import os
import random

# ================== НАСТРОЙКИ ==================
VOICE = "ru-RU-SvetlanaNeural"
OUTPUT_DIR = "Вывод_Аудио"
RATE = "+0%"
FINAL_FILENAME = "Аудио_Целиком.mp3"
MAX_CHUNK_SIZE = 2500

MAX_RETRIES = 4          # сколько попыток на один чанк
RETRY_BASE_DELAY = 1.0   # базовая задержка retry (сек), растёт экспоненциально
MAX_CONCURRENT = 5       # ограничение одновременных запросов (не бомбим сервер)
# ===============================================

os.makedirs(OUTPUT_DIR, exist_ok=True)


def split_text(text, max_size=MAX_CHUNK_SIZE):
    chunks = []
    current_chunk = ""
    paragraphs = text.split('\n\n')

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        if len(para) > max_size:
            sentences = para.replace('!', '.!').replace('?', '.?').split('. ')
            for sent in sentences:
                sent = sent.strip()
                if not sent:
                    continue
                if len(current_chunk) + len(sent) + 2 < max_size:
                    current_chunk += sent + '. '
                else:
                    if current_chunk:
                        chunks.append(current_chunk.strip())
                    current_chunk = sent + '. '
        else:
            if len(current_chunk) + len(para) + 2 < max_size:
                current_chunk += para + '\n\n'
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = para + '\n\n'

    if current_chunk:
        chunks.append(current_chunk.strip())

    return chunks


async def generate_audio_with_retry(text, filename, semaphore):
    """Генерация одного чанка с retry и экспоненциальным backoff."""
    if not text.strip():
        return True

    path = os.path.join(OUTPUT_DIR, filename)

    async with semaphore:  # ограничиваем одновременные запросы
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                communicate = edge_tts.Communicate(text, VOICE, rate=RATE)
                await communicate.save(path)

                # Проверяем, что файл реально создан и не пустой
                if os.path.exists(path) and os.path.getsize(path) > 0:
                    print(f"✓ {filename} (попытка {attempt})")
                    return True
                raise RuntimeError("Пустой файл после генерации")

            except Exception as e:
                delay = RETRY_BASE_DELAY * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                print(f"✗ {filename} попытка {attempt}/{MAX_RETRIES}: {e}. Ждём {delay:.1f}с")
                if attempt < MAX_RETRIES:
                    await asyncio.sleep(delay)
                else:
                    print(f"‼ {filename} НЕ СГЕНЕРИРОВАН после {MAX_RETRIES} попыток")
                    return False


def merge_mp3_files(input_dir, output_file, expected_count):
    """Склеивает mp3. Возвращает True, если все чанки на месте."""
    print(f"\nСклеиваю файлы в {output_file}...")
    mp3_files = sorted(
        f for f in os.listdir(input_dir)
        if f.startswith('chunk_') and f.endswith('.mp3')
    )

    if len(mp3_files) != expected_count:
        print(f"⚠ Найдено {len(mp3_files)} из {expected_count} чанков — склейка будет неполной!")

    with open(output_file, 'wb') as outfile:
        for mp3_file in mp3_files:
            with open(os.path.join(input_dir, mp3_file), 'rb') as infile:
                outfile.write(infile.read())

    print(f"✓ Файл сохранён: {output_file}")
    return len(mp3_files) == expected_count


async def main():
    input_file = "text.txt"
    if not os.path.exists(input_file):
        print(f"❌ Файл {input_file} не найден!")
        return

    with open(input_file, 'r', encoding='utf-8') as f:
        full_text = f.read()

    print(f"Прочитан текст: {len(full_text)} символов")

    chunks = split_text(full_text)
    print(f"Разбито на {len(chunks)} частей")

    # --- ПАРАЛЛЕЛЬНЫЙ ЗАПУСК ---
    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
    tasks = [
        generate_audio_with_retry(chunk, f"chunk_{i:03d}.mp3", semaphore)
        for i, chunk in enumerate(chunks)
    ]

    # gather ждёт ВСЕ задачи, не падая при ошибке одной (return_exceptions=True)
    results = await asyncio.gather(*tasks, return_exceptions=True)

    ok = sum(1 for r in results if r is True)
    failed = [i for i, r in enumerate(results) if r is not True]
    print(f"\nГотово чанков: {ok}/{len(chunks)}")

    if failed:
        print(f"⚠ Не удалось сгенерировать: {failed}")
        print("Склеиваю то, что есть...")

    # --- СКЛЕЙКА ---
    final_path = os.path.join(OUTPUT_DIR, FINAL_FILENAME)
    complete = merge_mp3_files(OUTPUT_DIR, final_path, len(chunks))

    # Удаляем временные только если всё успешно
    if complete:
        print("\nУдаляю временные файлы...")
        for f in os.listdir(OUTPUT_DIR):
            if f.startswith('chunk_') and f.endswith('.mp3'):
                os.remove(os.path.join(OUTPUT_DIR, f))

    print(f"\n✓ Итоговый файл: {final_path}")


if __name__ == "__main__":
    asyncio.run(main())