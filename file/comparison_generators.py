import os
import time
import threading
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============= ИСХОДНАЯ ВЕРСИЯ (для сравнения) =============
def find_files_with_extension(directory, extension, recursive=True):
    directory = Path(directory)

    if recursive:
        for root, dirs, files in os.walk(directory):
            filtered_files = filter(lambda f: f.lower().endswith(extension.lower()), files)
            mapped_files = map(lambda f: Path(root) / f, filtered_files)
            for file_path in mapped_files:
                yield file_path
    else:
        filtered_files = filter(lambda f: f.lower().endswith(extension.lower()), os.listdir(directory))
        mapped_files = map(lambda f: directory / f, filtered_files)
        for file_path in mapped_files:
            yield file_path

def rename_files_with_counter(directory, extension, new_name_base="rename_file", recursive=True):
    renamed_files = []
    files_to_rename = list(find_files_with_extension(directory, extension, recursive))
    
    for counter, old_path in enumerate(files_to_rename, 1):
        new_name = f"{new_name_base}_{counter}{extension}"
        new_path = old_path.parent / new_name
        old_path.rename(new_path)
        renamed_files.append((old_path, new_path))
        print(f"Переименован: {old_path} -> {new_path}")
    
    return renamed_files

# ============= МНОГОПОТОЧНАЯ ВЕРСИЯ =============
def find_files_with_extension_parallel(directory, extension, recursive=True):
    """Многопоточный поиск файлов"""
    directory = Path(directory)
    files_found = []
    
    def walk_directory(root_path):
        for root, dirs, files in os.walk(root_path):
            for file in files:
                if file.lower().endswith(extension.lower()):
                    files_found.append(Path(root) / file)
    
    if recursive:
        walk_directory(directory)
    else:
        for file in os.listdir(directory):
            if file.lower().endswith(extension.lower()):
                files_found.append(directory / file)
    
    return files_found

def rename_single_file(args):
    """Функция для переименования одного файла (для многопоточности)"""
    old_path, counter, new_name_base, extension = args
    new_name = f"{new_name_base}_{counter}{extension}"
    new_path = old_path.parent / new_name
    
    try:
        old_path.rename(new_path)
        return (old_path, new_path, True)
    except Exception as e:
        return (old_path, None, False, str(e))

def rename_files_parallel(directory, extension, new_name_base="rename_file", recursive=True, max_workers=None):
    """Многопоточное переименование файлов"""
    # Находим файлы
    files_to_rename = find_files_with_extension_parallel(directory, extension, recursive)
    
    if not files_to_rename:
        return []
    
    # Подготавливаем аргументы для каждого файла
    tasks = [(file, idx, new_name_base, extension) for idx, file in enumerate(files_to_rename, 1)]
    
    renamed_files = []
    
    # Используем ThreadPoolExecutor для параллельного переименования
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(rename_single_file, task) for task in tasks]
        
        for future in as_completed(futures):
            result = future.result()
            if len(result) == 3:  # Успешное переименование
                old_path, new_path, success = result
                renamed_files.append((old_path, new_path))
                print(f"Переименован: {old_path} -> {new_path}")
            else:  # Ошибка
                old_path, _, success, error = result
                print(f"Ошибка при переименовании {old_path}: {error}")
    
    return renamed_files

# ============= ТЕСТИРОВАНИЕ ПРОИЗВОДИТЕЛЬНОСТИ =============
def create_test_files(count=50):
    """Создает тестовые файлы для тестирования"""
    print(f"\nСоздаю {count} тестовых файлов...")
    for i in range(count):
        filename = f"test_file_{i}.txt"
        with open(filename, 'w') as f:
            f.write(f"Тестовый файл {i}")
    print(f"Создано {count} файлов")

def cleanup_test_files():
    """Удаляет тестовые файлы"""
    for file in Path(".").glob("test_file_*.txt"):
        file.unlink()
    print("Тестовые файлы удалены")

def measure_performance(func, *args, **kwargs):
    """Измеряет время выполнения функции"""
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    return result, end_time - start_time

# ============= ДЕМОНСТРАЦИЯ =============
if __name__ == "__main__":
    print("\n" + "="*60)
    print("ТЕСТИРОВАНИЕ ПРОИЗВОДИТЕЛЬНОСТИ ГЕНЕРАТОРА ФАЙЛОВ")
    print("="*60)
    
    # Создаем тестовые файлы
    create_test_files()
    
    print("\n" + "-"*60)
    print("1. ПОИСК ФАЙЛОВ")
    print("-"*60)
    
    # Тест поиска файлов (оригинальная версия)
    print("\nОригинальная версия (генератор):")
    start = time.time()
    files_orig = list(find_files_with_extension(".", ".txt", recursive=True))
    time_orig = time.time() - start
    print(f"Найдено файлов: {len(files_orig)}")
    print(f"Время: {time_orig:.4f} секунд")
    
    # Тест поиска файлов (параллельная версия)
    print("\nПараллельная версия:")
    start = time.time()
    files_par = find_files_with_extension_parallel(".", ".txt", recursive=True)
    time_par = time.time() - start
    print(f"Найдено файлов: {len(files_par)}")
    print(f"Время: {time_par:.4f} секунд")
    
    if time_orig > 0:
        speedup = time_orig / time_par
        print(f"Ускорение: {speedup:.2f}x")
    
    print("\n" + "-"*60)
    print("2. ПЕРЕИМЕНОВАНИЕ ФАЙЛОВ")
    print("-"*60)
    
    # Восстанавливаем исходные имена файлов для чистоты теста
    for i, file in enumerate(files_orig):
        new_name = f"test_file_{i}.txt"
        try:
            file.rename(new_name)
        except:
            pass
    
    # Тест переименования (оригинальная версия)
    print("\nОригинальная версия (последовательная):")
    result_orig, time_orig_rename = measure_performance(
        rename_files_with_counter, ".", ".txt", "rename_orig", recursive=True
    )
    print(f"Переименовано файлов: {len(result_orig)}")
    print(f"Время: {time_orig_rename:.4f} секунд")
    
    # Восстанавливаем имена для следующего теста
    for old, new in result_orig:
        new.rename(old)
    
    # Тест переименования (параллельная версия)
    print("\nПараллельная версия (многопоточная):")
    result_par, time_par_rename = measure_performance(
        rename_files_parallel, ".", ".txt", "rename_par", recursive=True, max_workers=8
    )
    print(f"Переименовано файлов: {len(result_par)}")
    print(f"Время: {time_par_rename:.4f} секунд")
    
    if time_orig_rename > 0:
        speedup_rename = time_orig_rename / time_par_rename
        print(f"Ускорение: {speedup_rename:.2f}x")
    
    print("\n" + "="*60)
    print("ВЫВОДЫ:")
    print("="*60)
    print(f"Поиск файлов: {time_orig:.4f}с -> {time_par:.4f}с (ускорение в {time_orig/time_par:.2f} раз)")
    print(f"Переименование: {time_orig_rename:.4f}с -> {time_par_rename:.4f}с (ускорение в {time_orig_rename/time_par_rename:.2f} раз)")
    
    # Очистка
    cleanup_test_files()
    
    print("\n" + "="*60)
    print("ДЕМОНСТРАЦИЯ РАБОТЫ С ВАШИМИ ФАЙЛАМИ")
    print("="*60)
    
    # Поиск существующих .txt файлов
    print("\nПоиск .txt файлов в текущей папке:")
    for file_path in find_files_with_extension(".", ".txt", recursive=True):
        print(f"    {file_path}")
    
    # Параллельное переименование (если есть файлы)
    txt_files = list(find_files_with_extension(".", ".txt", recursive=True))
    if txt_files:
        print(f"\nНайдено {len(txt_files)} .txt файлов")
        print("\nВыполняю параллельное переименование...")
        renamed = rename_files_parallel(".", ".txt", "my_new_name", recursive=True)
        print(f"\nПереименовано файлов: {len(renamed)}")
    else:
        print("\nНет .txt файлов для переименования")
    
    print("\n" + "="*60)