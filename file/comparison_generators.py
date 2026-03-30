import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import time
from functools import partial

def find_files_with_extension(directory, extension, recursive=True):
    """Оригинальная версия генератора"""
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


def find_files_parallel_thread(directory, extension, recursive=True, max_workers=None):
    """
    Многопоточная версия генератора (для I/O-bound операций)
    Использует ThreadPoolExecutor для параллельного обхода папок
    """
    directory = Path(directory)
    
    def process_directory(root_dir):
        """Обработка одной директории"""
        result = []
        try:
            for root, dirs, files in os.walk(root_dir):
                # Фильтруем файлы по расширению
                filtered_files = filter(
                    lambda f: f.lower().endswith(extension.lower()), 
                    files
                )
                # Преобразуем в полные пути
                mapped_files = [Path(root) / f for f in filtered_files]
                result.extend(mapped_files)
        except Exception as e:
            print(f"Ошибка при обработке {root_dir}: {e}")
        return result
    
    # Получаем список поддиректорий для параллельной обработки
    if recursive:
        try:
            # Собираем все поддиректории
            subdirs = []
            for root, dirs, files in os.walk(directory):
                subdirs.append(Path(root))
            
            # Параллельно обрабатываем директории
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(executor.map(process_directory, subdirs))
            
            # Возвращаем результаты
            for result in results:
                for file_path in result:
                    yield file_path
        except Exception as e:
            print(f"Ошибка при многопоточном обходе: {e}")
    else:
        # Для одной директории используем обычный подход
        yield from find_files_with_extension(directory, extension, False)


def find_files_parallel_process(directory, extension, recursive=True, max_workers=None):
    """
    Многопроцессорная версия генератора (для CPU-bound операций)
    Использует ProcessPoolExecutor для параллельной обработки
    """
    directory = Path(directory)
    
    def process_chunk(chunk_dirs):
        """Обработка группы директорий"""
        results = []
        for root_dir in chunk_dirs:
            try:
                for root, dirs, files in os.walk(root_dir):
                    filtered_files = filter(
                        lambda f: f.lower().endswith(extension.lower()), 
                        files
                    )
                    mapped_files = [Path(root) / f for f in filtered_files]
                    results.extend(mapped_files)
            except Exception as e:
                print(f"Ошибка при обработке {root_dir}: {e}")
        return results
    
    if recursive:
        try:
            # Собираем все поддиректории
            subdirs = []
            for root, dirs, files in os.walk(directory):
                subdirs.append(Path(root))
            
            # Разбиваем директории на чанки для процессоров
            chunk_size = max(1, len(subdirs) // (max_workers or os.cpu_count()))
            chunks = [subdirs[i:i + chunk_size] for i in range(0, len(subdirs), chunk_size)]
            
            # Параллельно обрабатываем чанки
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                results = list(executor.map(process_chunk, chunks))
            
            # Возвращаем результаты
            for result in results:
                for file_path in result:
                    yield file_path
        except Exception as e:
            print(f"Ошибка при многопроцессорном обходе: {e}")
    else:
        yield from find_files_with_extension(directory, extension, False)


def rename_files_with_counter_parallel(directory, extension, new_name_base="rename_file", 
                                      recursive=True, parallel_type="thread", max_workers=None):
    """
    Параллельная версия переименования файлов
    """
    renamed_files = []
    
    # Выбираем подходящий генератор
    if parallel_type == "thread":
        find_func = find_files_parallel_thread
    elif parallel_type == "process":
        find_func = find_files_parallel_process
    else:
        find_func = find_files_with_extension
    
    # Получаем файлы через параллельный генератор
    files_to_rename = list(find_func(directory, extension, recursive, max_workers))
    
    # Переименовываем файлы
    for counter, old_path in enumerate(files_to_rename, 1):
        new_name = f"{new_name_base}_{counter}{extension}"
        new_path = old_path.parent / new_name
        old_path.rename(new_path)
        renamed_files.append((old_path, new_path))
        print(f"Переименован: {old_path} -> {new_path}")
    
    return renamed_files


# ===================== ТЕСТИРОВАНИЕ ПРОИЗВОДИТЕЛЬНОСТИ =====================

def create_test_structure(base_path="test_structure", num_dirs=10, num_files_per_dir=100):
    """Создает тестовую структуру для измерения производительности"""
    base = Path(base_path)
    
    # Удаляем старую тестовую структуру
    if base.exists():
        import shutil
        shutil.rmtree(base)
    
    base.mkdir(exist_ok=True)
    
    print(f"Создание тестовой структуры: {num_dirs} папок, {num_files_per_dir} файлов в каждой...")
    
    for i in range(num_dirs):
        subdir = base / f"folder_{i}"
        subdir.mkdir(exist_ok=True)
        
        for j in range(num_files_per_dir):
            file_path = subdir / f"file_{j}.txt"
            file_path.write_text(f"Тестовый файл {j}\n" * 10)  # ~1KB на файл
    
    total_files = num_dirs * num_files_per_dir
    print(f"Создано {total_files} тестовых файлов")
    return total_files


def measure_performance(func, *args, **kwargs):
    """Измеряет время выполнения функции"""
    start_time = time.time()
    result = list(func(*args, **kwargs))
    end_time = time.time()
    return len(result), end_time - start_time


def performance_test():
    """Сравнение производительности различных версий"""
    print("\n" + "="*70)
    print(" ТЕСТИРОВАНИЕ ПРОИЗВОДИТЕЛЬНОСТИ")
    print("="*70)
    
    # Создаем тестовую структуру
    total_files = create_test_structure("perf_test", num_dirs=20, num_files_per_dir=50)
    
    print("\n" + "-"*70)
    print("Тест 1: Поиск файлов (обход файловой системы)")
    print("-"*70)
    
    # 1. Обычная версия
    print("\n1. Обычная версия (генератор):")
    count, duration = measure_performance(
        lambda: find_files_with_extension("perf_test", ".txt", recursive=True)
    )
    print(f"   Найдено файлов: {count}")
    print(f"   Время выполнения: {duration:.4f} секунд")
    original_time = duration
    
    # 2. Многопоточная версия
    print("\n2. Многопоточная версия (ThreadPoolExecutor):")
    count, duration = measure_performance(
        lambda: find_files_parallel_thread("perf_test", ".txt", recursive=True, max_workers=4)
    )
    print(f"   Найдено файлов: {count}")
    print(f"   Время выполнения: {duration:.4f} секунд")
    print(f"   Ускорение: {original_time/duration:.2f}x")
    
    # 3. Многопроцессорная версия
    print("\n3. Многопроцессорная версия (ProcessPoolExecutor):")
    count, duration = measure_performance(
        lambda: find_files_parallel_process("perf_test", ".txt", recursive=True, max_workers=4)
    )
    print(f"   Найдено файлов: {count}")
    print(f"   Время выполнения: {duration:.4f} секунд")
    print(f"   Ускорение: {original_time/duration:.2f}x")
    
    # Тест переименования
    print("\n" + "-"*70)
    print("Тест 2: Переименование файлов")
    print("-"*70)
    
    # Создаем свежую тестовую структуру для переименования
    create_test_structure("perf_test_rename", num_dirs=10, num_files_per_dir=30)
    
    # Обычное переименование
    print("\n1. Обычное переименование:")
    start = time.time()
    renamed = rename_files_with_counter("perf_test_rename", ".txt", "file", recursive=True)
    duration = time.time() - start
    print(f"   Переименовано файлов: {len(renamed)}")
    print(f"   Время выполнения: {duration:.4f} секунд")
    original_rename_time = duration
    
    # Создаем свежую тестовую структуру для параллельного переименования
    create_test_structure("perf_test_rename_parallel", num_dirs=10, num_files_per_dir=30)
    
    # Параллельное переименование
    print("\n2. Параллельное переименование (многопоточное):")
    start = time.time()
    renamed = rename_files_with_counter_parallel(
        "perf_test_rename_parallel", ".txt", "file", 
        recursive=True, parallel_type="thread", max_workers=4
    )
    duration = time.time() - start
    print(f"   Переименовано файлов: {len(renamed)}")
    print(f"   Время выполнения: {duration:.4f} секунд")
    print(f"   Ускорение: {original_rename_time/duration:.2f}x")
    
    # Очистка
    import shutil
    shutil.rmtree("perf_test", ignore_errors=True)
    shutil.rmtree("perf_test_rename", ignore_errors=True)
    shutil.rmtree("perf_test_rename_parallel", ignore_errors=True)
    
    print("\n" + "="*70)
    print(" ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("="*70)


def demonstration():
    """Демонстрация работы параллельных версий"""
    # Создаем простую тестовую структуру
    test_dir = Path("demo_test")
    test_dir.mkdir(exist_ok=True)
    
    for i in range(5):
        subdir = test_dir / f"subdir_{i}"
        subdir.mkdir(exist_ok=True)
        for j in range(3):
            (subdir / f"file_{j}.txt").write_text(f"Content {j}")
    
    print("\n" + "="*70)
    print(" ДЕМОНСТРАЦИЯ РАБОТЫ ПАРАЛЛЕЛЬНЫХ ВЕРСИЙ")
    print("="*70)
    
    print("\n1. Многопоточный поиск файлов:")
    print("-" * 50)
    for file_path in find_files_parallel_thread("demo_test", ".txt", recursive=True):
        print(f"   {file_path}")
    
    print("\n2. Многопроцессорный поиск файлов:")
    print("-" * 50)
    for file_path in find_files_parallel_process("demo_test", ".txt", recursive=True):
        print(f"   {file_path}")
    
    print("\n3. Параллельное переименование (многопоточное):")
    print("-" * 50)
    renamed = rename_files_with_counter_parallel(
        "demo_test", ".txt", "demo_file", 
        recursive=True, parallel_type="thread"
    )
    
    print("\n4. Результат после переименования:")
    print("-" * 50)
    for file_path in find_files_with_extension("demo_test", ".txt", recursive=True):
        print(f"   {file_path}")
    
    # Очистка
    import shutil
    shutil.rmtree("demo_test", ignore_errors=True)


# ===================== ОСНОВНАЯ ПРОГРАММА =====================

if __name__ == "__main__":
    # Оригинальный код с демонстрацией
    print("\n" + "-"*50 + "\n")
    print("Найденные файлы (оригинальная версия):")
    for file_path in find_files_with_extension(".", ".txt", recursive=True):
        print(f"    {file_path}")
    
    print("\n" + "-"*50 + "\n")
    print("Переименование файлов (оригинальная версия):")
    renamed = rename_files_with_counter(".", ".txt", "test", recursive=True)
    
    print("\n" + "="*70)
    print(" ЗАПУСК ТЕСТА ПРОИЗВОДИТЕЛЬНОСТИ")
    print("="*70)
    
    # Запускаем тест производительности
    try:
        performance_test()
    except Exception as e:
        print(f"Ошибка при тестировании: {e}")
    
    # Демонстрация работы
    print("\n")
    demonstration()