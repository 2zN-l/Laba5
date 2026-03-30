import os
from pathlib import Path

def find_files_with_extension(directory, extension, recursive=True):
    directory = Path(directory)

    if recursive:

        for root, dirs, files in os.walk(directory):
            filtered_files = filter(lambda f: f.lower().endswith(extension.lower()), files)
            mapped_files = map(lambda f: Path(root) / f, filtered_files)

            for file_path in mapped_files:
                yield file_path
                
    else:
        filtered_files = filter(lambda f: f.lower().endswith(extension.lower()),os.listdir(directory))

        mapped_files = map(lambda f: directory / f,filtered_files)
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





print("\n" + "-"*50 + "\n")


print("Найденные файлы:")
for file_path in find_files_with_extension(".", ".txt", recursive=True):
    print(f"    {file_path}")
    
    
print("\n" + "-"*50 + "\n")


print("Переименование файлов:")
renamed = rename_files_with_counter(".", ".txt", "test", recursive=True)
    
