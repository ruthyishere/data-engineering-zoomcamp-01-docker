from pathlib import Path

current_directory = Path.cwd() #Same as Path('.').resolve()
current_file = Path(__file__).name # __file__ gives the filename of the current script

print(f"Files in {current_directory}: ")

for filepath in current_directory.iterdir():
    if filepath.name != current_file:
        print(f"- {filepath.name}")
        if filepath.is_file():
            content = filepath.read_text(encoding='utf-8')
            print(f"  Content:{content}")
