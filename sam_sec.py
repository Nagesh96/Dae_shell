import chardet

with open('<filename>', 'rb') as f:
    result = chardet.detect(f.read())
    encoding = result['encoding']
    print(encoding)
