from sys import argv
from os import system

month, chunck, lang, output_dir = argv[1], argv[2], argv[3], argv[4]

system(f"wget -q https://commoncrawl.s3.amazonaws.com/cc-index/collections/CC-MAIN-{month}/indexes/cdx-00{chunck}.gz")
system(f"gunzip cdx-00{chunck}")

f = open(f"cdx-00{chunck}")

while True: 
    line = f.readline()

    if '"languages": "' + lang + '"' in line:
        k = open(output_dir + 'ka.txt', 'a')
        k.close()

    if not line: 
        break
        
f.close()

system(f"rm cdx-00{chunck}")
