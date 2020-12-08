from glob import glob
from tqdm.notebook import tqdm
from datetime import datetime
import json
from os import system
import argparse
import subprocess

def collect(continue_with_file=None):

    index_file_batch_path_ = glob('/content/drive/MyDrive/commoncrowl/aze*')[0] if not continue_with_file else '/content/drive/MyDrive/commoncrowl/__{}.tar.gz'.format(continue_with_file)
    index_file_batch_path  = index_file_batch_path_.replace('commoncrowl/aze', 'commoncrowl/__aze').replace('commoncrowl/arm', 'commoncrowl/__arm')
    if not continue_with_file:
        system("mv {} {}".format(index_file_batch_path_, index_file_batch_path))
    
    batch_name = index_file_batch_path_.split('/')[-1].replace('.tar.gz', '')
    batch_name = batch_name.replace('__', '')
    print(batch_name)

    system("cp {} /content/index_file_batch.tar.gz".format(index_file_batch_path))
    system("tar -xf /content/index_file_batch.tar.gz -C /content/indexes")
    system("rm /content/index_file_batch.tar.gz")

    system("cat /content/indexes/* > /content/indexes.txt")
    system("rm /content/indexes/*")

    lines_lenght_ = subprocess.getoutput("wc -l /content/indexes.txt")
    lines_lenght = int(lines_lenght_.split(' ')[0])

    start_date = datetime.now()

    index_file = open('/content/indexes.txt', 'r')

    existing = [int(a.replace('.tar.gz', '').split('-')[-1]) for a in glob('/content/drive/MyDrive/commoncrowl-data/{}*'.format(batch_name))]
    start_from = 0 if len(existing) == 0 else max(existing)

    print('starting from', start_from, 'of', lines_lenght)

    for line_index in range(lines_lenght):
        if line_index < start_from + 1:
            continue
            
        line = index_file.readline()

        index = json.loads('{"url":' + line.split('{"url":')[1])

        system("curl -s -r{}-$(({}+{}-1)) https://commoncrawl.s3.amazonaws.com/{} > /content/tmp.warc.gz".format(index["offset"], index["offset"], index["length"], index["filename"])) 
        system("warcio extract --payload /content/tmp.warc.gz 0 > tmp.html")
        system("lynx --dump /content/tmp.html > /content/text/tmp{}.txt".format(str(line_index)))

        system("rm /content/*.html ")
        system("rm /content/*.warc.gz")

        system("cat /content/text/*.txt > /content/text/_tmp")
        system("rm /content/text/*.txt")
        system("mv /content/text/_tmp /content/text/extracted-{}.txt".format(str(line_index)))
            
        if line_index != 0 and line_index % 3000 == 0 or line_index == lines_lenght - 1:
            passed = (datetime.now()-start_date).seconds/60
            print(line_index, '/', lines_lenght, '    ', round(passed, 2), '/', round(passed *(lines_lenght - start_from)/(line_index - start_from), 2))

            print('Saving to cloud...', str(line_index), line_index)
            system("tar czf /content/text/{}-{}.tar.gz --absolute-names /content/text/extracted-{}.txt".format(batch_name, str(line_index), str(line_index)))
            system("cp /content/text/{}-{}.tar.gz /content/drive/MyDrive/commoncrowl-data/".format(batch_name, str(line_index)))
            system("rm /content/text/extracted-{}.txt".format(str(line_index)))

    index_file.close()

    system("mv {} {}".format(index_file_batch_path, index_file_batch_path_.replace('commoncrowl/__aze', 'commoncrowl/++aze').replace('commoncrowl/__arm', 'commoncrowl/++arm')))
    system("rm /content/indexes.txt")
    system("rm /content/text/*")
    

    collect()


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Collect Data From CommonCrawl')
    parser.add_argument('--continue_with_file', help='File to continue with', )
    args = parser.parse_args()

    collect(args.continue_with_file)
