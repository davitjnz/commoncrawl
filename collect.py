from glob import glob
from tqdm.notebook import tqdm
from datetime import datetime
import json
from os import system
import argparse
import subprocess
import pandas as pd

def get_batch(runner_id, lang, index_dir, data_dir):
    
    """
        Get running or first avaliable batch name and line from whitch to start data collection 
    """

    df = pd.read_csv('{}/register.csv'.format(data_dir), index_col=None)
    running = df[(df.runner_id == runner_id) & (df.status == 'running')]
    
    if running.shape[0] != 0:
        batch_name = running.iloc[0].batch_name
        start_from = running.iloc[0].collected_lines_count
    else:
        batch_name = get_new_batch(df, index_dir, lang)

        start_from = 0
        
        df = df.append({"runner_id": runner_id, "status": 'running', "collected_lines_count": 0, "batch_name": batch_name}, ignore_index=True)
        df.to_csv('{}/register.csv'.format(data_dir), index=None)

    return batch_name, start_from


def get_new_batch(df, index_dir, lang):

    batch_names = [ a.split('/')[-1].replace('.tar.gz', '') for a in glob('{}/{}*.tar.gz'.format(index_dir, lang))]
    avaliable_batches = list(set(batch_names) - set(df.batch_name.values))
    if len(avaliable_batches) == 0:
        raise Exception("No index batches left")
    
    batch_name = avaliable_batches[0]

    return  batch_name


def download_and_extract_batch_file(index_file_batch_path, work_dir):
    """
        Download common crawl index file, extrac and concat index files 
    """

    system('cp {} {}/index_file_batch.tar.gz'.format(index_file_batch_path, work_dir))
    system('tar -xf {}/index_file_batch.tar.gz -C {}/indexes'.format(work_dir, work_dir))
    system('rm {}/index_file_batch.tar.gz'.format(work_dir))

    system('cat {}/indexes/* > {}/indexes.txt'.format(work_dir, work_dir))
    system('rm {}/indexes/*'.format(work_dir))



def download_webpage_data_as_text(line, line_index, work_dir):
    """
        Download thml data, extract and concat to single file
    """

    index = json.loads('{"url":' + line.split('{"url":')[1])

    system('curl -s -r{offset}-$(({offset}+{length}-1)) https://commoncrawl.s3.amazonaws.com/{filename} > {work_dir}/tmp.warc.gz'.format(
        offset = index['offset'], 
        length = index['length'], 
        filename = index['filename'], 
        work_dir = work_dir))
     
    system('warcio extract --payload {work_dir}/tmp.warc.gz 0 > tmp.html'.format(work_dir = work_dir))
    system('lynx --dump {work_dir}/tmp.html > {work_dir}/text/tmp{line_index}.txt'.format(
        work_dir = work_dir,
        line_index = str(line_index)
    ))

    system('rm {work_dir}/*.html'.format(work_dir = work_dir))
    system('rm {work_dir}/*.warc.gz'.format(work_dir = work_dir))

    system('cat {work_dir}/text/*.txt > {work_dir}/text/_tmp'.format(work_dir = work_dir))
    system('rm {work_dir}/text/*.txt'.format(work_dir = work_dir))
    system('mv {work_dir}/text/_tmp {work_dir}/text/extracted-{line_index}.txt'.format(work_dir = work_dir, line_index = str(line_index)))



def save_data(runner_id, batch_name, data_dir, work_dir, line_index, lines_lenght, start_date, start_from):
    """
        Save collected text and empty files
    """

    passed = (datetime.now()-start_date).seconds/60
    print(line_index, '/', lines_lenght, '    ', round(passed, 2), '/', round(passed *(lines_lenght - start_from)/(line_index - start_from), 2))
    print('Saving to cloud...', str(line_index), line_index)

    system('tar czf {work_dir}/text/{batch_name}-{line_index}.tar.gz --absolute-names {work_dir}/text/extracted-{line_index}.txt'.format(
        batch_name=batch_name, 
        line_index = str(line_index),
        work_dir = work_dir
    ))

    system('cp {work_dir}/text/{batch_name}-{line_index}.tar.gz {data_dir}/'.format(
        batch_name = batch_name, 
        line_index = str(line_index), 
        data_dir = data_dir,
        work_dir = work_dir
    ))
    
    system('rm {work_dir}/text/extracted-{line_index}.txt'.format(
        line_index = str(line_index),
        work_dir = work_dir
    ))

    df = pd.read_csv('{}/register.csv'.format(data_dir), index_col=None)
    index = df[(df.runner_id == runner_id) & (df.status == 'running')].index[0]
    df.at[index, "collected_lines_count"] = line_index
    df.to_csv('{}/register.csv'.format(data_dir), index=None)
    


def set_df(data_dir, runner_id, col, val):
    df = pd.read_csv('{}/register.csv'.format(data_dir), index_col=None)
    index = df[(df.runner_id == runner_id) & (df.status == 'running')].index[0]
    df.at[index, col] = val
    df.to_csv('{}/register.csv'.format(data_dir), index=None)



def collect(runner_id, **kwargs):
    """
        Start data collection process
    """
    lang, index_dir, data_dir, work_dir, save_on = kwargs["lang"], kwargs["index_dir"], kwargs["data_dir"], kwargs["work_dir"], kwargs["save_on"]
    
    system('mkdir -p {}/indexes'.format(work_dir))
    system('mkdir -p {}/text'.format(work_dir))

    system('rm -f {}/indexes/*'.format(work_dir))
    system('rm -f {}/text/*'.format(work_dir))

    system('rm -f {}/indexes.txt'.format(work_dir))
    system('rm -f {}/*warc.gz'.format(work_dir))
    
    start_date = datetime.now()
    
    if 'batch_name' not in kwargs:
        batch_name, start_from = get_batch(runner_id, lang, index_dir, data_dir)
    else:
        batch_name, start_from = kwargs['batch_name'], kwargs['start_from']

    index_file_batch_path = '{}/{}.tar.gz'.format(index_dir, batch_name)
    
    download_and_extract_batch_file(index_file_batch_path, work_dir)

    lines_lenght_ = subprocess.getoutput('wc -l ./indexes.txt')
    lines_lenght = int(lines_lenght_.split(' ')[0])

    index_file = open('./indexes.txt', 'r')

    print('batch_name:', batch_name, 'starting from', start_from, 'of', lines_lenght)

    for line_index in range(lines_lenght):
        if line_index < start_from + 1:
            continue
            
        line = index_file.readline()

        download_webpage_data_as_text(line, line_index, work_dir)
            
        if line_index != 0 and line_index % save_on == 0 or line_index == lines_lenght - 1:
            save_data(runner_id, batch_name, data_dir, work_dir, line_index, lines_lenght, start_date, start_from)

    index_file.close()


    system('rm {}/indexes.txt'.format(work_dir))
    system('rm {}/text/*'.format(work_dir))
    
    df = pd.read_csv('{}/register.csv'.format(data_dir), index_col=None)
    index = df[(df.runner_id == runner_id) & (df.status == 'running')].index[0]
    df.at[index, 'status'] = 'complated'
    
    batch_name = get_new_batch(df, index_dir, lang)
    
    df = df.append({"runner_id": runner_id, "status": 'running', "collected_lines_count": 0, "batch_name": batch_name}, ignore_index=True)
    df.to_csv('{}/register.csv'.format(data_dir), index=None)
    
    collect(runner_id, 
            lang = lang, 
            index_dir = index_dir, 
            data_dir = data_dir, 
            work_dir = work_dir, 
            batch_name = batch_name,
            start_from = 0,
            save_on = save_on)


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Collect Data From CommonCrawl')
    parser.add_argument('--runner_id', type=int, help='Runner collecting the data', required=True )
    parser.add_argument('--lang', help='Runner collecting the data', default = 'aze')
    parser.add_argument('--index_dir', default='/content/drive/MyDrive/commoncrowl')
    parser.add_argument('--data_dir', default='/content/drive/MyDrive/commoncrowl-data')
    parser.add_argument('--work_dir', default='/content')
    parser.add_argument('--save_on', type=int, default=3000)

    args = parser.parse_args()
    
    kwargs = {}
    for arg_name in [ arg_name for arg_name in vars(args) if getattr(args, arg_name) and arg_name != 'runner_id']:
        kwargs[arg_name] = getattr(args, arg_name)
        
    collect(args.runner_id, **kwargs)
