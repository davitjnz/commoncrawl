# !/usr/bin/python
# coding=utf-8

from glob import glob
from datetime import datetime
import json
from os import system
import argparse
import subprocess
import pandas as pd
import re

def clean(path, lang, **kwargs):
    batch_name = path.split('/')[-1].split('.')[0]
    system('mkdir ./{batch_name}'.format(batch_name = batch_name))
    system('cp {path} ./{batch_name}/{batch_name}.tar.gz'.format(batch_name = batch_name, path = path))
    system('tar -xf ./{batch_name}/{batch_name}.tar.gz -C ./{batch_name} --absolute-names'.format(batch_name = batch_name))
    system('du ./{batch_name}/{batch_name}.tar.gz'.format(batch_name = batch_name))
    system('rm ./{batch_name}/{batch_name}.tar.gz'.format(batch_name = batch_name))
        
    source_file_path = glob('./{batch_name}/content/text/extracted-*.txt'.format(batch_name = batch_name))[0]
    system('du {source_file_path}'.format(source_file_path = source_file_path))

    source_file = open(source_file_path, 'r')
    result_file_path = './clear/{}.txt'.format(path.split('/')[-1].split('.')[0])
    result_file = open(result_file_path, 'a')
    
    lang_patterns = {
        'az': re.compile(r'[AaBbCcÇçDdEeƏəFfGgĞğHhXxIıİiJjKkQqLlMmNnOoÖöPpRrSsŞşTtUuÜüVvYyZz \!\?]'),
        'hy': re.compile(r'[աԱբԲգԳդԴեԵզԶէԷըԸթԹժԺիԻլԼխԽծԾկԿհՀձՁղՂճՃմՄյՅնՆշՇոՈչՉպՊջՋռՌսՍվՎտՏրՐցՑւՒփՓքՔօՕֆՖ \!\?]')
    }
    uppercase_characters = {
        'az': 'ABCÇDEƏFGĞHXIİJKQLMNOÖPRSŞTUÜVYZ',
        'hy': 'ԱԲԳԴԵԶԷԸԹԺԻԼԽԾԿՀՁՂՃՄՅՆՇՈՉՊՋՌՍՎՏՐՑՒՓՔՕՖ'
    }
    re_brakets = '\[.*\]|\(.*\)'
    re_urls = '(https?:\/\/(www\.)?[-a-zA-Z0-9@:%._\+~#=]{1,256}\.[a-zA-Z0-9()]{1,6}([-a-zA-Z0-9()@:%_\+.~#?&\/=]*))'
    re_files = '(file:\/\/?\/([\.\w\/]*))'
    re_strip = '(?:^[\* \n\r\t\d]+)|(?:[\* \n\r\t\d]+$)'
    re_long_words = '([\b\w-]{32,})'

    # print('{}|{}|{}|{}|{}'.format(re_brakets, re_urls, re_files, re_strip, re_long_words))
    remove_pattern = re.compile(r'{}|{}|{}|{}|{}'.format(re_brakets, re_urls, re_files, re_strip, re_long_words))

    multi_space_pattern = re.compile(r' {2,}')

    errors = 0
    lines = 0

    line = True
    prev_line = ''
    cach_for_dublicates = []

    while True:
        lines += 1
        try:
            line = source_file.readline()
            if not line:
                break
        except:
            errors += 1
            continue

        line = re.sub(remove_pattern, '',line)
        line = re.sub(multi_space_pattern, ' ',line)

        if len(line) <= 20 or len(lang_patterns[lang].findall(line))/len(line) < 0.7 or line in cach_for_dublicates:
            if prev_line:
                
                result_file.write(prev_line + '\n')
                cach_for_dublicates.append(prev_line)
                cach_for_dublicates = cach_for_dublicates[-10000:]
                
                prev_line = ''
            continue
        
        # lines_raw.append(line)

        if line[-1] in '.!?;':
            concated_lines = (prev_line + ' ' if prev_line else '') + line
            result_file.write(concated_lines + '\n')
            cach_for_dublicates.append(concated_lines)
            cach_for_dublicates = cach_for_dublicates[-10000:]
            prev_line = ''
            continue

        
        if prev_line and line[0] in uppercase_characters[lang]:
            result_file.write(prev_line + '\n')
            cach_for_dublicates.append(prev_line)
            cach_for_dublicates = cach_for_dublicates[-10000:]
            
            prev_line = line
            continue
        
        prev_line += line
        
        
    source_file.close()
    result_file.close()
    print('lines', lines, 'errors', errors)
    system('du {result_file_path}'.format(result_file_path = result_file_path))
    system('rm {source_file_path}'.format(source_file_path = source_file_path))
    system('rm -rf ./{batch_name}'.format(batch_name = batch_name))

if __name__ == '__main__':
    parser = argparse.ArgumentParser('Clean the Data From CommonCrawl')
    parser.add_argument('--batch_path', type=str, help='Batch path', required=True )
    parser.add_argument('--lang', type=str, help='Language of the batch text', required=True, default='az' )


    args = parser.parse_args()
    
    kwargs = {}
    # for arg_name in [ arg_name for arg_name in vars(args) if getattr(args, arg_name)]:
    #     kwargs[arg_name] = getattr(args, arg_name)
        
    clean(args.batch_path, args.lang, **kwargs)
