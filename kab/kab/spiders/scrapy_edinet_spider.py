# -*- coding: utf-8 -*-
import scrapy
import json
import requests
import re
import zipfile
import os
import time
from datetime import datetime, date, timedelta
import shutil
from pathlib import Path
from kab.items import KabItem
from kab.config import COLUMNS
import glob

current_dir = Path(__file__).parent
dl_path = current_dir / 'DL/'
out_path = current_dir / 'out/'

DL_FOLDER = str(dl_path) + '/'
OUT_FOLDER = str(out_path) + '/'
XBRL_REGEXP = re.compile(r'jpcrp[0-9]{6}-.{3}-[0-9]{3}_E[0-9]{5}-[0-9]{3}_[0-9]{4}-[0-9]{2}-[0-9]{2}_[0-9]{2}_[0-9]{4}-[0-9]{2}-[0-9]{2}.xbrl')
START_DATE = date(2019,1,1)
END_DATE = date(2019,12,1)

class ScrapyEdinetSpiderSpider(scrapy.Spider):
  name = 'scrapy_edinet_spider'
  allowed_domains = ['disclosure.edinet-fsa.go.jp']
  start_urls = [
    "https://disclosure.edinet-fsa.go.jp/api/v1/documents.json?date={}&type=2"
    .format(str(START_DATE + timedelta(i))) for i in range((END_DATE - START_DATE).days - 1)
  ]

  def parse(self, response):
    """
    レスポンスに対するパース処理
    """
    # jsonにする
    res_json = json.loads(response.body_as_unicode())['results']

    print('レスポンス受け取り成功、処理開始')
    print(len(res_json))

    meta_infos = [] # xbrlを参照しなくても得られる情報

    for doc in res_json:
      # 雑多なデータを無視して有報と四半期報告書のみをダウンロード対象にする。
      if doc['ordinanceCode'] == '010':
        if doc['formCode'] == '043000' or doc['formCode'] == '030000':
          if doc['secCode']:

            meta_infos.append({
              'docID': doc['docID'],
              'secCode': doc['secCode'],
              'filerName': doc['filerName'],
              'docDescription': doc['docDescription'],
              'periodStart': doc['periodStart'],
              'periodEnd': doc['periodEnd'],
            })


    for i in range(len(meta_infos)):
      file_url = 'https://disclosure.edinet-fsa.go.jp/api/v1/documents/' + meta_infos[i]['docID'] + '?type=1'
      yield scrapy.Request(file_url, callback=self.parse_file,
                           meta=meta_infos[i])

  def parse_file(self, response):
    zip_filepath = DL_FOLDER + response.meta['docID'] + '.zip'

    # zipファイル保存
    self.save_zipfile(response, zip_filepath)
    print('file: ' + response.meta['docID'] + " has been downloaded!")

    # zip解凍、XBRL抽出、余分なファイル削除
    xbrl_filepath = self.unzip_and_arrange(zip_filepath)

    # XBRL読み込み、必要なカラムを抽出
    lines = self.read_xbrl(xbrl_filepath)

    # XBRL整形
    parsed_lines = [self.parse_xbrl_line(l) for l in lines]
    excluded_lines = [l for l in parsed_lines if not len(l) == 3]
    print(excluded_lines)
    parsed_lines = [l for l in parsed_lines if len(l) == 3]

    # XBRLからCOLUMNSに対応する値を取得
    items = {}
    for column in COLUMNS:
      items[column] = self.return_value_of(column, parsed_lines)

    yield KabItem(
      secCode = response.meta['secCode'],
      filerName = response.meta['filerName'],
      docDescription = response.meta['docDescription'],
      periodStart = response.meta['periodStart'],
      periodEnd = response.meta['periodEnd'],
      **items,
    )

  def save_zipfile(self, response, filepath):
    chunks = [response.body[i: i+1024] for i in range(0, len(response.body), 1024)]
    with open(filepath, 'wb') as f:
      for chunk in chunks:
        if chunk:
          f.write(chunk)
          f.flush()

  def unzip_and_arrange(self, filepath):
    with zipfile.ZipFile(filepath) as zip_e:
      n_l = zip_e.infolist()
      for i in n_l:
        match_obj = XBRL_REGEXP.search(i.filename)
        if match_obj:
          print('file found!')
          zip_e.extract(i, OUT_FOLDER)
          print('unzipped!')
          xbrl_filepath = OUT_FOLDER + match_obj.group()
          try:
            shutil.copy(xbrl_filepath, OUT_FOLDER)
          except shutil.SameFileError:
            pass
          shutil.rmtree(OUT_FOLDER + 'XBRL/')
          os.remove(filepath)
          return xbrl_filepath

  def read_xbrl(self, filepath):
    with open(filepath) as f:
      lines = [l for l in f.readlines() if "jpdei" in l or "jppfs" in l or "jpcrp" in l]

    for l in lines:
      if "jpdei_cor:NumberOfSubmissionDEI" in l:
        start = lines.index(l)
      elif "jpcrp_cor:BusinessResultsOfGroupTextBlock" in l or "BusinessResultsOfReportingCompanyTextBlock" in l:
        end = lines.index(l)
        break

    return lines[start:end]

  def parse_xbrl_line(self, line):
    pat = [r'/>', r'</.+', r'<', r'>']
    pat_c = list(map(re.compile, pat))

    for j in range(3):
      line = re.sub(pat_c[j], '', line)
    line = re.sub(r'>', ' ', line)
    attrs = line.split()

    if attrs == []:
      return attrs
    if attrs[1]=='xsi:nil="true"':
      attrs.pop(1)
      attrs.append('null')
    if 'contextRef' not in attrs[1]:
      attrs.pop(1)

    t = ['Instant', 'Duration']
    st = ['i', 'd']
    y = ['Current', 'Prior1', 'Prior2', 'Prior3', 'Prior4']
    # for i in range(len(y)):
    #   for j in range(len(t)):
    #     if y[i] + 'Year' + t[j] in attrs[1]:
    #       attrs[1] = attrs[1].replace(y[i] + 'Year' + t[j], str(i)+'_'+st[j])
    #       break
    #   else:
    #     continue
    #   break

    # if 'NonConsolidatedMember' in attrs[1]:
    #   attrs[1] = attrs[1].replace('NonConsolidatedMember', 'nc')
    if len(attrs) > 3:
      if 'decimals' in attrs[3]:
        attrs.pop(3)
      if 'unitRef' in attrs[2]:
        attrs.pop(2)

    return attrs

  def return_value_of(self, column, lines):
    column_lines = [
      l for l in lines
      if column in l[0] and 'Current' in l[1] and not 'NonConsolidatedMember' in l[1]
    ]
    if not len(column_lines) == 1:
      print(column_lines)
      return None
    return column_lines[0][2]
