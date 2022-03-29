import sys
import os
sys.path.append(os.getcwd())

import unittest
from output_preprocess.handle_output import buildXMLResult,finalOutput

# Xpath query for test
# XpathQuery = "child::library/child::album/child::artists/child::artist[child::name='Anang Ashanty']/child::name"
# XpathQuery = "count(child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::songs/child::song/child::title)"
# XpathQuery = "child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::genres/child::genre/text()"
# XpathQuery = "child::library/child::album[child::year>=1990 or child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name" # Xpath:3 objects, MonggoDB: 4 objects Anggun
# XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name/child::text()"
XpathQuery = "child::library/child::album[child::year>=1990 or child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name/child::text()"


class handle_output_TestCase(unittest.TestCase):
  maxDiff = None

  # =====================count=====================
  def test_case1(self):
    # XpathQuery = "count(child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::songs/child::song/child::title)"

    resultList = [{'_id': {'$oid': '623f46229083253fb26c6ca0'}, 'library': {'album': {
      'songs': {'song': [{'title': 'Timang-Timang'}, {'title': 'Miliki Diriku'}, {'title': 'Bua Hati'}]}}}},
      {'_id': {'$oid': '623f46229083253fb26c6ca2'}, 'library': {'album': {'songs': {
      'song': [{'title': 'Separuh Jiwaku Pergi'}, {'title': 'Belajarlah Untuk Cinta'},
      {'title': 'Hujanpun Menangis'}]}}}}]
    projection = {'library.album.songs.song.title': 1}
    xml_result = finalOutput(resultList,projection,operator="count",pretty=False)
    self.assertEqual(xml_result, "<result>6</result>")
 
  # =====================text=====================
  def test_case2(self):
    # XpathQuery = "child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::songs/child::song/child::title/text()"

    resultList = [{'_id': {'$oid': '623f46229083253fb26c6ca0'}, 'library': {'album': {
      'songs': {'song': [{'title': 'Timang-Timang'}, {'title': 'Miliki Diriku'}, {'title': 'Bua Hati'}]}}}},
      {'_id': {'$oid': '623f46229083253fb26c6ca2'}, 'library': {'album': {'songs': {
      'song': [{'title': 'Separuh Jiwaku Pergi'}, {'title': 'Belajarlah Untuk Cinta'},
      {'title': 'Hujanpun Menangis'}]}}}}]
    projection = {'library.album.songs.song.title': 1}
    xml_result = finalOutput(resultList,projection,operator="text",pretty=False)
    self.assertEqual(xml_result, "<result>Timang-Timang,Miliki Diriku,Bua Hati,Separuh Jiwaku Pergi,Belajarlah Untuk Cinta,Hujanpun Menangis</result>")

  # =====================nodes=====================
  def test_case3(self):
    # XpathQuery = "child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::songs/child::song/child::title"

    resultList = [{'_id': {'$oid': '6241d914303d5379b0bee742'}, 
    'library': {'album': 
    {'songs': {'song': [{'title': 'Timang-Timang'}, 
    {'title': 'Miliki Diriku'}, {'title': 'Bua Hati'}]}}}}, 
    {'_id': {'$oid': '6241d914303d5379b0bee743'}, 
    'library': {'album': 
    {'songs': {'song': [{'title': 'Separuh Jiwaku Pergi'}, 
    {'title': 'Belajarlah Untuk Cinta'}, {'title': 'Hujanpun Menangis'}]}}}}]
    projection = {'library.album.songs.song.title': 1}
    xml_result = finalOutput(resultList,projection,operator="",pretty=False)
    self.assertEqual(xml_result, "<result><title>Timang-Timang</title><title>Miliki Diriku</title>\
<title>Bua Hati</title><title>Separuh Jiwaku Pergi</title><title>Belajarlah Untuk Cinta</title>\
<title>Hujanpun Menangis</title></result>")

  def test_case4(self):
    # XpathQuery = "child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::genres/child::genre"

    resultList = [{'_id': {'$oid': '623f46229083253fb26c6ca0'},
                'library': {'album': {'genres': {'genre': ['Pop', 'World']}}}},
              {'_id': {'$oid': '623f46229083253fb26c6ca2'},
                'library': {'album': {'genres': {'genre': ['Pop', 'World']}}}}]
    projection = {'library.album.genres.genre': 1}
    xml_result = finalOutput(resultList,projection,operator="",pretty=False)
    self.assertEqual(xml_result, "<result><genre>Pop</genre><genre>World</genre><genre>Pop</genre><genre>World</genre></result>")

  def test_case5(self):
    # XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name"
    resultList = [{'_id': {'$oid': '623f46229083253fb26c6ca0'}, 'library': {
        'album': {'artists': {'artist': [{'name': 'Anang Ashanty'}, {'name': 'Kris Dayanti'}]}}}},
              {'_id': {'$oid': '623f46229083253fb26c6ca2'},
                'library': {'album': {'artists': {'artist': {'name': 'Anang Ashanty'}}}}}]
    projection = {'library.album.artists.artist.name': 1}
    xml_result = finalOutput(resultList,projection,operator="",pretty=False)
    self.assertEqual(xml_result, "<result><name>Anang Ashanty</name><name>Kris Dayanti</name><name>Anang Ashanty</name></result>")

  def test_case6(self):
    # XpathQuery = "child::library/child::album/child::artists/child::artist"
        
    resultList =[{'_id': {'$oid': '6241d914303d5379b0bee741'}, 
    'library': {'album': {'artists': {'artist': {'name': 'Anggun', 'country': 'Indonesia'}}}}}, 
    {'_id': {'$oid': '6241d914303d5379b0bee742'}, 
    'library': {'album': {'artists': {'artist': [{'name': 'Anang Ashanty', 'country': 'Indonesia'}, {'name': 'Kris Dayanti', 'country': 'Indonesia'}]}}}}, {'_id': {'$oid': '6241d914303d5379b0bee743'}, 'library': {'album': {'artists': {'artist': {'name': 'Anang Ashanty', 'country': 'Indonesia'}}}}}, {'_id': {'$oid': '6241d914303d5379b0bee744'}, 'library': {'album': {'artists': {'artist': {'name': 'Siti Nurhaliza', 'country': 'Malaysia'}}}}}, 
    {'_id': {'$oid': '6241d914303d5379b0bee745'}, 
    'library': {'album': {'artists': {'artist': {'name': 'Job Bunjob Pholin', 'country': 'Thailand'}}}}}, 
    {'_id': {'$oid': '6241d914303d5379b0bee746'}, 
    'library': {'album': {'artists': {'artist': {'name': 'Wham!', 'country': 'United Kingdom'}}}}}]   
    projection = {'library.album.artists.artist': 1}
    xml_result = finalOutput(resultList,projection,operator="",pretty=False)
    self.assertEqual(xml_result, "<result><artist><name>Anggun</name><country>Indonesia</country></artist><artist><name>Anang Ashanty</name><country>Indonesia</country></artist><artist><name>Kris Dayanti</name><country>Indonesia</country></artist><artist><name>Anang Ashanty</name><country>Indonesia</country></artist><artist><name>Siti Nurhaliza</name><country>Malaysia</country></artist><artist><name>Job Bunjob Pholin</name><country>Thailand</country></artist><artist><name>Wham!</name><country>United Kingdom</country></artist></result>")

if __name__ == '__main__':
  unittest.main()