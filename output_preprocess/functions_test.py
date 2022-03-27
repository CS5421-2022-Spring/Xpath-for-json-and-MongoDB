import unittest
from handle_functions import *
from bson.json_util import dumps

database = 'test'
collection = 'library'

class MyTestCase(unittest.TestCase):
    def test_handleCount1(self):
        # XpathQuery = "count(child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::songs/child::song/child::title)"

        result = [{'_id': {'$oid': '623f46229083253fb26c6ca0'}, 'library': {'album': {
            'songs': {'song': [{'title': 'Timang-Timang'}, {'title': 'Miliki Diriku'}, {'title': 'Bua Hati'}]}}}},
                  {'_id': {'$oid': '623f46229083253fb26c6ca2'}, 'library': {'album': {'songs': {
                      'song': [{'title': 'Separuh Jiwaku Pergi'}, {'title': 'Belajarlah Untuk Cinta'},
                               {'title': 'Hujanpun Menangis'}]}}}}]
        projection = {'library.album.songs.song.title': 1}

        count_result = handleCount(projection, result)

        self.assertEqual(count_result, 6)

    def test_handleCount2(self):
        # XpathQuery = "count(child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::genres/child::genre)"

        result = [{'_id': {'$oid': '623f46229083253fb26c6ca0'},
                   'library': {'album': {'genres': {'genre': ['Pop', 'World']}}}},
                  {'_id': {'$oid': '623f46229083253fb26c6ca2'},
                   'library': {'album': {'genres': {'genre': ['Pop', 'World']}}}}]
        projection = {'library.album.genres.genre': 1}

        count_result = handleCount(projection, result)

        self.assertEqual(count_result, 4)

    def test_handleCount3(self):
        # XpathQuery = "count(child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name)"
        result = [{'_id': {'$oid': '623f46229083253fb26c6ca0'}, 'library': {
            'album': {'artists': {'artist': [{'name': 'Anang Ashanty'}, {'name': 'Kris Dayanti'}]}}}},
                  {'_id': {'$oid': '623f46229083253fb26c6ca2'},
                   'library': {'album': {'artists': {'artist': {'name': 'Anang Ashanty'}}}}}]
        projection = {'library.album.artists.artist.name': 1}

        count_result = handleCount(projection, result)

        self.assertEqual(count_result, 3)

    def test_handleCount4(self):
        result = [{'_id': {'$oid': '623f46229083253fb26c6ca0'},
                   'library': {'album': [{'genres': {'genre': ['Pop', 'World']}},
                                         {'genres': {'genre': ['Pop']}}]}},
                  {'_id': {'$oid': '623f46229083253fb26c6ca2'},
                   'library': {'album': {'genres': {'genre': ['Pop', 'World']}}}}]
        projection = {'library.album.genres.genre': 1}

        count_result = handleCount(projection, result)

        self.assertEqual(count_result, 5)

    def test_handleText5(self):
        result = [{'_id': {'$oid': '623f46229083253fb26c6ca0'},
                   'library': {'album': {'genres': {'genre': ['Pop', 'World']}}}},
                  {'_id': {'$oid': '623f46229083253fb26c6ca2'},
                   'library': [{'album': {'genres': {'genre': ['Pop', 'World']}}},
                               {'album': {'genres': {'genre': ['Pop']}}}]
                   }]
        projection = {'library.album.genres.genre': 1}

        count_result = handleCount(projection, result)

        self.assertEqual(count_result, 5)

    def test_handleText6(self):
        result = [{'_id': {'$oid': '623f46229083253fb26c6ca2'},
                   'library': [{'album': {'genres': {'genre': ['Pop', 'World']}}},
                               {'album': {'genres': {'genre': ['Pop']}}}]
                   }]
        projection = {'library.album.genres.genre': 1}

        count_result = handleCount(projection, result)

        self.assertEqual(count_result, 3)


    def test_handleText1(self):
        # XpathQuery = "child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::songs/child::song/child::title/text()"

        result = [{'_id': {'$oid': '623f46229083253fb26c6ca0'}, 'library': {'album': {
            'songs': {'song': [{'title': 'Timang-Timang'}, {'title': 'Miliki Diriku'}, {'title': 'Bua Hati'}]}}}},
                  {'_id': {'$oid': '623f46229083253fb26c6ca2'}, 'library': {'album': {'songs': {
                      'song': [{'title': 'Separuh Jiwaku Pergi'}, {'title': 'Belajarlah Untuk Cinta'},
                               {'title': 'Hujanpun Menangis'}]}}}}]
        projection = {'library.album.songs.song.title': 1}

        text_result = handleText(projection, result)

        self.assertEqual(text_result, ['Timang-Timang', 'Miliki Diriku', 'Bua Hati', 'Separuh Jiwaku Pergi',
                                       'Belajarlah Untuk Cinta', 'Hujanpun Menangis'])

    def test_handleText2(self):
        # XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name/child::text()"

        result = [{'_id': {'$oid': '623f46229083253fb26c6ca0'}, 'library': {
            'album': {'artists': {'artist': [{'name': 'Anang Ashanty'}, {'name': 'Kris Dayanti'}]}}}},
                  {'_id': {'$oid': '623f46229083253fb26c6ca2'},
                   'library': {'album': {'artists': {'artist': {'name': 'Anang Ashanty'}}}}}]
        projection = {'library.album.artists.artist.name': 1}

        text_result = handleText(projection, result)

        self.assertEqual(text_result, ['Anang Ashanty', 'Kris Dayanti', 'Anang Ashanty'])

    def test_handleText3(self):
        # XpathQuery = "child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty']/child::genres/child::genre/text()"

        result = [{'_id': {'$oid': '623f46229083253fb26c6ca0'},
                   'library': {'album': {'genres': {'genre': ['Pop', 'World']}}}},
                  {'_id': {'$oid': '623f46229083253fb26c6ca2'},
                   'library': {'album': {'genres': {'genre': ['Pop', 'World']}}}}]
        projection = {'library.album.genres.genre': 1}

        text_result = handleText(projection, result)

        self.assertEqual(text_result, ['Pop', 'World', 'Pop', 'World'])

    def test_handleText4(self):
        result = [{'_id': {'$oid': '623f46229083253fb26c6ca0'},
                   'library': {'album': [{'genres': {'genre': ['Pop', 'World']}},
                                         {'genres': {'genre': ['World']}}]}},
                  {'_id': {'$oid': '623f46229083253fb26c6ca2'},
                   'library': {'album': {'genres': {'genre': ['Pop', 'World']}}}}]
        projection = {'library.album.genres.genre': 1}

        text_result = handleText(projection, result)

        self.assertEqual(text_result, ['Pop', 'World','World', 'Pop', 'World'])

    def test_handleText5(self):
        result = [{'_id': {'$oid': '623f46229083253fb26c6ca0'},
                   'library': {'album': {'genres': {'genre': ['Pop', 'World']}}}},
                  {'_id': {'$oid': '623f46229083253fb26c6ca2'},
                   'library': [{'album': {'genres': {'genre': ['Pop']}}},
                               {'album': {'genres': {'genre': ['Pop', 'World']}}}]
                   }]
        projection = {'library.album.genres.genre': 1}

        text_result = handleText(projection, result)

        self.assertEqual(text_result, ['Pop', 'World','Pop', 'Pop', 'World'])


if __name__ == '__main__':
    unittest.main()
