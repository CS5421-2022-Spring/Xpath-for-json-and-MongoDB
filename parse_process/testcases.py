import unittest

from parse_process.parse import parse_to_MongoDB_Query_filter

# todo:测试 parse_to_MongoDB_Query_filter function
class parse_to_MongoDB_Query_filter_TestCase(unittest.TestCase):

    def test_case1(self):
        XpathQuery = "child::library/child::album[child::year>=1990 or child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name"
        filter = parse_to_MongoDB_Query_filter("", XpathQuery)

        self.assertEqual(filter,{'$or': [{'library.album.year': {'$gte': 1990}}, {'library.album.year': {'$lte': 2000}}], 'library.album.artists.artist.country': 'Indonesia'})

    def test_case2(self):
        XpathQuery = "child::library/child::album[child::artists/child::artist[child::age>=20 or child::age<=30] and child::artists/child::artist[child::country='Indonesia']]/child::title"
        filter = parse_to_MongoDB_Query_filter("",XpathQuery)

        self.assertEqual(filter,{'$or': [{'library.album.artists.artist.age': {'$gte': 20}}, {'library.album.artists.artist.age': {'$lte': 30}}], 'library.album.artists.artist.country': 'Indonesia'})

    def test_case3(self):
        XpathQuery = "child::library/child::album[child::year>=1990 and child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name"
        filter = parse_to_MongoDB_Query_filter("",XpathQuery)

        self.assertEqual(filter,{'library.album.year': {'$gte': 1990, '$lte': 2000}, 'library.album.artists.artist.country': 'Indonesia'})

    def test_case4(self):
        XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name"
        filter = parse_to_MongoDB_Query_filter("",XpathQuery)

        self.assertEqual(filter,{'library.album.artists.artist.name': 'Anang Ashanty', 'library.album.artists.artist.country': 'Indonesia'})

    def test_case5(self):
        XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name[child::age>30]"
        filter = parse_to_MongoDB_Query_filter("",XpathQuery)

        self.assertEqual(filter,{'library.album.artists.artist.name': 'Anang Ashanty', 'library.album.artists.artist.country': 'Indonesia', 'library.album.artists.artist.name.age': {'$gt': 30}})

    def test_case6(self):
        XpathQuery = "child::library/child::album[child::artists/child::artist[child::name='Anang Ashanty'] and child::artists/child::artist[child::country='Indonesia']]/child::title"
        filter = parse_to_MongoDB_Query_filter("",XpathQuery)

        self.assertEqual(filter,{'library.album.artists.artist.name': 'Anang Ashanty', 'library.album.artists.artist.country': 'Indonesia'})

    def test_case7(self):
        XpathQuery = "child::library/child::album[child::artists/child::artist[child::age>=20 and child::age<=30] and child::artists/child::artist[child::country='Indonesia']]/child::title"
        filter = parse_to_MongoDB_Query_filter("",XpathQuery)

        self.assertEqual(filter,{'library.album.artists.artist.age': {'$gte': 20, '$lte': 30}, 'library.album.artists.artist.country': 'Indonesia'})

    def test_case8(self):
        XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']/child::artist[child::country='Indonesia']]/child::title"
        filter = parse_to_MongoDB_Query_filter("",XpathQuery)

        self.assertEqual(filter,{'library.album.artists.artist.name': 'Anang Ashanty', 'library.album.artists.artist.country': 'Indonesia'})

    def test_case9(self):
        XpathQuery = "child::library/child::album[child::artists/child::artist[child::name='Anang Ashanty'] and child::artists/child::artist[child::country='Indonesia']]/child::title"
        filter = parse_to_MongoDB_Query_filter("",XpathQuery)

        self.assertEqual(filter,{'library.album.artists.artist.name': 'Anang Ashanty', 'library.album.artists.artist.country': 'Indonesia'})

    def test_case10(self):
        XpathQuery = "child::library/child::album[child::artists/child::artist/child::name='Anang Ashanty' and child::artists/child::artist/child::country='Indonesia']/child::title"
        filter = parse_to_MongoDB_Query_filter("",XpathQuery)

        self.assertEqual(filter,{'library.album.artists.artist.name': 'Anang Ashanty', 'library.album.artists.artist.country': 'Indonesia'})

    def test_case11(self):
        XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty'] and child::artists/child::artist[child::country='Indonesia']]/child::title"
        filter = parse_to_MongoDB_Query_filter("",XpathQuery)

        self.assertEqual(filter,{'library.album.artists.artist.name': 'Anang Ashanty', 'library.album.artists.artist.country': 'Indonesia'})

    def test_case12(self):
        XpathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']/child::artist/child::name='Anang Ashanty' and child::artists/child::artist[child::country='Indonesia']]/child::title"
        filter = parse_to_MongoDB_Query_filter("",XpathQuery)

        self.assertEqual(filter,{'library.album.artists.artist.name': 'Anang Ashanty', 'library.album.artists.artist.country': 'Indonesia'})




if __name__ == '__main__':
    unittest.main()
