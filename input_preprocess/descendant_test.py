import unittest
from handle_descendant import *

database = 'test'
collection = 'library'
class MyTestCase(unittest.TestCase):
    def test_bfs_parse_full_path_for_descendant(self):
        self.assertEqual(bfs_parse_full_path_for_descendant("descendant-or-self", "descendant-or-self::title/", database,collection,"library.album.title"), {"library.album.title"})
        self.assertEqual(bfs_parse_full_path_for_descendant("descendant", "descendant::title/", database,collection, "library.album.title"), set())
        self.assertEqual(bfs_parse_full_path_for_descendant("descendant-or-self", "descendant-or-self::title/", database, collection, "library.album.songs"), {"library.album.songs.song.title"})
        self.assertEqual(bfs_parse_full_path_for_descendant("descendant", "descendant::title/", database, collection, ""), {"library.album.songs.song.title", "library.album.title"})
        self.assertEqual(bfs_parse_full_path_for_descendant("descendant", "descendant::title/", database, collection, "library.album.songs"), {"library.album.songs.song.title"})

    def test_convert_pure_json_path_to_full_path(self):
        self.assertEqual(convert_pure_json_path_to_full_path("song.title"), "child::song/child::title")  # add assertion here

    def test_convert_all_descendant_to_child(self):
        Xpath_for_descendant_text = "descendant::song/descendant::title"
        xpath_set = set()
        convert_all_descendant_to_child("descendant", Xpath_for_descendant_text, database, collection, xpath_set)
        self.assertEqual(xpath_set, {'child::library/child::album/child::songs/child::song/child::title'})

        Xpath_for_descendant_text = "descendant::album[child::year>=1990 and child::year <=2000]/child::artists/child::artist[child::country='Indonesia']/child::name"
        xpath_set = set()
        convert_all_descendant_to_child("descendant", Xpath_for_descendant_text, database, collection, xpath_set)
        self.assertEqual(xpath_set, {"child::library/child::album[child::year>=1990 and child::year <=2000]/child::artists/child::artist[child::country='Indonesia']/child::name"})

        Xpath_for_descendant_text = "descendant::title"
        xpath_set = set()
        convert_all_descendant_to_child("descendant", Xpath_for_descendant_text, database, collection, xpath_set)
        self.assertEqual(xpath_set, {'child::library/child::album/child::title', 'child::library/child::album/child::songs/child::song/child::title'})


if __name__ == '__main__':
    unittest.main()
