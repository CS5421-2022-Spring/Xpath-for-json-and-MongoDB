import unittest

from XpathQuery_preprocess import data_preprocess

class parse_to_MongoDB_Query_filter_TestCase(unittest.TestCase):
    def test_case1(self):
        XpathQuery = "child::library/child::album[child::year>=1990 or child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name"
        result = data_preprocess(XpathQuery, True, "", "")
        self.assertEqual(result, [("child::library/child::album[child::year>=1990 or child::year<=2000]/child::artists/child::artist[child::country='Indonesia']/child::name", "")])

    def test_case2(self):
        XPathQuery =  "count(child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name[child::age>30])"
        result = data_preprocess(XPathQuery, True, "", "")
        self.assertEqual(result, [("child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name[child::age>30]", "count")])
    
    def test_case3(self):
        XPathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name/child::text()"
        result =  data_preprocess(XPathQuery, True, "", "")
        self.assertEqual(result, [("child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name", "text")])
    
    def test_case4(self):
        XPathQuery = "child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name/text()"
        result =  data_preprocess(XPathQuery, True, "", "")
        self.assertEqual(result, [("child::library/child::album[child::artists[child::artist/child::name='Anang Ashanty']]/child::artists/child::artist[child::country='Indonesia']/child::name", "text")])

    
if __name__ == '__main__':
    unittest.main()