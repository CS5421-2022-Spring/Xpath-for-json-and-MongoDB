import unittest
from handle_concise import *
short_hand_convert_to_full_path


class MyTestCase(unittest.TestCase):
    def test_short_hand_convert_to_full_path(self):
        self.assertEqual(short_hand_convert_to_full_path("/a[b < 5 and c > 6]"), "/child::a[child::b < 5 and child::c > 6]")  # add assertion here
        self.assertEqual(short_hand_convert_to_full_path("/a[b = ’A’]"), "/child::a[child::b = ’A’]")
        self.assertEqual(short_hand_convert_to_full_path("//b/c"),"/descendant::b/child::c")
        self.assertEqual(short_hand_convert_to_full_path("(/d|/e)"), "(/child::d|/child::e)")

if __name__ == '__main__':
    unittest.main()
