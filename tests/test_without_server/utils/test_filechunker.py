import unittest

from hpycc.utils.filechunker import make_chunks


class TestMakeChunks(unittest.TestCase):
    def test_make_chunks_splits_with_num_zero(self):
        res = make_chunks(0)
        expected = []
        self.assertEqual(expected, res)

    def test_make_chunks_splits_with_a_full_chunk(self):
        res = make_chunks(10, 10)
        expected = [(0, 10)]
        self.assertEqual(expected, res)

    def test_make_chunks_splits_with_two_full_chunks(self):
        res = make_chunks(20, 10)
        expected = [(0, 10), (10, 10)]
        self.assertEqual(expected, res)

    def test_make_chunks_chunks_sum_correctly(self):
        res = make_chunks(500, 3)
        summed = sum([i[1] for i in res])
        self.assertEqual(summed, 500)

    def test_make_chunks_chunks_num_less_than_chunksize(self):
        res = make_chunks(3, 10)
        expected = [(0, 3)]
        self.assertEqual(expected, res)

    def test_make_chunks_chunks_num_greater_than_chunksize(self):
        res = make_chunks(10, 3)
        expected = [(0, 3), (3, 3), (6, 3), (9, 1)]
        self.assertEqual(expected, res)

    def test_make_chunks_chunksize_equal_zero(self):
        with self.assertRaises(ZeroDivisionError):
            make_chunks(10, 0)

    def test_make_chunks_uses_10000_as_default_chunksize(self):
        res = make_chunks(10000)
        expected = [(0, 10000)]
        self.assertEqual(expected, res)
