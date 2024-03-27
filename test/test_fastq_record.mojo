from blazeseq import FastqRecord, RecordParser
from blazeseq.helpers import get_next_line
from testing import assert_equal, assert_false, assert_true, assert_raises
from pathlib import Path

alias test_dir = "test/test_data/fastq_parser/"


fn test_invalid_file(file: String, msg: String = "") raises:
    with assert_raises(contains=msg):
        var parser = RecordParser(test_dir + file)
        parser.parse_all()


fn test_invalid() raises:
    test_invalid_file("error_diff_ids.fastq", "Non matching headers")
    test_invalid_file("error_long_qual.fastq", "Corrput Lengths")
    test_invalid_file("error_no_qual.fastq", "Corrput Lengths")
    # test_invalid_file("error_qual_tab.fastq")
    # test_invalid_file("error_qual_del.fastq")
    # test_invalid_file("error_qual_escape.fastq")
    # test_invalid_file("error_qual_null.fastq")
    # test_invalid_file("error_qual_space.fastq")
    # test_invalid_file("error_trunc_at_seq.fastq")
    # test_invalid_file("error_double_seq.fastq")
    test_invalid_file("error_trunc_in_plus.fastq")
    # test_invalid_file("error_spaces.fastq")
    # test_invalid_file("error_double_qual.fastq")
    # test_invalid_file("error_trunc_in_seq.fastq")
    # test_invalid_file("error_trunc_in_title.fastq")
    # test_invalid_file("error_trunc_at_plus.fastq")
    # test_invalid_file("error_trunc_at_qual.fastq")
    # test_invalid_file("error_qual_vtab.fastq")
    # test_invalid_file("error_tabs.fastq")


fn main() raises:
    test_invalid()
