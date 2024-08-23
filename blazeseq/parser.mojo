from blazeseq.record import FastqRecord, RecordCoord
from blazeseq.CONSTS import *

# from blazeseq.stats import FullStats
from blazeseq.iostream import BufferedLineIterator
import time


struct RecordParser:
    var stream: BufferedLineIterator
    var quality_schema: QualitySchema

    fn __init__(
        inout self, path: String, schema: String = "generic"
    ) raises -> None:
        self.stream = BufferedLineIterator(path, 8 * 1024)
        self.quality_schema = self._parse_schema(schema)

    fn parse_all(inout self) raises:
        while True:
            var record: FastqRecord
            record = self._parse_record()
            _ = record.validate_record()

            # ASCII validation is carried out in the reader
            # @parameter
            # if validate_quality:
            #     _ = record.validate_quality_schema()

    @always_inline
    fn next(inout self) raises -> FastqRecord:
        """Method that lazily returns the Next record in the file."""
        var record: FastqRecord
        record = self._parse_record()
        _ = record.validate_record()

        # ASCII validation is carried out in the reader
        # @parameter
        # if validate_quality:
        #     _ = record.validate_quality_schema()
        return record

    @always_inline
    fn _parse_record(inout self) raises -> FastqRecord:
        var line1 = self.stream.read_line()
        var line2 = self.stream.read_line()
        var line3 = self.stream.read_line()
        var line4 = self.stream.read_line()
        return FastqRecord(line1, line2, line3, line4, self.quality_schema)

    @staticmethod
    @always_inline
    fn _parse_schema(quality_format: String) -> QualitySchema:
        var schema: QualitySchema

        if quality_format == "sanger":
            schema = sanger_schema
        elif quality_format == "solexa":
            schema = solexa_schema
        elif quality_format == "illumina_1.3":
            schema = illumina_1_3_schema
        elif quality_format == "illumina_1.5":
            schema = illumina_1_5_schema
        elif quality_format == "illumina_1.8":
            schema = illumina_1_8
        elif quality_format == "generic":
            schema = generic_schema
        else:
            print(
                """Uknown quality schema please choose one of 'sanger', 'solexa',"
                " 'illumina_1.3', 'illumina_1.5' 'illumina_1.8', or 'generic'.
                Parsing with generic schema."""
            )
            return generic_schema
        return schema


struct CoordParser:
    var stream: BufferedLineIterator

    fn __init__(inout self, path: String) raises -> None:
        self.stream = BufferedLineIterator(path, DEFAULT_CAPACITY)

    @always_inline
    fn parse_all(inout self) raises:
        while True:
            var record: RecordCoord
            record = self._parse_record()
            record.validate()

    @always_inline
    fn next(inout self) raises -> RecordCoord:
        var read: RecordCoord
        read = self._parse_record()
        read.validate()
        return read

    @always_inline
    fn _parse_record(inout self) raises -> RecordCoord:
        var line1 = self.stream.read_line_span()
        if line1[0] != "@":
            print("String:", line1[0])
            raise Error("Sequence Header is corrupt")

        var line2 = self.stream.read_line_span()
        var line3 = self.stream.read_line_span()
        if line3[0] != "+":
            raise Error("Quality Header is corrupt")
        var line4 = self.stream.read_line_span()
        return RecordCoord(line1, line2, line3, line4)


fn main() raises:
    var n = 0
    var parser = CoordParser("data/M_abscessus_HiSeq.fq")

    var start = time.now()
    while True:
        try:
            var record = parser.next()
            n += 1
        except Error:
            print(Error._message())
            print(n)
            break
    var end = time.now()
    print("Time taken: ", (end - start) / 1e9)
