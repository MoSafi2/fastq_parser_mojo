from blazeseq.record import FastqRecord
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
            # print(record)
            # _ = record.validate_record()

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


# struct CoordParser:
#     var stream: BufferedLineIterator

#     fn __init__(inout self, path: String) raises -> None:
#         self.stream = BufferedLineIterator(path, DEFAULT_CAPACITY)

#     @always_inline
#     fn parse_all(inout self) raises:
#         while True:
#             var record: RecordCoord
#             record = self._parse_record()
#             record.validate()

#     @always_inline
#     fn next(inout self) raises -> RecordCoord:
#         var read: RecordCoord
#         read = self._parse_record()
#         read.validate()
#         return read

#     @always_inline
#     fn _parse_record(inout self) raises -> RecordCoord:
#         var line1 = self.stream.read_next_coord()
#         if (
#             self.stream.buf[self.stream.map_pos_2_buf(line1.start.value())]
#             != read_header
#         ):
#             raise Error("Sequence Header is corrupt")

#         var line2 = self.stream.read_next_coord()

#         var line3 = self.stream.read_next_coord()
#         if (
#             self.stream.buf[self.stream.map_pos_2_buf(line3.start.value())]
#             != quality_header
#         ):
#             raise Error("Quality Header is corrupt")

#         var line4 = self.stream.read_next_coord()
#         return RecordCoord(line1, line2, line3, line4)

#     @always_inline
#     fn _parse_record2(inout self) raises -> RecordCoord:
#         var coords = self.stream.read_n_coords[4]()
#         var n = 0
#         if self.stream.buf[coords[0].start.value()] != read_header:
#             print(
#                 coords[n],
#                 StringRef(
#                     self.stream.buf.unsafe_ptr() + coords[n].start.value(),
#                     coords[n].end.value() - coords[n].start.value(),
#                 ),
#             )
#             raise Error("Sequence Header is corrupt")

#         if self.stream.buf[coords[2].start.value()] != quality_header:
#             raise Error("Quality Header is corrupt")

#         return RecordCoord(coords[0], coords[1], coords[2], coords[3])


fn main() raises:
    var n = 0
    var parser = RecordParser("data/M_abscessus_HiSeq.fq")

    # for i in range(500):
    #     l1 = parser.stream.read_line()
    #     l2 = parser.stream.read_line()
    #     l3 = parser.stream.read_line()
    #     l4 = parser.stream.read_line()
    #     l1.append(0)
    #     l2.append(0)
    #     l3.append(0)
    #     l4.append(0)
    #     l1.append(10)
    #     l2.append(10)
    #     l3.append(10)
    #     l4.append(10)
        # print(String(l1), String(l2), String(l3), String(l4))
    # print(r.__str__())
    var start = time.now()
    while True:
        try:
            var record = parser.next()
            n += 1
        except Error:
            print(n)
            break
    var end = time.now()
    print("Time taken: ", (end - start) / 1e9)
