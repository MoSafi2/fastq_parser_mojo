from math import align_down
from sys.info import sizeof
from blazeseq.CONSTS import (
    sanger_schema,
    solexa_schema,
    illumina_1_3_schema,
    illumina_1_5_schema,
    illumina_1_8,
    generic_schema,
)

alias width = sizeof[DType.uint8]()


@always_inline
fn arg_true[simd_width: Int](v: SIMD[DType.bool, simd_width]) -> Int:
    for i in range(simd_width):
        if v[i]:
            return i
    return -1


fn find_chr_next_occurance(
    buffer: UnsafePointer[UInt8], len: UInt, start: UInt, chr: UInt = 10
) -> Int:
    # print("find_chr_next_occurance", len, start)
    var aligned = start + align_down(len, width)
    for s in range(start, aligned, width):
        var v = buffer.load[width=width](s)
        var mask = v == chr
        if mask.reduce_or():
            return s + arg_true(mask)

    for i in range(aligned, len):
        if buffer[i] == chr:
            return i

    # for i in range(start, start + len):
    #     if buffer[i] == chr:
    #         return i

    return -1


@value
struct QualitySchema(Stringable, CollectionElement):
    var SCHEMA: StringLiteral
    var LOWER: UInt8
    var UPPER: UInt8
    var OFFSET: UInt8

    fn __init__(
        inout self, schema: StringLiteral, lower: Int, upper: Int, offset: Int
    ):
        self.SCHEMA = schema
        self.UPPER = upper
        self.LOWER = lower
        self.OFFSET = offset

    fn __str__(self) -> String:
        return (
            String("Quality schema: ")
            + self.SCHEMA
            + "\nLower: "
            + str(self.LOWER)
            + "\nUpper: "
            + str(self.UPPER)
            + "\nOffset: "
            + str(self.OFFSET)
        )


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


@always_inline
fn _validate_ascii(buffer: UnsafePointer[UInt8], len: UInt) raises:
    for i in range(len):
        if buffer[i] < 33 or buffer[i] > 126:
            raise Error("Invalid ASCII character found in the record.")
