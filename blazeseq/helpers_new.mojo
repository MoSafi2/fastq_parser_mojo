from math import align_down
from sys.info import sizeof

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
    # var aligned = start + align_down(len, width)
    # for s in range(start, aligned, width):
    #     var v = buffer.load[width=width](s)
    #     var mask = v == chr
    #     if mask.reduce_or():
    #         return s + arg_true(mask)

    # for i in range(aligned, len):
    #     if buffer[i] == chr:
    #         return i

    for i in range(start, start + len):
        if buffer[i] == chr:
            return i
    return -1
