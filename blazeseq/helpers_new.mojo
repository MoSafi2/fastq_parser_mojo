from math import align_down


alias width = sizeof[DType.uint8]()

@always_inline
fn arg_true[simd_width: Int](v: SIMD[DType.bool, simd_width]) -> Int:
    for i in range(simd_width):
        if v[i]:
            return i
    return -1


fn find_chr_next_occurance[T: DType](buffer: List[Scalar[T]], start: UInt, chr: UInt = 10) -> Int: 
    var length = len(buffer)
    var aligned = start + align_down(length, width)

    for s in range(start, aligned, width):
        var v = SIMD[T, width].load(buffer.unsafe_ptr(), offset = s)
        var mask = v == chr
        if mask.reduce_or():
            return s + arg_true(mask)

    for i in range(aligned, length):
        if buffer[i] == chr:
            return i
    return -1
