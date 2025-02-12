import math
from algorithm import vectorize
from blazeseq.CONSTS import *
from tensor import Tensor
from collections.vector import *
from memory import Span
from tensor import Tensor, TensorShape
from collections.list import List
from memory import memcpy, pack_bits
from bit import count_leading_zeros, count_trailing_zeros
from python import PythonObject, Python

######################### Character find functions ###################################

alias NEW_LINE = 10
alias LOOP_SIZE = SIMD_U8_WIDTH * 4
alias SIMD_U8_WIDTH: Int = simdwidthof[DType.uint8]()


@always_inline
fn get_next_line[
    is_mutable: Bool, //, origin: Origin[is_mutable]
](buffer: Span[UInt8, origin], start: Int) -> Int:
    """Function to get the next line using either SIMD instruction (default) or iteratively.
    """

    var in_start = start
    while buffer[in_start] == NEW_LINE:  # Skip leading \n
        in_start += 1
        if in_start >= len(buffer):
            return 0

    var next_line_pos = memchr_wide(buffer, NEW_LINE, in_start)
    if next_line_pos == -1:
        next_line_pos = len(
            buffer
        )  # If no line separator found, return the reminder of the string, behavior subject to change
    return next_line_pos


@always_inline("nodebug")
fn memchr_wide(haystack: Span[UInt8], chr: UInt8, start: Int = 0) -> Int:
    """
    Function to find the next occurrence of character.
    """
    if len(haystack[start:]) < LOOP_SIZE:
        for i in range(start, len(haystack)):
            if haystack[i] == chr:
                return i
        return -1

    # Do an unaligned initial read, it doesn't matter that this will overlap the next portion
    var ptr = haystack[start:].unsafe_ptr()
    var v = ptr.load[width=SIMD_U8_WIDTH]()
    var mask = v == chr

    var packed = pack_bits(mask)
    if packed:
        var index = Int(count_trailing_zeros(packed))
        return index + start

    # Now get the alignment
    var offset = SIMD_U8_WIDTH - (ptr.__int__() & (SIMD_U8_WIDTH - 1))
    var aligned_ptr = ptr.offset(offset)

    # Find the last aligned end
    var haystack_len = len(haystack) - (start + offset)
    var aligned_end = math.align_down(
        haystack_len, LOOP_SIZE
    )  # relative to start + offset

    # Now do aligned reads all through
    for s in range(0, aligned_end, LOOP_SIZE):
        var a = aligned_ptr.load[width=SIMD_U8_WIDTH](s)
        var b = aligned_ptr.load[width=SIMD_U8_WIDTH](s + 1 * SIMD_U8_WIDTH)
        var c = aligned_ptr.load[width=SIMD_U8_WIDTH](s + 2 * SIMD_U8_WIDTH)
        var d = aligned_ptr.load[width=SIMD_U8_WIDTH](s + 3 * SIMD_U8_WIDTH)
        var eqa = a == chr
        var eqb = b == chr
        var eqc = c == chr
        var eqd = d == chr
        var or1 = eqa | eqb
        var or2 = eqc | eqd
        var or3 = or1 | or2

        var packed = pack_bits(or3)
        if packed:
            # Now check each register knowing we have a match
            var packed_a = pack_bits(eqa)
            if packed_a:
                var index = Int(count_trailing_zeros(packed_a))
                return s + index + offset + start
            var packed_b = pack_bits(eqb)
            if packed_b:
                var index = Int(count_trailing_zeros(packed_b))
                return s + (1 * SIMD_U8_WIDTH) + index + offset + start
            var packed_c = pack_bits(eqc)
            if packed_c:
                var index = Int(count_trailing_zeros(packed_c))
                return s + (2 * SIMD_U8_WIDTH) + index + offset + start

            var packed_d = pack_bits(eqd)
            var index = Int(count_trailing_zeros(packed_d))
            return s + (3 * SIMD_U8_WIDTH) + index + offset + start

    # Now by single SIMD jumps
    var single_simd_end = math.align_down(
        haystack_len, SIMD_U8_WIDTH
    )  # relative to start + offset
    for s in range(aligned_end, single_simd_end, SIMD_U8_WIDTH):
        var v = aligned_ptr.load[width=SIMD_U8_WIDTH](s)
        var mask = v == chr

        var packed = pack_bits(mask)
        if packed:
            var index = Int(count_trailing_zeros(packed))
            return s + index + offset + start

    # Finish and last bytes
    for i in range(single_simd_end + start + offset, len(haystack)):
        if haystack[i] == chr:
            return i

    return -1


@always_inline
fn arg_true[simd_width: Int](v: SIMD[DType.bool, simd_width]) -> Int:
    for i in range(simd_width):
        if v[i]:
            return i
    return -1


@always_inline
fn find_chr_next_occurance[
    T: DType
](in_tensor: Tensor[T], chr: Int, start: Int = 0) -> Int:
    """
    Function to find the next occurance of character using SIMD instruction.
    Checks are in-bound. no-risk of overflowing the tensor.
    """
    var len = in_tensor.num_elements() - start
    var aligned = start + math.align_down(len, simd_width)

    for s in range(start, aligned, simd_width):
        var v = in_tensor.load[width=simd_width](s)
        x = v.cast[DType.uint8]()
        var mask = x == chr
        if mask.reduce_or():
            return s + arg_true(mask)

    for i in range(aligned, in_tensor.num_elements()):
        if in_tensor[i] == chr:
            return i
    return -1


fn find_chr_last_occurance[
    T: DType
](in_tensor: Tensor[T], start: Int, end: Int, chr: Int) -> Int:
    for i in range(end - 1, start - 1, -1):
        if in_tensor[i] == chr:
            return i
    return -1


@always_inline
fn _align_down(value: Int, alignment: Int) -> Int:
    return value._positive_div(alignment) * alignment


################################ Tensor slicing ################################################


@always_inline
fn slice_tensor[
    T: DType, USE_SIMD: Bool = True
](in_tensor: Tensor[T], start: Int, end: Int) -> Tensor[T]:
    if start >= end:
        return Tensor[T](0)

    @parameter
    if USE_SIMD:
        return slice_tensor_simd(in_tensor, start, end)
    else:
        return slice_tensor_iter(in_tensor, start, end)


@always_inline
fn slice_tensor_simd[
    T: DType
](in_tensor: Tensor[T], start: Int, end: Int) -> Tensor[T]:
    """
    Generic Function that returns a python-style tensor slice from start till end (not inclusive).
    """

    var out_tensor: Tensor[T] = Tensor[T](end - start)

    @parameter
    fn inner[simd_width: Int](size: Int):
        var transfer = in_tensor.load[width=simd_width](start + size)
        out_tensor.store[width=simd_width](size, transfer)

    vectorize[inner, simd_width](out_tensor.num_elements())

    return out_tensor


@always_inline
fn slice_tensor_iter[
    T: DType
](in_tensor: Tensor[T], start: Int, end: Int) -> Tensor[T]:
    var out_tensor = Tensor[T](end - start)
    for i in range(start, end):
        out_tensor[i - start] = in_tensor[i]
    return out_tensor


@always_inline
fn write_to_buff[T: DType](src: Tensor[T], mut dest: Tensor[T], start: Int):
    """
    Copy a small tensor into a larger tensor given an index at the large tensor.
    Implemented iteratively due to small gain from copying less then 1MB tensor using SIMD.
    Assumes copying is always in bounds. Bound checking is the responsbility of the caller.
    """
    for i in range(src.num_elements()):
        dest[start + i] = src[i]


# The Function does not provide bounds checks on purpose, the bounds checks is the callers responsibility
@always_inline
fn cpy_tensor[
    T: DType
    # simd_width: Int
](
    mut dest: Tensor[T],
    src: Tensor[T],
    num_elements: Int,
    dest_strt: Int = 0,
    src_strt: Int = 0,
):
    var dest_ptr: UnsafePointer[Scalar[T]] = dest._ptr + dest_strt
    var src_ptr: UnsafePointer[Scalar[T]] = src._ptr + src_strt
    memcpy(dest_ptr, src_ptr, num_elements)


################################ Next line Ops ##############################

# The next line OPs is dependent on find_chr_next_occurance and slice_tensor


@always_inline
fn get_next_line[T: DType](in_tensor: Tensor[T], start: Int) -> Tensor[T]:
    """Function to get the next line using either SIMD instruction (default) or iterativly.
    """

    var in_start = start
    while in_tensor[in_start] == new_line:  # Skip leadin \n
        print("skipping \n")
        in_start += 1
        if in_start >= in_tensor.num_elements():
            return Tensor[T](0)

    var next_line_pos = find_chr_next_occurance(in_tensor, new_line, in_start)
    if next_line_pos == -1:
        next_line_pos = (
            in_tensor.num_elements()
        )  # If no line separator found, return the reminder of the string, behaviour subject to change
    return slice_tensor_simd(in_tensor, in_start, next_line_pos)


@always_inline
fn get_next_line_index[T: DType](in_tensor: Tensor[T], start: Int) -> Int:
    var in_start = start

    var next_line_pos = find_chr_next_occurance(in_tensor, new_line, in_start)
    if next_line_pos == -1:
        return -1
    return next_line_pos


############################# Fastq recod-related Ops ################################


fn find_last_read_header(
    in_tensor: Tensor[U8], start: Int = 0, end: Int = -1
) -> Int:
    var end_inner: Int
    if end == -1:
        end_inner = in_tensor.num_elements()
    else:
        end_inner = end

    var last_chr = find_chr_last_occurance(
        in_tensor, start, end_inner, read_header
    )
    if in_tensor[last_chr - 1] == new_line:
        return last_chr
    else:
        end_inner = last_chr
        if (end_inner - start) < 4:
            return -1
        last_chr = find_last_read_header(in_tensor, start, end_inner)
    return last_chr


@value
struct QualitySchema(Stringable, CollectionElement, Writable):
    var SCHEMA: StringLiteral
    var LOWER: UInt8
    var UPPER: UInt8
    var OFFSET: UInt8

    fn __init__(
        mut self, schema: StringLiteral, lower: Int, upper: Int, offset: Int
    ):
        self.SCHEMA = schema
        self.UPPER = upper
        self.LOWER = lower
        self.OFFSET = offset

    fn write_to[w: Writer](self, mut writer: w) -> None:
        writer.write(self.__str__())

    fn __str__(self) -> String:
        return (
            String("Quality schema: ")
            + self.SCHEMA
            + "\nLower: "
            + String(self.LOWER)
            + "\nUpper: "
            + String(self.UPPER)
            + "\nOffset: "
            + String(self.OFFSET)
        )
