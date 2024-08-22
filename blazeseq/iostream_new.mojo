from blazeseq.CONSTS import DEFAULT_CAPACITY
from pathlib import Path
from buffer import Buffer
from memory import memcpy, memset_zero
from builtin.file import _OwnedStringRef
from sys import external_call
from blazeseq.helpers_new import find_chr_next_occurance
from utils.span import Span

alias carriage_return = 13
alias U8 = UInt8
alias MAX_CAPACITY = 128 * 1024
alias MAX_SHIFT = 30


struct BufferedReader[check_ascii: Bool = False](Sized):
    var buf: UnsafePointer[UInt8]
    var source: FileHandle
    var head: Int
    var end: Int
    var capacity: Int

    ###-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
    ###---------------------------------------------------------------  Dunder Methods  ----------------------------------------------------------------------------------------------------------------------------###
    ###-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

    fn __init__(
        inout self, source: Path, capacity: Int = DEFAULT_CAPACITY
    ) raises:
        if source.exists():
            self.source = open(source, "r")
        else:
            raise Error("Provided file not found for read")

        self.buf = UnsafePointer[UInt8]().alloc(capacity)

        memset_zero(self.buf, capacity)

        self.head = 0
        self.end = 0
        self.capacity = capacity

        _ = self._fill_buffer_init()

    # TODO: Add the slice version of this
    @always_inline
    fn __getitem__(self, idx: Int) -> UInt8:
        """Get a single  value at the given index."""
        return self.buf.load(idx)

    # TODO: Add the slice version of this
    @always_inline
    fn __setitem__(self, idx: Int, value: UInt8):
        """Set a single value at the given index."""
        self.buf.store(idx, value)

    ###-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
    ###--------------------------------------------------------------  Private methods with no side effect----------------------------------------------------------------------------------------------------------###
    ###-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

    @always_inline
    fn get_capacity(self) -> Int:
        """Get the capacity of the buffer."""
        return self.capacity

    @always_inline
    fn uninatialized_space(self) -> Int:
        """Get the uninitialized space in the buffer."""
        return self.get_capacity() - self.end

    @always_inline
    fn len(self) -> Int:
        """Get the length of the data in the buffer."""
        return self.end - self.head

    @always_inline
    fn __len__(self) -> Int:
        """Alias for len."""
        return self.len()

    @always_inline
    fn _check_buf_state(inout self) -> Bool:
        """Check if the buffer is empty."""
        if self.head >= self.end:
            return True
        else:
            return False

    ###-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
    ###--------------------------------------------------------------  Private methods with side effect-------------------------------------------------------------------------------------------------------------###
    ###-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

    @always_inline
    fn _reset_buffer(inout self):
        """Reset the buffer head and end, in effect emptying the buffer."""
        self.head = 0
        self.end = 0

    @always_inline
    fn _fill_buffer_init(inout self) raises -> Int:
        """Fill the buffer initially with data from the source. Avoids the overhead of left shifting.
        """
        var nels = self.uninatialized_space()
        var nels_read = self.source.read(self.buf, nels)
        if nels_read == 0:
            raise Error("EOF")
        self.end = int(nels_read)
        return len(nels_read)

    @always_inline
    fn _left_shift(inout self):
        """Shift the remaining elements of the buffer to the left to remove the consumed data.
        """
        if self.head == 0:
            return
        var no_items = self.end - self.head
        memcpy(self.buf, self.buf + self.head, no_items)
        self.head = 0
        self.end = no_items

    @always_inline
    fn _fill_buffer(inout self) raises -> Int:
        """Fill the buffer with data from the source. If the buffer is not empty, left shift the buffer to make space for new data.
        returns: the new length of the buffer.
        """
        if self._check_buf_state():
            self._reset_buffer()
        else:
            self._left_shift()

        var nels = self.uninatialized_space()
        var nels_read = self.source.read(self.buf, nels)
        if nels_read == 0:
            raise Error("EOF")
        self.end += int(nels_read)
        return self.len()

    @always_inline
    fn _resize_buf(inout self, amt: Int, max_capacity: Int) raises -> None:
        """Resize the buffer to accommodate more data. If the new capacity exceeds the max capacity, the buffer is resized to the max capacity.
        """

        if self.get_capacity() == max_capacity:
            raise Error("Buffer is at max capacity")

        var nels: Int
        if self.get_capacity() + amt > max_capacity:
            nels = max_capacity
        else:
            nels = self.get_capacity() + amt

        var new_buf = UnsafePointer[UInt8]().alloc(nels)

        var nels_to_copy = min(self.get_capacity(), self.get_capacity() + amt)
        memcpy(new_buf, self.buf, nels_to_copy)

        self.buf = new_buf
        self.capacity = nels

    ###-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
    ###--------------------------------------------------------------  Public methods with side effect--------------------------------------------------------------------------------------------------------------###
    ###-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

    @always_inline
    fn read(inout self, n: Int) raises -> List[UInt8]:
        """Read n bytes from the buffer."""

        if self._check_buf_state():
            self._reset_buffer()
            _ = self._fill_buffer()

        var nels = min(n, self.len())
        var data = List[UInt8](capacity=nels)
        data.size = nels
        memcpy(data.unsafe_ptr(), self.buf + self.head, nels)
        self.head += nels
        return data

    @always_inline
    fn read_span(
        inout self, n: Int
    ) raises -> Span[is_mutable=False, T=UInt8, lifetime = __lifetime_of(self)]:
        """Read n bytes from the buffer."""
        if self._check_buf_state():
            self._reset_buffer()
            _ = self._fill_buffer()

        var nels = min(n, self.len())
        var data = Span[
            is_mutable=False, T=UInt8, lifetime = __lifetime_of(self)
        ](unsafe_ptr=self.buf + self.head, len=nels)
        self.head += nels
        return data

    # # TODO: Make this functional in the end
    # @always_inline
    # fn _handle_windows_sep(self, in_slice: Slice) -> Slice:
    #     return in_slice
    #     # if self.buf[in_slice.end.value()] != carriage_return:
    #     #     return in_slice
    #     # return Slice(in_slice.start.value(), in_slice.end.value() - 1)

    # fn __getitem__[
    #     width: Int = 1
    # ](self, index: Int) -> SIMD[DType.uint8, width]:
    #     if self.head <= index <= self.end:
    #         return SIMD[DType.uint8, width](index)
    #     else:
    #         return SIMD[DType.uint8, size=width](0)

    # @always_inline
    # fn _line_coord(inout self) raises -> Slice:
    #     # Normal state
    #     if self._check_buf_state():
    #         _ = self._fill_buffer()

    #     var coord: Slice
    #     var line_start = self.head
    #     var line_end = find_chr_next_occurance[DType.uint8](
    #         self.buf, start=self.head
    #     )

    #     coord = Slice(line_start, line_end)

    #     # Handle small buffers
    #     if coord.end.value() == -1 and self.head == 0:
    #         for i in range(MAX_SHIFT):
    #             if coord.end.value() != -1:
    #                 return self._handle_windows_sep(coord)
    #             else:
    #                 coord = self._line_coord_missing_line()

    #     # Handle incomplete lines across two chunks
    #     if coord.end.value() == -1:
    #         print("incomplete lines across two chunks")
    #         _ = self._fill_buffer()
    #         return self._handle_windows_sep(self._line_coord_incomplete_line())

    #     self.head = line_end + 1

    #     # Handling Windows-syle line seperator
    #     if self.buf[line_end] == carriage_return:
    #         line_end -= 1
    #     var s = slice(line_start, line_end)
    #     return s

    # fn _line_coord_incomplete_line(inout self) raises -> Slice:
    #     if self._check_buf_state():
    #         _ = self._fill_buffer()
    #     var line_start = self.head
    #     var line_end = find_chr_next_occurance(self.buf, self.head)
    #     self.head = line_end + 1

    #     # if self.buf[line_end] == carriage_return:
    #     #     line_end -= 1

    #     var s = slice(line_start, line_end)
    #     return s

    # @always_inline
    # fn _line_coord_missing_line(inout self) raises -> Slice:
    #     self._resize_buf(self.get_capacity(), MAX_CAPACITY)
    #     _ = self._fill_buffer()
    #     var line_start = self.head
    #     var line_end = find_chr_next_occurance(self.buf, self.head)
    #     self.head = line_end + 1
    #     var s = slice(line_start, line_end)
    #     return s


fn main() raises:
    from pathlib import Path

    var b = BufferedReader(
        Path(
            "/home/mohamed/Documents/Projects/BlazeSeq/data/M_abscessus_HiSeq.fq"
        ),
        64 * 1024,
    )

    # for i in range(30):
    #     print(b[i])

    while True:
        var s = b.read(100)
        print(String(s))

    # for i in range(10):
    #     print(s[i])
