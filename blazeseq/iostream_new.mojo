from blazeseq.CONSTS import DEFAULT_CAPACITY
from pathlib import Path
from buffer import Buffer
from memory import memcpy
from builtin.file import _OwnedStringRef
from sys import external_call
from blazeseq.helpers_new import find_chr_next_occurance


alias carriage_return = 13
alias U8 = UInt8
alias MAX_CAPACITY = 128 * 1024
alias MAX_SHIFT = 30

struct BufferedLineIterator[check_ascii: Bool = False](Sized, Stringable):
    var buf: List[UInt8]
    var source: FileHandle
    var head: Int
    var end: Int
    var capacity: Int

    fn __init__(
        inout self, source: Path, capacity: Int = DEFAULT_CAPACITY
    ) raises:
        if source.exists():
            self.source = open(source, "r")
        else:
            raise Error("Provided file not found for read")

        self.buf = List[UInt8]()
        self.head = 0
        self.end = 0
        self.capacity = capacity
        _ = self._fill_buffer()

    fn _fill_buffer(inout self) raises -> Int:
        self._left_shift()
        var nels = self.uninatialized_space()
        self.buf = self.source.read_bytes(nels)

        if len(self.buf) == 0:
            raise Error("EOF")
        self.end += nels
        return len(self)

    fn _store[check_ascii: Bool = False](inout self, amt: Int) raises:
        """Calls the source to read n bytes."""
        self.buf = self.source.read_bytes(amt)
        self.end += amt

    fn _left_shift(inout self):
        if self.head == 0:
            return
        var no_items = len(self)
        memcpy(
            self.buf.unsafe_ptr(), self.buf.unsafe_ptr() + self.head, no_items
        )
        self.head = 0
        self.end = no_items

    fn _check_buf_state(inout self) -> Bool:
        if self.head >= self.end:
            self.head = 0
            self.end = 0
            return True
        else:
            return False

    @always_inline
    fn _line_coord(inout self) raises -> Slice:
        if self._check_buf_state():
            _ = self._fill_buffer()

        var coord: Slice
        var line_start = self.head
        var line_end = find_chr_next_occurance[DType.uint8](
            self.buf, start=self.head
        )

        coord = Slice(line_start, line_end)

        # Handle small buffers
        if coord.end.value() == -1 and self.head == 0:
            for i in range(MAX_SHIFT):
                if coord.end.value() != -1:
                    return self._handle_windows_sep(coord)
                else:
                    coord = self._line_coord_missing_line()

        
        # Handle incomplete lines across two chunks
        if coord.end.value() == -1:
            _ = self._fill_buffer()
            return self._handle_windows_sep(self._line_coord_incomplete_line())

        self.head = line_end + 1
        # Handling Windows-syle line seperator
        if self.buf[line_end] == carriage_return:
            line_end -= 1
        return slice(line_start, line_end)

    
    fn _line_coord_incomplete_line(inout self) raises -> Slice:
        if self._check_buf_state():
            _ = self._fill_buffer()
        var line_start = self.head
        var line_end = find_chr_next_occurance(self.buf, self.head)
        self.head = line_end + 1

        if self.buf[line_end] == carriage_return:
            line_end -= 1
        return slice(line_start, line_end)

    
    @always_inline
    fn _line_coord_missing_line(inout self) raises -> Slice:
        self._resize_buf(self.get_capacity(), MAX_CAPACITY)
        _ = self._fill_buffer()
        var line_start = self.head
        var line_end = find_chr_next_occurance(self.buf, self.head)
        self.head = line_end + 1

        return slice(line_start, line_end)
    

    @always_inline
    fn _resize_buf(inout self, amt: Int, max_capacity: Int) raises:
        if self.get_capacity() == max_capacity:
            raise Error("Buffer is at max capacity")

        var nels: Int
        if self.get_capacity() + amt > max_capacity:
            nels = max_capacity
        else:
            nels = self.get_capacity() + amt
        var x = List[U8](nels)
        var nels_to_copy = min(self.get_capacity(), self.get_capacity() + amt)
        for i in range(nels_to_copy):
            x[i] = self.buf[i]
        self.buf = x


    
    @always_inline
    fn _handle_windows_sep(self, in_slice: Slice) -> Slice:
        if self.buf[in_slice.end.value()] != carriage_return:
            return in_slice
        return Slice(in_slice.start.value(), in_slice.end.value() - 1)

    fn usable_space(self) -> Int:
        return self.uninatialized_space() + self.head

    fn uninatialized_space(self) -> Int:
        return self.get_capacity() - self.end

    fn get_capacity(self) -> Int:
        return self.capacity

    fn __len__(self) -> Int:
        return self.end - self.head

    fn __str__(self) -> String:
        return ""

    fn __getitem__[
        width: Int = 1
    ](self, index: Int) -> SIMD[DType.uint8, width]:
        if self.head <= index <= self.end:
            return SIMD[DType.uint8, width](index)
        else:
            return SIMD[DType.uint8, size=width](0)


fn main() raises:
    var b = BufferedLineIterator(Path("data/TESTX_H7YRLADXX_S1_L001_R1_001.fastq"))
    var n = 0
    while True:
        try:
            _ = b._line_coord()
            n += 1
        except:
            break
    print(n)
