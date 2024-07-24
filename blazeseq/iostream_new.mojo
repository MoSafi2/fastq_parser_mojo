from blazeseq.CONSTS import DEFAULT_CAPACITY
from pathlib import Path
from buffer import Buffer
from memory import memcpy
from builtin.file import _OwnedStringRef
from sys import external_call
from blazeseq.helpers_new import find_chr_next_occurance


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
        self._store(nels)
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

        self.head = line_end + 1

        return coord

    fn _line_coord_incomplete_line(inout self):
        pass

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
    var b = BufferedLineIterator(Path("data/fastqc_data.txt"))
    print(b._line_coord())
