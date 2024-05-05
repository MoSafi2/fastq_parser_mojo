from memory.memory import memcpy
from blazeseq.helpers import get_next_line_index, slice_tensor, cpy_tensor
from blazeseq.CONSTS import (
    simd_width,
    I8,
    DEFAULT_CAPACITY,
    MAX_CAPACITY,
    MAX_SHIFT,
    carriage_return,
)
from pathlib import Path
import time
from tensor import Tensor
from algorithm.swap import swap
from utils.static_tuple import InlineArray



# Implement functionality from: Buffer-Reudx rust cate allowing for BufferedReader that supports partial reading and filling ,
# https://github.com/dignifiedquire/buffer-redux
# Minimial Implementation that support only line iterations

# BUG in resizing buffer: One extra line & bad consumed and file coordinates.


trait reader:
    fn read_bytes(inout self, amt: Int) raises -> Tensor[I8]:
        ...

    fn read_to_buffer(
        inout self, inout buf: Tensor[I8], buf_pos: Int, amt: Int
    ) raises -> Int:
        ...

    fn __moveinit__(inout self, owned other: Self):
        ...


struct FileReader(reader):
    var handle: FileHandle

    fn __init__(inout self, path: Path) raises:
        self.handle = open(path, "r")

    @always_inline
    fn read_bytes(inout self, amt: Int = -1) raises -> Tensor[I8]:
        return self.handle.read_bytes(amt)

    # Does not work well currently
    @always_inline
    fn read_to_buffer(
        inout self, inout buf: Tensor[I8], buf_pos: Int, amt: Int
    ) raises -> Int:
        var out = self.read_bytes(amt)
        if out.num_elements() == 0:
            return 0
        cpy_tensor[I8](buf, out, out.num_elements(), buf_pos, 0)
        return out.num_elements()

    fn __moveinit__(inout self, owned other: Self):
        self.handle = other.handle ^


struct TensorReader(reader):
    var pos: Int
    var source: Tensor[I8]

    fn __init__(inout self, source: Tensor[I8]):
        self.source = source
        self.pos = 0

    @always_inline
    fn read_bytes(inout self, amt: Int) raises -> Tensor[I8]:
        var ele = min(amt, self.source.num_elements() - self.pos)

        if ele == 0:
            return Tensor[I8](0)
        var out = Tensor[I8](ele)
        cpy_tensor[I8](out, self.source, out.num_elements(), 0, self.pos)
        self.pos += out.num_elements()
        return out

    fn read_to_buffer(
        inout self, inout buf: Tensor[I8], buf_pos: Int, amt: Int
    ) raises -> Int:
        var ele = min(amt, self.source.num_elements() - self.pos)
        if ele == 0:
            return 0
        cpy_tensor[I8](buf, self.source, ele, buf_pos, self.pos)
        self.pos += ele
        return ele

    fn __moveinit__(inout self, owned other: Self):
        self.source = other.source ^
        self.pos = other.pos


# BUG Last line is not returned if the file does not end with line end seperator
# TODO: when in EOF Flush the buffer


struct BufferedLineIterator[T: reader, check_ascii: Bool = False](Sized, Stringable):
    """A poor man's BufferedReader and LineIterator that takes as input a FileHandle or an in-memory Tensor and provides a buffered reader on-top with default capactiy.
    """

    var source: FileReader
    var buf: Tensor[I8]
    var head: Int
    var end: Int
    var consumed: Int

    fn __init__(inout self, source: Path, capacity: Int = DEFAULT_CAPACITY) raises:
        if source.exists():
            self.source = FileReader(source)
        else:
            raise Error("Provided file not found for read")
        self.buf = Tensor[I8](capacity)
        self.head = 0
        self.end = 0
        self.consumed = 0
        _ = self._fill_buffer()
        self.consumed = 0  # Hack to make the initial buffer full non-consuming

    # fn __init__(
    #     inout self, source: Tensor[I8], capacity: Int = DEFAULT_CAPACITY
    # ) raises:
    #     self.source = TensorReader(source)
    #     self.buf = Tensor[I8](capacity)
    #     self.head = 0
    #     self.end = 0
    #     self.consumed = 0
    #     _ = self._fill_buffer()
    #     self.consumed = 0  # Hack to make the initial buffer full non-consuming

    # fn __init__(inout self, owned source: T, capacity: Int = DEFAULT_CAPACITY) raises:
    #     self.source = source^
    #     self.buf = Tensor[I8](capacity)
    #     self.head = 0
    #     self.end = 0
    #     self.consumed = 0
    #     _ = self._fill_buffer()
    #     self.consumed = 0  # Hack to make the initial buffer full non-consuming

    @always_inline
    fn read_next_line(inout self) raises -> Tensor[I8]:
        var line_coord = self._line_coord()
        return slice_tensor[I8](self.buf, line_coord.start, line_coord.end)

    @always_inline
    fn read_next_coord(inout self) raises -> Slice:
        var line_coord = self._line_coord()
        return slice(line_coord.start + self.consumed, line_coord.end + self.consumed)

    @always_inline
    fn _fill_buffer(inout self) raises -> Int:
        """Returns the number of bytes read into the buffer."""
        self._left_shift()
        var nels = self.uninatialized_space()
        var in_buf = self.source.read_bytes(nels)
        if in_buf.num_elements() == 0:
            raise Error("EOF")

        if in_buf.num_elements() < nels:
            self._resize_buf(in_buf.num_elements() - nels, MAX_CAPACITY)

        self._store[self.check_ascii](in_buf, in_buf.num_elements())
        self.consumed += nels
        return in_buf.num_elements()

    # if coord.end == -1:
    #     # Handle small buffers
    #     if self.head == 0:
    #         for i in range(MAX_SHIFT):
    #             if coord.end != -1:
    #                 return coord
    #             else:
    #                 coord = self._line_coord_missing_line()

    #     # Handle incomplete lines across two chunks
    #     _ = self._fill_buffer()
    #     return self._line_coord2()

    @always_inline
    fn _line_coord(inout self) raises -> Slice:
        if self._check_buf_state():
            _ = self._fill_buffer()

        var coord: Slice
        var line_start = self.head
        var line_end = get_next_line_index(self.buf, self.head)

        coord = Slice(line_start, line_end)

        # Handle small buffers
        if coord.end == -1 and self.head == 0:
            for i in range(MAX_SHIFT):
                if coord.end != -1:
                    return self._handle_windows_sep(coord)
                else:
                    coord = self._line_coord_missing_line()

        # Handle incomplete lines across two chunks
        if coord.end == -1:
            _ = self._fill_buffer()
            return self._handle_windows_sep(self._line_coord_incomplete_line())

        self.head = line_end + 1

        # Handling Windows-syle line seperator
        if self.buf[line_end] == carriage_return:
            line_end -= 1

        return slice(line_start, line_end)


    # BUG: Crashes at new chunk read
    fn _read_n_line[no: Int](inout self) raises -> InlineArray[Slice, no]:

        var coords = InlineArray[Slice, no](Slice(-1, -1))
        var internal_head = self.head

        for i in range(no):
            if self._check_buf_state():
                _ = self._fill_buffer()
            var coord: Slice
            var line_start = internal_head
            var line_end = get_next_line_index(self.buf, internal_head)

            coord = Slice(line_start, line_end)

            # Handle incomplete lines across two chunks
            if coord.end == -1:
                _ = self._fill_buffer()
                coords[i] =  self._line_coord_incomplete_line()
            internal_head = line_end + 1
            coords[i] =  slice(line_start, line_end)
        self.head = internal_head
        return coords




    @always_inline
    fn _line_coord_incomplete_line(inout self) raises -> Slice:
        if self._check_buf_state():
            _ = self._fill_buffer()
        var line_start = self.head
        var line_end = get_next_line_index(self.buf, self.head)
        self.head = line_end + 1

        if self.buf[line_end] == carriage_return:
            line_end -= 1

        return slice(line_start, line_end)

    @always_inline
    fn _line_coord_missing_line(inout self) raises -> Slice:
        self._resize_buf(self.capacity(), MAX_CAPACITY)
        _ = self._fill_buffer()
        var line_start = self.head
        var line_end = get_next_line_index(self.buf, self.head)

        self.head = line_end + 1

        return slice(line_start, line_end)

    @always_inline
    fn _store[
        check_ascii: Bool = False
    ](inout self, in_tensor: Tensor[I8], amt: Int) raises:
        @parameter
        if check_ascii:
            self._check_ascii(in_tensor)
        cpy_tensor[I8](self.buf, in_tensor, amt, self.end, 0)
        self.end += amt

    @always_inline
    fn _left_shift(inout self):
        if self.head == 0:
            return
        var no_items = self.len()
        cpy_tensor[I8](self.buf, self.buf, no_items, 0, self.head)
        self.head = 0
        self.end = no_items


    @always_inline
    fn _check_buf_state(inout self) -> Bool:
        if self.head >= self.end:
            self.head = 0
            self.end = 0
            return True
        else:
            return False

    @always_inline
    fn _resize_buf(inout self, amt: Int, max_capacity: Int) raises:
        if self.capacity() == max_capacity:
            raise Error("Buffer is at max capacity")

        var nels: Int
        if self.capacity() + amt > max_capacity:
            nels = max_capacity
        else:
            nels = self.capacity() + amt
        var x = Tensor[I8](nels)
        var nels_to_copy = min(self.capacity(), self.capacity() + amt)
        cpy_tensor[I8](x, self.buf, nels_to_copy, 0, 0)
        self.buf = x

    @always_inline
    @staticmethod
    fn _check_ascii(in_tensor: Tensor[I8]) raises:
        var aligned = math.align_down(in_tensor.num_elements(), simd_width)
        # alias bit_mask = 0xA0  # Between 32 and 127, makes a problems with 10
        alias bit_mask = 0x80  # Non negative
        for i in range(0, aligned, simd_width):
            var vec = in_tensor.load[width=simd_width](i)
            var mask = vec & bit_mask
            for i in range(len(mask)):
                if mask[i] != 0:
                    raise Error("Non ASCII letters found")

        for i in range(aligned, in_tensor.num_elements()):
            if in_tensor[i] & bit_mask != 0:
                raise Error("Non ASCII letters found")

    @always_inline
    fn _handle_windows_sep(self, in_slice: Slice) -> Slice:
        if self.buf[in_slice.end] != carriage_return:
            return in_slice
        return Slice(in_slice.start, in_slice.end - 1)

    ########################## Helpers functions, have no side effects #######################

    @always_inline
    fn map_pos_2_buf(self, file_pos: Int) -> Int:
        return file_pos - self.consumed

    @always_inline
    fn len(self) -> Int:
        return self.end - self.head

    @always_inline
    fn capacity(self) -> Int:
        return self.buf.num_elements()

    @always_inline
    fn uninatialized_space(self) -> Int:
        return self.capacity() - self.end

    @always_inline
    fn usable_space(self) -> Int:
        return self.uninatialized_space() + self.head

    @always_inline
    fn __len__(self) -> Int:
        return self.end - self.head

    @always_inline
    fn __str__(self) -> String:
        var out = Tensor[I8](self.len())
        cpy_tensor[I8](out, self.buf, self.len(), 0, self.head)
        return String(out._steal_ptr(), self.len())

    fn __getitem__(self, index: Int) raises -> Scalar[I8]:
        if self.head <= index <= self.end:
            return self.buf[index]
        else:
            raise Error("Out of bounds")

    fn __getitem__(self, slice: Slice) raises -> Tensor[I8]:
        if slice.start >= self.head and slice.end <= self.end:
            var out = Tensor[I8](slice.end - slice.start)
            cpy_tensor[I8](out, self.buf, slice.end - slice.start, 0, slice.start)
            return out
        else:
            raise Error("Out of bounds")

#TODO: Add a resize if the buffer is too small
struct BufferedWriter:
    var sink: FileHandle
    var buf: Tensor[DType.int8]
    var cursor: Int
    var written: Int

    fn __init__(inout self, out_path: String, buf_size: Int) raises:
        self.sink = open(out_path, "w")
        self.buf = Tensor[I8](buf_size)
        self.cursor = 0
        self.written = 0

    fn ingest(inout self, source: Tensor[I8]) raises -> Bool:
        if source.num_elements() > self.uninatialized_space():
            self.flush_buffer()
        cpy_tensor[I8](self.buf, source, source.num_elements(), self.cursor, 0)
        self.cursor += source.num_elements()
        return True

    fn flush_buffer(inout self) raises:
        var out = Tensor[I8](self.cursor)
        cpy_tensor[I8](out, self.buf, self.cursor, 0, 0)
        var out_string = StringRef(out._steal_ptr(), self.cursor)
        self.sink.write(out_string)
        self.written += self.cursor
        self.cursor = 0

    fn _resize_buf(inout self, amt: Int, max_capacity: Int = MAX_CAPACITY):
        var new_capacity = 0
        if self.buf.num_elements() + amt > max_capacity:
            new_capacity = max_capacity
        else:
            new_capacity = self.buf.num_elements() + amt
        var new_tensor = Tensor[I8](new_capacity)
        cpy_tensor[I8](new_tensor, self.buf, self.cursor, 0, 0)
        swap(self.buf, new_tensor)

    fn uninatialized_space(self) -> Int:
        return self.capacity() - self.cursor

    fn capacity(self) -> Int:
        return self.buf.num_elements()

    fn close(inout self) raises:
        self.flush_buffer()
        self.sink.close()

