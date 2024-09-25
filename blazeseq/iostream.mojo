from blazeseq.CONSTS import DEFAULT_CAPACITY
from pathlib import Path
from buffer import Buffer
from memory import memcpy, memset_zero, UnsafePointer
from builtin.file import _OwnedStringRef
from sys import external_call
from blazeseq.helpers import find_chr_next_occurance
from utils.span import Span
from buffer import Buffer
from utils.stringref import StringRef


alias carriage_return = 13
alias U8 = UInt8
alias MAX_CAPACITY = 128 * 1024
alias MAX_SHIFT = 30


struct BufferedReader(Sized):
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

    @always_inline
    fn __getitem__(self, idx: Int) -> UInt8:
        """Get a single  value at the given index."""
        return self.buf.load(idx)

    # From the mojo implementation for list,probably slow.
    @always_inline
    fn __getitem__(self, span: Slice) -> List[UInt8]:
        """Gets the sequence of elements at the specified positions.

        Args:
            span: A slice that specifies positions of the new list.

        Returns:
            A new list containing the list at the specified span.
        """

        var start: Int
        var end: Int
        var step: Int

        # Slice bound checking is done here
        start, end, step = span.indices(len(self))
        var r = range(start, end, step)

        if not len(r):
            return List[UInt8]()

        var res = List[UInt8](capacity=len(r))
        memcpy(res.unsafe_ptr(), self.buf + start, len(r))
        res.size = len(r)
        return res

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
        memcpy(self.buf, self.buf + self.head, self.len())
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
        var nels_read = self.source.read(self.buf + self.end, nels)
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

    fn _skip_delim(inout self) raises:
        """Skips one byte of the buffer."""
        self.head += 1

        # if self._check_buf_state():
        #     self._reset_buffer()
        #     _ = self._fill_buffer()

    ###-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
    ###--------------------------------------------------------------  Public methods with side effect--------------------------------------------------------------------------------------------------------------###
    ###-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###

    @always_inline
    fn read(inout self, n: Int) raises -> List[UInt8]:
        """Read n bytes from the buffer."""

        if self._check_buf_state():
            self._reset_buffer()
            _ = self._fill_buffer()

        var nels = n
        var data = List[UInt8](capacity=nels)
        data.size = min(nels, self.len())
        memcpy(data.unsafe_ptr(), self.buf + self.head, nels)
        self.head += nels
        return data

    @always_inline
    fn read_span(inout self, n: Int) raises -> StringRef:
        """Read n bytes from the buffer."""
        if self._check_buf_state():
            self._reset_buffer()
            _ = self._fill_buffer()

        var nels = min(n, self.len())
        var data = StringRef(self.buf + self.head, nels)
        self.head += nels
        return data

    fn read_buffer(inout self, n: Int) raises -> Buffer[DType.uint8]:
        """Read n bytes from the buffer."""
        if self._check_buf_state():
            self._reset_buffer()
            _ = self._fill_buffer()

        var nels = n
        var data = Buffer[DType.uint8](self.buf + self.head, nels)
        self.head += nels
        return data

    @always_inline
    fn robust_read(inout self, n: Int) raises -> List[UInt8]:
        """Read n bytes from the buffer, if the number of bytes requested is greater than the buffer size, the buffer is resized to accommodate the new data.
        """

        if n > self.get_capacity():
            self._resize_buf(n, max(self.capacity, n - self.capacity))
            _ = self._fill_buffer()

        var nels = min(n, self.len())
        var data = List[UInt8](capacity=nels)
        data.size = nels
        memcpy(data.unsafe_ptr(), self.buf + self.head, nels)
        self.head += nels
        return data

    @always_inline
    fn robust_read_span(
        inout self, n: Int
    ) raises -> Span[T=UInt8, lifetime = __lifetime_of(self)]:
        """Read n bytes from the buffer, if the number of bytes requested is greater than the buffer size, the buffer is resized to accommodate the new data.
        """
        if n > self.get_capacity():
            self._resize_buf(n, max(self.capacity, n - self.capacity))
            _ = self._fill_buffer()

        var nels = min(n, self.len())
        var data = Span[T=UInt8, lifetime = __lifetime_of(self)](
            unsafe_ptr=self.buf + self.head, len=nels
        )
        self.head += nels
        return data

    ###--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###
    ###-------------------------------------------------------------------------  BufferedLineIterator---------------------------------------------------------------------------------------###
    ###--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------###


struct BufferedLineIterator:
    var inner: BufferedReader

    fn __init__(
        inout self, source: Path, capacity: Int = DEFAULT_CAPACITY
    ) raises:
        self.inner = BufferedReader(source, capacity)

    @always_inline
    fn __len__(self) -> Int:
        return len(self.inner)

    @always_inline
    fn __getitem__(self, idx: Int) -> UInt8:
        return self.inner[idx]

    @always_inline
    fn __getitem__(self, span: Slice) -> List[UInt8]:
        return self.inner[span]

    @always_inline
    fn __setitem__(self, idx: Int, value: UInt8):
        self.inner[idx] = value

    # TODO: Handle small buffers as well
    @always_inline
    fn read_line(inout self) raises -> List[UInt8]:
        var idx = find_chr_next_occurance(
            self.inner.buf, self.inner.len(), self.inner.head
        )

        if idx == -1:
            _ = self.inner._fill_buffer()
            idx = find_chr_next_occurance(
                self.inner.buf, self.inner.len(), self.inner.head
            )

        var res = self.inner.read(idx - self.inner.head)
        self.inner._skip_delim()
        return res

    @always_inline
    fn read_line_span(inout self) raises -> StringRef:
        var idx = find_chr_next_occurance(
            self.inner.buf, self.inner.len(), self.inner.head
        )

        if idx == -1:
            _ = self.inner._fill_buffer()
            idx = find_chr_next_occurance(
                self.inner.buf, self.inner.len(), self.inner.head
            )

        var res = self.inner.read_span(idx - self.inner.head)
        self.inner._skip_delim()
        return res
