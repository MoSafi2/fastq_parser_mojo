"""Testing file for different read and return patterns."""

from pathlib import Path
from collections.optional import Optional
from time import perf_counter
from memory import UnsafePointer
from utils import StringRef, StringSlice


fn read_bytes(file: Path) raises:
    """Reads a file and returns a list of lines."""
    var handle = open(file, "r")

    var t1 = perf_counter()
    while True:
        var l = handle.read_bytes(4096)
        if len(l) == 0:
            break

    var t2 = perf_counter()
    print("return_byes_list, Time taken:", t2 - t1)


fn read_read(file: Path) raises:
    """Reads a file and returns a list of lines."""
    var handle = open(file, "r")

    var t1 = perf_counter()
    while True:
        var l = handle.read(4096)
        if len(l) == 0:
            break

    var t2 = perf_counter()
    print("return_list, Time taken:", t2 - t1)


fn read_to_pointer(file: Path) raises:
    """Reads a file and returns a list of lines."""
    var handle = open(file, "r")
    var ptr = UnsafePointer[UInt8]()
    ptr = ptr.alloc(4096)
    var t1 = perf_counter()
    while True:
        var l = handle.read(ptr, 4096)
        if l == 0:
            break
    var t2 = perf_counter()
    print("read_to_pointer, Time taken:", t2 - t1)


fn return_list(file: FileHandle) raises -> None:
    """Reads a file and returns a list of lines."""

    var t1 = perf_counter()
    while True:
        var l = file.read_bytes(4000)
        if len(l) == 0:
            break

        for i in range(0, len(l), 100):
            line = l[i : i + 100]

    var t2 = perf_counter()
    print("return_list, Time taken:", t2 - t1)


fn return_slice(file: FileHandle) raises -> None:
    """Reads a file and returns a list of lines."""

    var t1 = perf_counter()
    while True:
        var l = file.read_bytes(4000)
        if len(l) == 0:
            break

        for i in range(0, len(l), 100):
            line = Slice(i, i + 100)
    var t2 = perf_counter()
    print("return_slice, Time taken:", t2 - t1)


fn return_string_ref(file: FileHandle) raises -> None:
    """Reads a file and returns a list of lines."""

    var t1 = perf_counter()
    while True:
        var l = file.read(4000)
        if len(l) == 0:
            break
        for i in range(0, len(l), 100):
            line = StringRef(l._buffer.unsafe_ptr() + i, 100)
    var t2 = perf_counter()
    print("return_string_ref, Time taken:", t2 - t1)


fn return_string_slice(file: FileHandle) raises -> None:
    """Reads a file and returns a list of lines."""

    var t1 = perf_counter()
    while True:
        var l = file.read(4000)
        if len(l) == 0:
            break
        for i in range(0, len(l), 100):
            line = StringSlice[__origin_of(l)](
                ptr=l._buffer.unsafe_ptr() + i, length=100
            )
    var t2 = perf_counter()
    print("return_string_slice, Time taken:", t2 - t1)


fn main() raises:
    var file = Path("data/M_abscessus_HiSeq.fq")
    read_read(file)
    read_bytes(file)
    read_to_pointer(file)
    var handle = open(file, "r")
    # return_list(handle)
    _ = handle.seek(0)
    return_slice(handle)
    _ = handle.seek(0)
    return_string_ref(handle)
    _ = handle.seek(0)
    return_string_slice(handle)
    pass
