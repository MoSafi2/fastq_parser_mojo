from blazeseq.CONSTS import *
from blazeseq.iostream import BufferedLineIterator
from utils.variant import Variant
from utils import Span
from math import align_down, remainder


alias LU8 = List[UInt8]
alias schema = Variant[String, QualitySchema]


@value
struct FastqRecord(Sized, Stringable, CollectionElement):
    """Struct that represent a single FastaQ record."""

    var SeqHeader: LU8
    var SeqStr: LU8
    var QuHeader: LU8
    var QuStr: LU8
    var quality_schema: QualitySchema

    fn __init__(
        inout self,
        SH: LU8,
        SS: LU8,
        QH: LU8,
        QS: LU8,
        quality_schema: schema = "generic",
    ) raises:
        self.SeqHeader = SH
        self.QuHeader = QH
        self.SeqStr = SS
        self.QuStr = QS

        if quality_schema.isa[String]():
            self.quality_schema = self._parse_schema(quality_schema[String])
        else:
            self.quality_schema = quality_schema[QualitySchema]

    fn __init__(
        inout self,
        SH: String,
        SS: String,
        QH: String,
        QS: String,
        quality_schema: schema = "generic",
    ):
        self.SeqHeader = SH.as_bytes()
        self.SeqStr = SS.as_bytes()
        self.QuHeader = QH.as_bytes()
        self.QuStr = QS.as_bytes()
        if quality_schema.isa[String]():
            var q: String = quality_schema[String]
            self.quality_schema = self._parse_schema(q)
        else:
            self.quality_schema = quality_schema[QualitySchema]

    @always_inline
    fn get_seq(self) -> String:
        var temp = self.SeqStr
        temp.append(0)
        return String(temp)

    @always_inline
    fn get_qulity(self) -> String:
        var temp = self.QuStr
        temp.append(0)
        return String(temp)

    @always_inline
    fn get_qulity_scores(self, quality_format: String) -> LU8:
        var schema = self._parse_schema((quality_format))
        temp = self.QuStr
        for i in range(len(temp)):
            temp[i] = temp[i] - schema.OFFSET
        return temp

    @always_inline
    fn get_qulity_scores(self, schema: QualitySchema) -> LU8:
        temp = self.QuStr
        for i in range(len(temp)):
            temp[i] = temp[i] - schema.OFFSET
        return temp

    @always_inline
    fn get_qulity_scores(self, offset: UInt8) -> LU8:
        temp = self.QuStr
        for i in range(len(temp)):
            temp[i] = temp[i] - offset
        return temp

    @always_inline
    fn get_header(self) -> String:
        var temp = self.SeqHeader
        temp.append(0)
        return String(temp)

    @always_inline
    fn wirte_record(self) -> LU8:
        return self.__concat_record_tensor()

    @always_inline
    fn validate_record(self) raises -> Bool:
        if self.SeqHeader[0] != read_header:
            print("Sequence Header is corrupt")
            return False

        if self.QuHeader[0] != quality_header:
            print("Quality Header is corrupt")
            return False

        if self.len_record() != self.len_quality():
            print("Corrput Lengths")
            return False

        if self.len_qu_header() > 1:
            if self.len_qu_header() != self.len_seq_header():
                print("Quality Header is corrupt")
                return False

        if self.len_qu_header() > 1:
            for i in range(1, self.len_qu_header()):
                if self.QuHeader[i] != self.SeqHeader[i]:
                    print("Non matching headers")
                    return False
        return True

    @always_inline
    fn validate_quality_schema(self) raises -> Bool:
        for i in range(self.len_quality()):
            if (
                self.QuStr[i] > self.quality_schema.UPPER
                or self.QuStr[i] < self.quality_schema.LOWER
            ):
                print("Corrput quality score according to proivded schema")
                return False
        return True

    @always_inline
    fn total_length(self) -> Int:
        return (
            self.len_seq_header()
            + self.len_record()
            + self.len_qu_header()
            + self.len_quality()
            + 4
        )

    @always_inline
    fn __concat_record_tensor(self) -> LU8:
        var final_list = List[UInt8](capacity=self.total_length())

        for i in range(self.len_seq_header()):
            final_list.append(self.SeqHeader[i])
        final_list.append(10)

        for i in range(self.len_record()):
            final_list.append(self.SeqStr[i])
        final_list.append(10)

        for i in range(self.len_qu_header()):
            final_list.append(self.QuHeader[i])
        final_list.append(10)

        for i in range(self.len_quality()):
            final_list.append(self.QuStr[i])
        final_list.append(10)

        return LU8(final_list)

    @always_inline
    fn __concat_record_str(self) -> String:
        if self.total_length() == 0:
            return ""

        var line1 = self.SeqHeader
        line1.append(0)
        var line1_str = String(line1)

        var line2 = self.SeqStr
        line2.append(0)
        var line2_str = String(line2)

        var line3 = self.QuHeader
        line3.append(0)
        var line3_str = String(line3)

        var line4 = self.QuStr
        line4.append(0)
        var line4_str = String(line4)

        return (
            line1_str
            + "\n"
            + line2_str
            + "\n"
            + line3_str
            + "\n"
            + line4_str
            + "\n"
        )

    @staticmethod
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
                "Uknown quality schema please choose one of 'sanger', 'solexa',"
                " 'illumina_1.3', 'illumina_1.5' 'illumina_1.8', or 'generic'"
            )
            return generic_schema
        return schema

    # BUG: returns Smaller strings that expected.
    @always_inline
    fn __str__(self) -> String:
        return self.__concat_record_str()

    @always_inline
    fn __len__(self) -> Int:
        return len(self.SeqStr)

    @always_inline
    fn len_record(self) -> Int:
        return len(self.SeqStr)

    @always_inline
    fn len_quality(self) -> Int:
        return len(self.QuStr)

    @always_inline
    fn len_qu_header(self) -> Int:
        return len(self.QuHeader)

    @always_inline
    fn len_seq_header(self) -> Int:
        return len(self.SeqHeader)

    @always_inline
    fn hash[bits: Int = 3, length: Int = 64 // bits](self) -> UInt64:
        """Hashes the first xx bp (if possible) into one 64bit. Max length is 64/nBits per bp.
        """

        @parameter
        if length < 32:
            return self._hash_packed(self.SeqStr.unsafe_ptr(), length)

        return self._hash_additive(self.SeqStr.unsafe_ptr(), length)

    @staticmethod
    fn _hash_packed[
        bits: Int = 3
    ](bytes: UnsafePointer[UInt8], length: Int) -> UInt64:
        """
        Hash the DNA strand to into 64bits unsigned number using xbit encoding.
        If the length of the bytes strand is longer than 32 bps, the hash is truncated for the first 32 bps.
        """

        alias rnge: Int = 64 // bits
        var hash: UInt64 = 0
        var mask = (0b1 << bits) - 1
        for i in range(min(rnge, length)):
            # Mask for for first <n> significant bits.
            var base_val = bytes[i] & mask
            hash = (hash << bits) | int(base_val)
        return hash

    @staticmethod
    fn _hash_additive[
        bits: Int = 3
    ](bytes: UnsafePointer[UInt8], length: Int) -> UInt64:
        """Hashes DNA sequences longer than 32bps. It hashes 16bps spans of the sequences and using 2 or 3 bit encoding and adds them to the hash.
        """
        constrained[
            bits <= 3, "Additive hashing can only hash up to 3bit resolution"
        ]()
        var full_hash: UInt64 = 0
        var mask = (0b1 << bits) - 1
        var rounds = align_down(length, 16)
        var rem = length % 16

        for round in range(rounds):
            var interim_hash: UInt64 = 0

            @parameter
            for i in range(16):
                var base_val = bytes[i + 16 * round] & mask
                interim_hash = interim_hash << bits | int(base_val)
            full_hash = full_hash + interim_hash

        if rem > 0:
            var interim_hash: UInt64 = 0
            for i in range(rem):
                var base_val = bytes[i + 16 * rounds] & mask
                interim_hash = interim_hash << bits | int(base_val)
            full_hash = full_hash + interim_hash

        return full_hash

    @always_inline
    fn __hash__(self) -> Int:
        return int(self.hash())

    @always_inline
    fn __eq__(self, other: Self) -> Bool:
        return self.__hash__() == other.__hash__()

    fn __ne__(self, other: Self) -> Bool:
        return self.__hash__() != other.__hash__()


@value
struct RecordCoord(Sized, Stringable, CollectionElement):
    """Struct that represent coordinates of a FastqRecord in a chunk. Provides minimal validation of the record. Mainly used for fast parsing.
    """

    var SeqHeader: StringRef
    var SeqStr: StringRef
    var QuHeader: StringRef
    var QuStr: StringRef

    fn __init__(
        inout self,
        SH: StringRef,
        SS: StringRef,
        QH: StringRef,
        QS: StringRef,
    ):
        self.SeqHeader = SH
        self.SeqStr = SS
        self.QuHeader = QH
        self.QuStr = QS

    @always_inline
    fn validate(self) raises:
        if self.seq_len() != self.qu_len():
            raise Error("Corrput Lengths")
        if (
            self.qu_header_len() > 1
            and self.qu_header_len() != self.seq_header_len()
        ):
            raise Error("Corrput Lengths")

    @always_inline
    fn seq_len(self) -> Int32:
        return self.SeqStr.length

    @always_inline
    fn qu_len(self) -> Int32:
        return self.QuStr.length

    @always_inline
    fn qu_header_len(self) -> Int32:
        return self.QuHeader.length

    @always_inline
    fn seq_header_len(self) -> Int32:
        return self.SeqHeader.length

    fn __len__(self) -> Int:
        return int(self.seq_len())

    fn __str__(self) -> String:
        return (
            String("SeqHeader: ")
            + str(self.SeqHeader.length)
            + "\nSeqStr: "
            + str(self.SeqStr.length)
            + "\nQuHeader: "
            + str(self.QuHeader.length)
            + "\nQuStr: "
            + str(self.QuStr.length)
        )


# fn main() raises:
#     var record = FastqRecord(
#         String("@HWI-ST180_0186:3:1:1484:1936#GGCTAC/1"),
#         String(
#             "NCTTGCCAAGACTGCGAAGGTGCAGTTCGCAAAGCGCGTACGCTGGCCACGTGTCCAAAACGTACGTTGGAGGGCGCCTTCGTCAACTCCGGAGCGAACG"
#         ),
#         String("+"),
#         String(
#             r"BPSSSTXUT[__acccccc\Y[[[][[[Y[_____ccc[c^^^^BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"
#         ),
#         String("sanger"),
#     )

#     print(record.validate_record())
