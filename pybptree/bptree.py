#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Original Author: Steve Johnson <steve.johnson.public@gmail.com>
#Current Author: Tim Henderson
#Email: tim.tadh@gmail.com
#For licensing see the LICENSE file in the top level directory.

import sys
import os
import signal
import subprocess
import json
import struct

bpbot_path = "b+bot"
INSERT = struct.pack("<c", chr(0))
FIND = struct.pack("<c", chr(1))
QUIT = struct.pack("<c", chr(2))
SIZE = struct.pack("<c", chr(3))
CONTAINS = struct.pack("<c", chr(4))

class Spec(object):

    def __init__(self, size, pytype):
        self.size = size
        self.pytype = pytype

    def bitpack(self, value):
        raise RuntimeError, NotImplemented

    def bitunpack(self, bitvalue):
        raise RuntimeError, NotImplemented

class Numtype(Spec):

    def bitpack(self, value):
        return self._struct.pack(value)

    def bitunpack(self, bitvalue):
        return self._struct.unpack(bitvalue)[0]

class String(Spec):

    def __init__(self, size):
        super(String, self).__init__(size, str)
        self._struct = struct.Struct('>%ds' % self.size)

    def bitpack(self, value):
        return self._struct.pack(value)

    def bitunpack(self, bitvalue):
        return self._struct.unpack(bitvalue)[0].strip('\0')

class Int(Numtype):

    _struct = struct.Struct('>i')

    def __init__(self):
        super(Int, self).__init__(4, int)

class UInt(Numtype):

    _struct = struct.Struct('>I')

    def __init__(self):
        super(UInt, self).__init__(4, int)

class Long(Numtype):

    _struct = struct.Struct('>q')

    def __init__(self):
        super(Long, self).__init__(8, int)

class ULong(Numtype):

    _struct = struct.Struct('>Q')

    def __init__(self):
        super(ULong, self).__init__(8, int)

class Float(Numtype):

    _struct = struct.Struct('>d')

    def __init__(self):
        super(Float, self).__init__(8, float)

class Tuple(Spec):

    def __init__(self, *specs):
        super(Tuple, self).__init__(sum(spec.size for spec in specs), tuple)
        self.specs = specs

    def bitpack(self, value):
        assert len(value) == len(self.specs)
        return ''.join(
            spec.bitpack(v)
            for v, spec in zip(value, self.specs)
        )

    def bitunpack(self, bitvalue):
        offset = 0
        values = list()
        for spec in self.specs:
            values.append(spec.bitunpack(bitvalue[offset:offset+spec.size]))
            offset += spec.size
        return tuple(values)


class GoBpTree(object):

    def __init__(self, path, keyspec, fields):
        '''Initialize a new GoBpTree. If the B+Tree file doesn't exist a new
        file is created. Otherwise, the record specification is assumed to be
        valid.

        :param path: the path to the backing file
        :param key_spec: a object subclassing `Spec` which provides bit-packing
                         and size information
        :param fields: a list of (name, Spec) tuples
        '''
        self.path = path
        self.keyspec = keyspec
        self.fields = fields

        self._proc = subprocess.Popen(
            [bpbot_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE
        )
        self.write_json({
            "op": "init",
            "path": self.path,
            "keysize": self.keyspec.size,
            "fieldsizes": [f.size for name, f in self.fields]
        })

        self.status = self._proc.stdout.readline()[:-1]
        if self.status != "ok":
            raise Exception("Failed B+ tree creation")
        self.closed = False

    def __del__(self):
        self.close()
        os.kill(self._proc.pid, signal.SIGTERM)

    def __len__(self):
        self._proc.stdin.write(SIZE)
        bitsize = self._proc.stdout.read(8)
        return struct.unpack(">Q", bitsize)[0]

    def __contains__(self, key):
        bitkey = self.keyspec.bitpack(key)
        self._proc.stdin.write(CONTAINS)
        self._proc.stdin.write(bitkey)
        bitbyte = self._proc.stdout.read(1)
        byte = ord(struct.unpack(">c", bitbyte)[0])
        return bool(byte)

    def write_json(self, data):
        # print data
        j = json.dumps(data)
        #print >>sys.stderr, j
        self._proc.stdin.write("%s\n" % j)

    def assert_status(self, status, fail_message):
        if self.status != status:
            raise Exception(fail_message)

    def insert(self, key, *fields):
        # print "insert %d:" % key, fields
        if len(fields) != len(self.fields):
            raise Exception("Wrong number of fields (need %d)" % len(self.fields))
        bitkey = self.keyspec.bitpack(key)
        bitfields = [
            fs.bitpack(value)
            for (name, fs), value in zip(self.fields, fields)
        ]
        self._proc.stdin.write(INSERT)
        self._proc.stdin.write(bitkey)
        self._proc.stdin.write(''.join(bitfields))
        #self.write_json({
        #    "op": "insert",
        #    "leftKey": bitkey,
        #    "fields": bitfields
        #})
        self.status = self._proc.stdout.readline()[:-1]
        self.assert_status("true", "Insert failed")

    def find(self, left_key, right_key=None):
        if right_key is None:
            right_key = left_key
        bitleft = self.keyspec.bitpack(left_key)
        if left_key != right_key:
            bitright = self.keyspec.bitpack(right_key)
        else:
            bitright = bitleft
        self._proc.stdin.write(FIND)
        self._proc.stdin.write(bitleft)
        self._proc.stdin.write(bitright)
        records = list()
        CONTBYTE = 0
        STOPBYTE = 1
        while True:
            sigbyte = ord(self._proc.stdout.read(1))
            if sigbyte == STOPBYTE:
                break
            elif sigbyte != CONTBYTE:
                print >>sys.stderr, "bad signal byte", ord(sigbyte)
                break
            bitkey = self._proc.stdout.read(self.keyspec.size)
            key = self.keyspec.bitunpack(bitkey)
            fields = dict(
                (name, fs.bitunpack(self._proc.stdout.read(fs.size)))
                for (name, fs) in self.fields)
            records.append((key, fields))
        return records

    def visualize(self, path):
        self.write_json({"op":"visualize", "fileName":path+".dot"})
        self.write_json({"op":"prettyprint", "fileName":path+".txt"})

    def close(self):
        if not self.closed:
            self.closed = True
            self._proc.stdin.write(QUIT)
            self.status = self._proc.stdout.readline()[:-1]
            print self.status
            try:
                self.assert_status("exited", "Exit failed")
            except:
                print self._proc.stdout.read()
                raise

