import subprocess, os
try:
    import simplejson as json
except ImportError:
    import json

bpbot_path = "b+bot"

def unpack_bytes(val, size):
    expanded = []
    for item in xrange(size):
        expanded.append((val >> ((size-item-1)<<3)) & 0xFF)
    return expanded

def pack_bytes(bytes, size):
    collapsed_val = 0
    #print bytes
    for byte, position in zip(bytes, xrange(size)):
        collapsed_val |= ord(byte) << ((size-position-1)<<3)
    return collapsed_val

class GoBpTree(object):
    def __init__(self, filename, key_size, field_sizes):
        super(GoBpTree, self).__init__()
        self.filename = filename
        self.key_size = key_size
        self.field_sizes = field_sizes

        self._proc = subprocess.Popen(
            [bpbot_path],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE
        )
        self.write_json({
            "op": "init",
            "filename": filename,
            "keysize": key_size,
            "fieldsizes": field_sizes
            })

        self.status = self._proc.stdout.readline()[:-1]
        if self.status != "ok":
            raise Exception("Failed B+ tree creation")

    def write_json(self, data):
        # print data
        j = json.dumps(data)
        self._proc.stdin.write("%s\n" % j)

    def assert_status(self, status, fail_message):
        if self.status != status:
            raise Exception(fail_message)

    def has_key(self, key):
        return len(self.find(key, key)) > 0

    def unpackkey(self, key):
        return unpack_bytes(key, self.key_size)

    def insert(self, key, *fields):
        # print "insert %d:" % key, fields
        expanded_key = self.unpackkey(key)
        if len(fields) != len(self.field_sizes):
            raise Exception("Wrong number of fields (need %d)" % len(self.field_sizes))
        expanded_fields = [unpack_bytes(value, size) for size, value in zip(self.field_sizes, fields)]
        self.write_json({
            "op": "insert",
            "leftKey": expanded_key,
            "fields": expanded_fields
            })

        self.status = self._proc.stdout.readline()[:-1]
        self.assert_status("true", "Insert failed")

    def find(self, left_key, right_key=None):
        if right_key is None:
            right_key = left_key
        expanded_left = self.unpackkey(left_key)
        if left_key != right_key:
            expanded_right = self.unpackkey(right_key)
        else:
            expanded_right = expanded_left
        # print "find %d, %d" % (left_key, right_key)
        self.write_json({
            "op": "find",
            "leftKey": expanded_left,
            "rightKey": expanded_right
            })
        self.status = ""
        results = []
        while self.status != "end":
            self.status = self._proc.stdout.readline()[:-1]
            if self.status == "end":
                break
            #print "  ", self.status
            data = json.loads(self.status)
            data[u'value'] = [
                pack_bytes(field.decode('base64'), size) 
                for field, size in zip(data[u'value'], self.field_sizes)]
            data[u'key'] = pack_bytes(data[u'key'].decode('base64'), self.key_size)
            #print "    ", data[u'key']
            results.append(data)
        return results

    def visualize(self, path):
        self.write_json({"op":"visualize", "fileName":path+".dot"})
        self.write_json({"op":"prettyprint", "fileName":path+".txt"})

    def close(self):
        self._proc.stdin.write("q\n")
        self.status = self._proc.stdout.readline()[:-1]
        self.assert_status("exited", "Exit failed")

