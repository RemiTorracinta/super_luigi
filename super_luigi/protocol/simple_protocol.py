


class RawProtocol(object):

    def read(self, line):
        key ,  value = line.split('\t', 1)
        return key, value

    def write(self, key, value):
        return '%s\t%s' % (key, value)


class RawValueProtocol(object):

    def read(self, line):
        value = line
        return None , value

    def write(self, key, value):
        return  value



class RawCodecProtocol(object):

    def read(self, line):
        key ,  value = line.decode('gb18030').split('\t', 1)
        return key, value

    def write(self, key, value):
        return '%s\t%s' % (key.encode('gb18030'), value.encode('gb18030'))


class RawValueCodecProtocol(object):

    def read(self, line):
        value = line.decode('gb18030')
        return None , value

    def write(self, key, value):
        return  value.encode('gb18030')



import ujson as json

class JSONProtocol(object):
    def read(self, line):
        key ,  value = line.split('\t', 1)
        return json.loads(key), json.loads(value)

    def write(self, key, value):
        return '%s\t%s' % (json.dumps(key), json.dumps(value))

class JSONValueProtocol(object):
    def read(self, line):
        return None, json.loads(line)

    def write(self, key, value):
        return json.dumps(value)



import cPickle

class BinaryPickleProtocol(object):

    def read(self, line):
        key_str,  v_str = line.split('\t', 1)
        return cPickle.loads(key_str.decode('string_escape')),  cPickle.loads(v_str.decode('string_escape'))

    def write(self, key, value):
        return '%s\t%s' % (cPickle.dumps(key,-1).encode('string_escape'), cPickle.dumps(value,-1).encode('string_escape'))

