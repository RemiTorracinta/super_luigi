'''
Created on 2013-6-6

@author: Sun
'''


import ujson as json
import cPickle
import sys

SECOND_SORT_SPLITTER = "."

class SecondSortProtocol(object):

    def read(self, line):
        compound_str,  v_str = line.split('\t', 1)
        key_str, sort_str = compound_str.rsplit(SECOND_SORT_SPLITTER,1)
        return json.loads(key_str),  json.loads(v_str)

    def write(self, compound_key, value):
        key, order = compound_key
        return '%s%s%s\t%s' % (json.dumps(key),SECOND_SORT_SPLITTER, order, json.dumps(value))


class SecondSortRawProtocol(object):

    def read(self, line):
        compound_str,  v_str = line.split('\t', 1)
        key_str, sort_str = compound_str.rsplit(SECOND_SORT_SPLITTER,1)
        return key_str,  v_str

    def write(self, compound_key, value):
        key, order = compound_key
        return '%s%s%s\t%s' % ( key, SECOND_SORT_SPLITTER, order,  value)

    
class SecondSortPickleProtocol(object):

    def read(self, line):
        compound_str,  v_str = line.split('\t', 1)
        key_str, sort_str = compound_str.rsplit(SECOND_SORT_SPLITTER,1)
        return json.loads(key_str),  cPickle.loads(v_str.decode('string_escape'))

    def write(self, compound_key, value):
        key, order = compound_key
        return '%s%s%s\t%s' % (json.dumps(key), SECOND_SORT_SPLITTER, order, cPickle.dumps(value,-1).encode('string_escape'))

class NewSecondSortPickleProtocol(object):

    def read(self, line):
        query, uri, sort_str, v_str = line.split('\t')
        return query.decode('gb18030'), (uri.decode('gb18030'), sort_str, cPickle.loads(v_str.decode('string_escape')))

    def write(self, compound_key, value):
        (query, uri), order = compound_key
        return '%s\t%s\t%s\t%s' % (query.encode('gb18030'), uri.encode('gb18030'), order, cPickle.dumps(value,-1).encode('string_escape'))

class NewSecondSortRawProtocol(object):

    def read(self, line):
        query, uri, sort_str, v_str = line.split('\t',3)
        return query.decode('gb18030'), (uri.decode('gb18030'), sort_str, v_str)

    def write(self, compound_key, value):
        (query, uri), order = compound_key
        return '%s\t%s\t%s\t%s' % (query.encode('gb18030'), uri.encode('gb18030'), order, value)
