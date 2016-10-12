__author__ = 'sun'


from luigi import *
from luigi.contrib.hdfs import  *
from luigi.contrib.hadoop import *

from luigi.parameter import BoolParameter
from super_luigi.job.super_job_runner import SuperHadoopJobRunner
from super_luigi.protocol.simple_protocol import RawProtocol
from super_luigi.tasks import HadoopExternalData, LocalExternalData

import sys


class SuperJobTask(JobTask):

    local = BoolParameter(default = False)

    INPUT_PROTOCOL = RawProtocol
    INTERNAL_PROTOCOL = RawProtocol
    OUTPUT_PROTOCOL = RawProtocol

    def __init__(self, *args, **kwargs):

        super(SuperJobTask, self).__init__(*args, **kwargs)


        self.map_task_num = 800
        self.red_task_num = 800

        self.map_memory = 2000
        self.red_memory = 2000

        self.priority = "NORMAL"

        self.options = dict()
        self.job_confs = dict()

        self.input_protocol = self.INPUT_PROTOCOL()
        self.internal_protocol = self.INTERNAL_PROTOCOL()
        self.output_protocol = self.OUTPUT_PROTOCOL()


    def _get_working_file_path(self, path):

        if not self.local:
            return path.rsplit('/',1)[-1]
        else:
            return path

    def _output(self, output_path):

        if self.local:

            return LocalTarget(output_path)
        else:

            return HdfsTarget(output_path)

    def _input(self, input_path):

        if  self.local:
            return LocalExternalData(input_path)
        else:
            return HadoopExternalData(input_path)


    def reader(self, input_stream):
        """Reader which uses python eval on each part of a tab separated string.
        Yields a tuple of python objects."""
        for input in input_stream:
            yield self.input_protocol.read(input)

    def writer(self, outputs, stdout, stderr=sys.stderr):
        for key, value  in outputs:
            print >> stdout, self.output_protocol.write(key,value)


    def mapper(self, item):
        """Re-define to process an input item (usually a line of input data)


        Defaults to identity mapper that sends all lines to the same reducer"""
        key, value = item
        yield  key,value

#    mapper = NotImplemented


    def _map_input(self, input_stream):
        """Iterate over input and call the mapper for each item.
           If the job has a parser defined, the return values from the parser will
           be passed as arguments to the mapper.

           If the input is coded output from a previous run, the arguments will be splitted in key and value."""
        for key, value  in self.reader(input_stream):
            mapper_result = self.mapper((key,value))
            if mapper_result:
                for k, v in mapper_result:
                    yield k, v
        if self.final_mapper != NotImplemented:
            for k,v  in self.final_mapper():
                yield k,v
        self._flush_batch_incr_counter()

    def _reduce_input(self, inputs, reducer, final=NotImplemented):
        """Iterate over input, collect values with the same key, and call the reducer for each uniqe key."""
        for key, values in groupby(inputs, itemgetter(0)):
            for output in reducer(key, (v[1] for v in values)):
                yield output
        if final != NotImplemented:
            for output in final():
                yield output
        self._flush_batch_incr_counter()

    def _run_mapper(self, stdin=sys.stdin, stdout=sys.stdout):
        """Run the mapper on the hadoop node."""
        self.init_hadoop()
        self.init_mapper()
        outputs = self._map_input((line[:-1] for line in stdin))
        if self.reducer == NotImplemented:
            self.writer(outputs, stdout)
        else:
            self.internal_writer(outputs, stdout)

    def _run_reducer(self, stdin=sys.stdin, stdout=sys.stdout):
        """Run the reducer on the hadoop node."""
        self.init_hadoop()
        self.init_reducer()
        if self.mapper == NotImplemented:
            outputs = self._reduce_input(self.reader((line[:-1] for line in stdin)), self.reducer, self.final_reducer)
        else:
            outputs = self._reduce_input(self.internal_reader((line[:-1] for line in stdin)), self.reducer, self.final_reducer)
        self.writer(outputs, stdout)

    def _run_combiner(self, stdin=sys.stdin, stdout=sys.stdout):
        self.init_hadoop()
        self.init_combiner()
        if self.mapper == NotImplemented:
            outputs = self._reduce_input(self.reader((line[:-1] for line in stdin)), self.combiner, self.final_combiner)
        else:
            outputs = self._reduce_input(self.internal_reader((line[:-1] for line in stdin)), self.combiner, self.final_combiner)
        self.internal_writer(outputs, stdout)


    def internal_reader(self, input_stream):
        """Reader which uses python eval on each part of a tab separated string.
        Yields a tuple of python objects."""
        for input in input_stream:
            yield self.internal_protocol.read(input)

    def internal_writer(self, outputs, stdout):
        """Writer which outputs the python repr for each item"""
        for key, value  in outputs:
            print >> stdout, self.internal_protocol.write(key,value)

    def job_runner(self):
        # We recommend that you define a subclass, override this method and set up your own config
        """ Get the MapReduce runner for this job

        If all outputs are HdfsTargets, the DefaultHadoopJobRunner will be used. Otherwise, the LocalJobRunner which streams all data through the local machine will be used (great for testing).
        """
        if self.local:
            return LocalJobRunner()
        else:
            return SuperHadoopJobRunner(self.options)


    def extra_archives(self):

        return []

    def jobconfs(self):

        jcs = super(SuperJobTask, self).jobconfs()

        idx = [i for i, conf in enumerate(jcs) if conf.startswith("mapred.job.name=")]

        if idx:
            task_name = self.task_id.replace('(',"").replace(")","")
            idx = idx[0]
            jcs[idx] = 'mapred.job.name=\"%s\"' % task_name


        custom_setting = set(['mapred.map.tasks', "mapred.job.map.capacity",
                              "mapred.reduce.tasks", "mapred.job.reduce.capacity",
                             "mapred.job.priority","stream.memory.limit"])

        jcs = [ conf for conf in jcs if conf.split('=',1)[0] not in custom_setting]

        jcs.append( "mapred.map.tasks=%s" % self.map_task_num )
        jcs.append( "mapred.job.map.capacity=%s" % self.map_task_num )
        if self.reducer != NotImplemented:
            jcs.append( "mapred.reduce.tasks=%s" %  self.red_task_num )
            jcs.append( "mapred.job.reduce.capacity=%s" % self.red_task_num )
        else:
            jcs.append( "mapred.reduce.tasks=%s" %  0)
            jcs.append( "mapred.job.reduce.capacity=%s" %0)

        jcs.append( "mapred.job.priority=%s" % self.priority)
        jcs.append( "mapred.map.memory.limit=%s" % self.map_memory)
        jcs.append( "mapred.reduce.memory.limit=%s" % self.red_memory)

        for k,v in self.job_confs.iteritems():
            jcs.append("{0}={1}".format(k, v))

        return jcs


    def add_second_sort_support(self, key_field_separator = "."):

        self.options["partitioner"] = "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"
        self.job_confs["stream.num.map.output.key.fields"] = "2"
        self.job_confs["mapred.text.key.partitioner.options"] = "-k1,1"
        self.job_confs["map.output.key.field.separator"] = key_field_separator
        self.job_confs["mapred.output.key.comparator.class"] = "org.apache.hadoop.mapred.lib.KeyFieldBasedComparator"
        self.job_confs["mapred.text.key.comparator.options"] = "-k1,1 -k2,2n"

    def add_newsecond_sort_support(self, key_field_separator = "."):

        self.options["partitioner"] = "org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner"
        self.job_confs["stream.map.output.field.separator"] = "\t"
        self.job_confs["stream.num.map.output.key.fields"] = "3"
        
        self.job_confs["map.output.key.field.separator"] = "\t"
        self.job_confs["mapred.text.key.partitioner.options"] = "-k1,1"
        
        #self.job_confs["mapred.output.key.comparator.class"] = "org.apache.hadoop.mapred.lib.KeyFieldBasedComparator"
        #self.job_confs["mapred.text.key.comparator.options"] = "-k1,1 -k2,2 -k3,3"


    def add_compress_support(self):

        self.job_confs["mapred.compress.map.output"] = "true"
        self.job_confs["mapred.map.output.compression.codec"] = "org.apache.hadoop.io.compress.QuickLzCodec"
        #self.job_confs["mapred.output.compress"] = "true"
        #self.job_confs["mapred.output.compression.code"] = "org.apache.hadoop.io.compress.LzmaCodec"


    def add_combined_input_support(self):

        self.options["inputformat"] = "org.apache.hadoop.mapred.CombineTextInputFormat"

    def add_multiple_output_support(self):

        self.options["outputformat"] = "org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat"

    def set_min_split_size(self, splitsize):
        self.job_confs['mapred.min.split.size'] = splitsize

    def set_max_split_size(self, splitsize):
        self.job_confs['mapred.max.split.size'] = splitsize

    def set_memory_size(self, memory_size):
        self.memory = memory_size


