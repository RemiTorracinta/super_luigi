__author__ = 'Sun'

import subprocess

import luigi.hdfs
from luigi.extend.job.parameter import *


class HadoopExternalData(luigi.ExternalTask):

    data_path = SuperParameter()

    def output(self):
        return luigi.hdfs.HdfsTarget(self.data_path)


class LocalExternalData(luigi.ExternalTask):

    data_path = SuperParameter()

    def output(self):
        return luigi.LocalTarget(self.data_path)


import uuid
class DistcpTask(luigi.Task):
    src_path = SuperParameter()
    su = SuperParameter()
    dest_path = SuperParameter()
    du = SuperParameter()


    def requires(self):

        yield HadoopExternalData(self.src_path)


    def output(self):

        return luigi.hdfs.HdfsTarget(self.dest_path)

    def run(self):

        config = configuration.get_config()
        hadoop_bin = config.get('hadoop', 'command')

        temp_dest_path = self.dest_path + "_" + str(uuid.uuid1())
        cmd_str = hadoop_bin + " distcp -su {0} -du {1} " \
                  "{2} {3} &> distcp.log ".format(
                        self.su, self.du,
                        self.src_path, temp_dest_path)
        p = subprocess.Popen(cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        value, err = p.communicate()

        if p.returncode != 0 or err:
            raise Exception("Distcp Failed:" + err)

        post_cmd_str = hadoop_bin + " fs mv {0} {1}".format(temp_dest_path, self.dest_path)

        p = subprocess.Popen(post_cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        value, err = p.communicate()

        if p.returncode != 0 or err:
            raise Exception("Moving Failed:" + err)




from luigi.extend.path import get_hadoop_files

class LinkDataTask(luigi.Task):
    src_path = SuperParameter()
    tgt_path = SuperParameter()

    def requires(self):

        yield HadoopExternalData(self.src_path)

    def output(self):

        return luigi.hdfs.HdfsTarget(self.tgt_path)

    def run(self):

        config = configuration.get_config()
        hadoop_bin = config.get('hadoop','command')

        file_paths = get_hadoop_files(self.src_path, hadoop_bin)
        temp_dest_path = self.tgt_path + "_" + str(uuid.uuid1())
        mkdir_str = hadoop_bin + " fs -mkdir {0}".format(temp_dest_path)

        p = subprocess.Popen(mkdir_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        value, err = p.communicate()

        if p.returncode != 0 or err:
            raise Exception("Making Target Dir Failed: " + mkdir_str + " with Error: " +  err)

        for path in file_paths:
            filename = path.split('/')[-1]
            cmd_str = hadoop_bin + " fs -ln {0} {1} ".format(self.src_path + "/" + filename, temp_dest_path + "/" + filename)

            p = subprocess.Popen(cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            value, err = p.communicate()

            if p.returncode != 0 or err:
                raise Exception("Linking File Failed: " + cmd_str + " with Error: " + err)

        post_cmd_str = hadoop_bin + " fs -mv {0} {1}".format(temp_dest_path, self.tgt_path)

        p = subprocess.Popen(post_cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        value, err = p.communicate()

        if p.returncode != 0 or err:
            raise Exception("Moving Failed: " + post_cmd_str + " with Error: " + err)

class UploadDataTask(luigi.Task):
    src_path = SuperParameter()
    tgt_path = SuperParameter()

    def requires(self):

        yield LocalExternalData(self.src_path)

    def output(self):

        return luigi.hdfs.HdfsTarget(self.tgt_path)

    def run(self):

        config = configuration.get_config()
        hadoop_bin = config.get('hadoop','command')

        temp_dest_path = self.tgt_path + "_" + str(uuid.uuid1())
        cmd_str = hadoop_bin + " fs -put {0} {1} &> upload.log ".format(
                        self.input().fn, temp_dest_path)
        p = subprocess.Popen(cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        value, err = p.communicate()


        if p.returncode != 0 or err:
            raise Exception("Upload Data Failed: " + cmd_str + " with Error: " +  err)

        post_cmd_str = hadoop_bin + " fs -mv {0} {1}".format(temp_dest_path, self.tgt_path)

        p = subprocess.Popen(post_cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        value, err = p.communicate()

        if p.returncode != 0 or err:
            raise Exception("Moving Failed: " + post_cmd_str + " with Error: " + err)

class MoveDataTask(luigi.Task):
    src_path = SuperParameter()
    tgt_path = SuperParameter()

    def output(self):

        return luigi.hdfs.HdfsTarget(self.tgt_path)

    def run(self):

        config = configuration.get_config()
        hadoop_bin = config.get('hadoop','command')

        cmd_str = hadoop_bin + " fs -mv {0} {1} ".format(self.src_path, self.tgt_path)

        p = subprocess.Popen(cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        value, err = p.communicate()

        if p.returncode != 0 or err:
            raise Exception("Moving Failed" + value)

class DownloadDataTask(luigi.Task):
    src_path = SuperParameter()
    tgt_path = SuperParameter()


    def output(self):

        return luigi.LocalTarget(self.tgt_path)

    def run(self):

        config = configuration.get_config()
        hadoop_bin = config.get('hadoop','command')

        cmd_str = hadoop_bin + " fs -getmerge {0} {1} ".format(self.src_path, self.tgt_path)

        p = subprocess.Popen(cmd_str, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        value, err = p.communicate()

        if p.returncode != 0 or err:
            raise Exception("Download Failed " + str(err) + "with command " + cmd_str)

