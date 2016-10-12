__author__ = 'sun'


from luigi.contrib.hadoop import *


class SuperHadoopJobRunner(DefaultHadoopJobRunner):
    ''' Takes care of uploading & executing a Hadoop job using Hadoop streaming

    '''
    def __init__(self, options):

        super(SuperHadoopJobRunner, self).__init__()

        self.tmp_dir = False
        self.options = options


    def run_job(self, job, tracking_url_callback=None):


        packages = [luigi] + self.modules + job.extra_modules() #+ list(_attached_packages)

        # find the module containing the job
        packages.append(__import__(job.__module__, None, None, 'dummy'))

        # find the path to out runner.py
        runner_path = mrrunner.__file__
        # assume source is next to compiled
        if runner_path.endswith("pyc"):
            runner_path = runner_path[:-3] + "py"

        base_tmp_dir = configuration.get_config().get('core', 'tmp-dir', None)
        if base_tmp_dir:
            warnings.warn("The core.tmp-dir configuration item is"\
                          " deprecated, please use the TMPDIR"\
                          " environment variable if you wish"\
                          " to control where luigi.hadoop may"\
                          " create temporary files and directories.")
            self.tmp_dir = os.path.join(base_tmp_dir, 'hadoop_job_%016x' % random.getrandbits(64))
            os.makedirs(self.tmp_dir)
        else:
            self.tmp_dir = tempfile.mkdtemp()

        logger.debug("Tmp dir: %s", self.tmp_dir)

        # build arguments
        config = configuration.get_config()
        python_executable = config.get('hadoop', 'python-executable', 'python')
        if job.mapper == NotImplemented:
            map_cmd = '\"cat\"'
        else:
            map_cmd = '\"{0} mrrunner.py map\"'.format(python_executable)
        cmb_cmd = '\"{0} mrrunner.py combiner\"'.format(python_executable)
        red_cmd = '\"{0} mrrunner.py reduce\"'.format(python_executable)

        # replace output with a temporary work directory
        output_final = job.output().path
        output_tmp_fn = output_final + '-temp-' + datetime.datetime.now().isoformat().replace(':', '-')
        tmp_target = luigi.hdfs.HdfsTarget(output_tmp_fn, is_tmp=True)

        arglist = [luigi.hdfs.load_hadoop_cmd(), 'jar', self.streaming_jar]

        # 'libjars' is a generic option, so place it first
        libjars = [libjar for libjar in self.libjars]

        for libjar in self.libjars_in_hdfs:
            subprocess.call([luigi.hdfs.load_hadoop_cmd(), 'fs', '-get', libjar, self.tmp_dir])
            libjars.append(os.path.join(self.tmp_dir, os.path.basename(libjar)))

        if libjars:
            arglist += ['-libjars', ','.join(libjars)]

        # Add static files and directories
        #extra_files = get_extra_files(job.extra_files())

        extra_files = job.extra_files()

        for file in extra_files:
            if not file:
                continue

            if file.startswith("hdfs"):
                link_name = file.rsplit('/',1)[-1]
                arglist += ["-cacheFile",  file + "#" + link_name]
            else:
                arglist += ["-file", file ]
        # files = []
        # for src, dst in extra_files:
        #     dst_tmp = '%s_%09d' % (dst.replace('/', '_'), random.randint(0, 999999999))
        #     files += ['%s#%s' % (src, dst_tmp)]
        #     # -files doesn't support subdirectories, so we need to create the dst_tmp -> dst manually
        #     job._add_link(dst_tmp, dst)
        #
        # if files:
        #     arglist += ['-files', ','.join(files)]

        # Add static archives
        extra_archives = list(job.extra_archives())

#        jumbo_archive = config.get('hadoop', 'jumbo-archive')

#        extra_archives.append(jumbo_archive + "#Jumbo")

        for archive in extra_archives:
            arglist += ['-cacheArchive',  archive]



        arglist += self.streaming_args

        arglist += ['-mapper', map_cmd]
        if job.combiner != NotImplemented:
            arglist += ['-combiner', cmb_cmd]
        if job.reducer != NotImplemented:
            arglist += ['-reducer', red_cmd]
        files = [runner_path, self.tmp_dir + '/packages.tar', self.tmp_dir + '/job-instance.pickle']

        for f in files:
            arglist += ['-file', f]

        if self.output_format:
            arglist += ['-outputformat', self.output_format]
        if self.input_format:
            arglist += ['-inputformat', self.input_format]

        for target in luigi.task.flatten(job.input_hadoop()):
            if isinstance(target, luigi.hdfs.HdfsTarget):
                arglist += ['-input', target.path]

        assert isinstance(job.output(), luigi.hdfs.HdfsTarget)
        arglist += ['-output', output_tmp_fn]


        for key, value in self.options.iteritems():
            arglist += ["-" + key, value ]

        jobconfs = job.jobconfs()

        for k, v in self.jobconfs.iteritems():
            jobconfs.append('%s=%s' % (k, v))

        for conf in jobconfs:
            arglist += ['-jobconf', conf]

        # submit job
        create_packages_archive(packages, self.tmp_dir + '/packages.tar')

        job.dump(self.tmp_dir)

        print >> sys.stderr, arglist

        run_and_track_hadoop_job(arglist, tracking_url_callback=tracking_url_callback)

        # rename temporary work directory to given output
        tmp_target.move(output_final, fail_if_exists=True)
        self.finish()

    def finish(self):
        # FIXME: check for isdir?
        if self.tmp_dir and os.path.exists(self.tmp_dir):
            logger.debug('Removing directory %s', self.tmp_dir)
            shutil.rmtree(self.tmp_dir)

    def __del__(self):
        self.finish()