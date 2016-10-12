__author__ = 'sun'

import luigi
from luigi.parameter import *


class SuperParameter(Parameter):
    counter = 0

    def __init__(self, description=None,
                 default_config = None):

        super(SuperParameter, self).__init__(description = description)
        self.value = None

        if not default_config:
            self.default_config_section = None
            self.default_config_name = None

        elif len(default_config) != 2:
            raise Exception("Invalid Default Config Setting")

        else:
            self.default_config_section, self.default_config_name = default_config



    def parse_from_input(self, param_name, x):

        self.value = super(SuperParameter, self).parse_from_input(param_name, x)

        return self.value

    @property
    def has_default(self):
        return self.default_config_section is not None and self.default_config_name is not None

    @property
    def default(self):

        section_str = self.default_config_section
        if isinstance(self.default_config_section, SuperParameter):
            section_str = self.default_config_section.value

        name_str = self.default_config_name
        if isinstance(self.default_config_name, SuperParameter):
            name_str = self.default_config_name.value

        conf = luigi.configuration.get_config()

        return self.parse(conf.get(section_str, name_str))




class MultiElemParameter(SuperParameter):
    # TODO(erikbern): why do we call this "boolean" instead of "bool"?
    # The integer parameter is called "int" so calling this "bool" would be
    # more consistent, especially given the Python type names.
    def __init__(self, splitter = None,  *args, **kwargs):
        super(MultiElemParameter, self).__init__(*args, **kwargs)

        self.splitter = splitter

    def parse(self, s):

        return tuple(filter(None, s.strip().split(self.splitter)))




class MultiLineParameter(MultiElemParameter):
    # TODO(erikbern): why do we call this "boolean" instead of "bool"?
    # The integer parameter is called "int" so calling this "bool" would be
    # more consistent, especially given the Python type names.
    def __init__(self, *args, **kwargs):
        super(MultiLineParameter, self).__init__(splitter = "\n", *args, **kwargs)

from collections import defaultdict
class FeatureSystemParameter(SuperParameter):

    def __init__(self, *args, **kwargs):
        super(FeatureSystemParameter, self).__init__(*args, **kwargs)


    def parse(self, s):

        input_feature_info = s.strip().split('\n')

        feature_system = defaultdict(list)

        conf = luigi.configuration.get_config()

        for info in input_feature_info:
            elems = info.split()
            dims = tuple(elems[0].split("_"))

            for elem in elems[1:]:
                section, name = elem.split('.')

                feature_system[dims].append(conf.get(section, name))

        return feature_system

class IndirectParameter(SuperParameter):

    def __init__(self, *args, **kwargs):
        super(IndirectParameter, self).__init__(*args, **kwargs)


    def parse(self, s):

        input_info = s.strip().split()

        input = []

        conf = luigi.configuration.get_config()

        for info in input_info:

            section, name = info.split('.')

            input.append(conf.get(section, name))

        return input


class MultipleReflectParameter(SuperParameter):

    def __init__(self, *args, **kwargs):
        super(MultipleReflectParameter, self).__init__(*args, **kwargs)


    def parse(self, s):

        sections = filter(None, s.strip().split("\n"))

        reflect_params = []

        conf = luigi.configuration.get_config()

        for section in sections:

            option_dict = dict(conf.items(section))
            reflect_params.append(option_dict)

        return reflect_params


# class DefaultConfigedParameter(RecsysParameter):
#
#     def __init__(self, section, name, *args, ** kwargs):
#         super(DefaultConfigedParameter, self).__init__(*args, ** kwargs)
#
#         self.section = section
#         self.name = name
#
#     @property
#     def has_default(self):
#         return True
#
#     @property
#     def default(self):
#
#         section_str = self.section
#         if isinstance(self.section, RecsysParameter):
#             section_str = self.section.value
#
#         name_str = self.name
#         if isinstance(self.name, RecsysParameter):
#             name_str = self.name.value
#
#         conf = luigi.configuration.get_config()
#
#         return self.parse(conf.get(section_str, name_str))
#
#
#     def parse(self,s):
#
#         result = super(ConfigedParameter, self).parse(s)
#
#         if result:
#             return result
#
#         section_str = self.section
#         if isinstance(self.section, RecsysParameter):
#             section_str = self.section.value
#
#         name_str = self.name
#         if isinstance(self.name, RecsysParameter):
#             name_str = self.name.value
#
#         conf = luigi.configuration.get_config()
#
#         return conf.get(section_str, name_str)
#
# class ConfigedIndirectParameter(ConfigedParameter):
#
#     def __init__(self, section, name, *args, ** kwargs):
#         super(ConfigedIndirectParameter, self).__init__(section, name, *args, ** kwargs)
#
#     def parse(self,s):
#
#         conf = luigi.configuration.get_config()
#
#         result = super(ConfigedIndirectParameter, self).parse(s)
#
#         input = []
#         for info in result:
#
#             section, name = info.split('.')
#
#             input.append(conf.get(section, name))
#
#         return input


