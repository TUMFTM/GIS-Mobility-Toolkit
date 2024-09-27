# -*- coding: utf-8 -*-
__author__ = "David Ziegler"
__copyright__ = "Copyright 2021, David Ziegler"
__credits__ = ["David Ziegler"]
__license__ = "MIT"
__version__ = "0.0.1"
__maintainer__ = "David Ziegler"
__email__ = "david.ziegler@tum.de"
__status__ = "Production"
__annotations__ = "Yaml config manager that deals with in Yaml imports and environment variable inserts as well as UTF-8"

import os, re
import tempfile
from typing import Any
from piny import MatcherWithDefaults, YamlLoader, ValidationError, LoadingError
from piny.loaders import yaml
from dotmap import DotMap

FRAMEWORK_ROOT = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
TEST_ROOT = os.path.join(FRAMEWORK_ROOT, "..", "tests")
TMP_ROOT = tempfile.gettempdir()
os.environ["PROJECT_DIR_PATH"] = FRAMEWORK_ROOT


class UTFYamlLoader(YamlLoader):

    def load(self, **params) -> Any:
        """
        Return Python object loaded (optionally validated) from the YAML-file

        :param params: named arguments used as optional loading params in validation
        """
        self._init_resolvers()
        try:
            with open(self.path, encoding='utf8') as fh:
                load = yaml.load(fh, Loader=self.matcher)
        except (yaml.YAMLError, FileNotFoundError) as e:
            raise LoadingError(origin=e, reason=str(e))

        if (self.validator is not None) and (self.schema is not None):
            return self.validator(self.schema, **self.schema_params).load(data=load, **params)
        return load


class MatcherWithDefaultsExt(MatcherWithDefaults):
    matcher = re.compile(r"\$\{([a-zA-Z_$0-9]+)(:-.*)?(:\?.*)?\}")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.add_constructor('!include', self.include)

    def _navigate_deep(self, yml, path):
        if path == '':
            return yml
        for p in path.split("/"):
            yml = yml[p]
        return yml

    def include(self, matcher, node):
        yml_path, sub = (node.value.split("|") + [""])[:2]
        node.value = yml_path.strip()
        yml = YamlLoader(path=self.constructor(None, node), matcher=MatcherWithDefaultsExt).load()

        return self._navigate_deep(yml, sub.strip())

    @staticmethod
    def constructor(loader, node):
        match = MatcherWithDefaultsExt.matcher.match(node.value)
        variable, default, error = match.groups()    # type: ignore

        if default:
            # lstrip() is dangerous!
            # It can remove legitimate first two letters in a value starting with `:-`
            default = default[2:]
            _env = os.environ.get(variable, default)
            return MatcherWithDefaultsExt.matcher.sub(_env.encode('unicode_escape').decode('ascii'), node.value)
        elif error:
            _env = os.environ.get(variable, None)
            if _env is None:
                raise ValidationError(origin="YamlParser", reason=str(error[2:]))
            return MatcherWithDefaultsExt.matcher.sub(_env.encode('unicode_escape').decode('ascii'), node.value)
        else:
            if variable == 'CONFIG_BASE_DIR':
                _env = os.path.dirname(loader.name)
            else:
                _env = os.environ.get(variable, default)
            assert _env is not None
            return MatcherWithDefaultsExt.matcher.sub(_env.encode('unicode_escape').decode('ascii'), node.value)


class ConfigManager(object):

    def __init__(self, path):
        self._path = path
        self._config = None

    def load(self, environment_vars=None):
        if environment_vars is not None:
            for k, v in environment_vars.items():
                os.environ[k] = v
        self._config = UTFYamlLoader(path=self._path, matcher=MatcherWithDefaultsExt).load()
        return self

    @property
    def config(self):
        return DotMap(self._config, _dynamic=False)
