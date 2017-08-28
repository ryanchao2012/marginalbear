import types
import logging
from .utils import OkLogger

oklogger = OkLogger('okpipeline')


class OkPipeline(object):

    def __init__(self, target_obj, init_attr_ls, ls, logger_name='okpipeline'):
        self.obj = target_obj
        self.curr_attr_ls = init_attr_ls
        self.pipe = ls
        self.logger = logging.getLogger(logger_name)

    def run(self):
        for node in self.pipe:
            in_ls, out_ls, fn = self._format_node(node)

            self.logger.info('Add attributes: {}'.format(out_ls))
            in_attr = [self._rget_attr(self.obj, a) for a in in_ls]
            out_ = fn(*in_attr)
            if isinstance(out_, tuple):
                out = list(out_)
            else:
                out = [out_]
            if len(out) != len(out_ls):
                raise AssertionError('Output lengths don\'t match.')

            for attr, value in zip(out_ls, out):
                self._rset_attr(self.obj, attr, value)

            self.curr_attr_ls = out_ls

        return out_

    def _format_node(self, node):
        arg_num = len(node)
        if arg_num == 1:
            fn = node[0]
            fn_name = self._get_obj_name(fn)
            in_ls = self.curr_attr_ls
            out_ls = ['{}__{}'.format(in_ls[0], fn_name)]

        elif arg_num == 2:
            fn = node[1]
            fn_name = self._get_obj_name(fn)

            io_tp = node[0]
            in_ls = io_tp[0]
            io_num = len(io_tp)
            if io_num == 1:
                out_ls = ['{}__{}'.format(in_ls[0], fn_name)]
            elif io_num == 2:
                out_ls = io_tp[1]
            else:
                raise AssertionError('Too many IO.')
        else:
            raise AssertionError('Too many arguments.')

        return in_ls, out_ls, fn

    def _get_obj_name(self, obj):
        if isinstance(obj, types.FunctionType):
            return obj.__name__.lower()
        else:
            return obj.__class__.__name__.lower()

    def _rget_attr(self, obj, attr_name):
        idx = attr_name.find('.')
        if idx < 0:
            if hasattr(obj, attr_name):
                return obj.__getattribute__(attr_name)
            else:
                raise AttributeError('No attribute: "{}" in {}.'.format(attr_name, obj))
        elif idx == 0:
            raise AttributeError('Attibute name start from "." character.')
        else:
            attr_ = attr_name[:idx]
            nattr = attr_name[idx + 1:]
            if hasattr(obj, attr_):
                return self._rget_attr(obj.__getattribute__(attr_), nattr)
            else:
                raise AttributeError('No attribute: "{}" in {}.'.format(attr_, obj))

    def _rset_attr(self, obj, attr_name, value):
        idx = attr_name.find('.')
        if idx < 0:
            obj.__setattr__(attr_name, value)
        else:
            attr_ = attr_name[:idx]
            nattr = attr_name[idx + 1:]
            if hasattr(obj, attr_):
                self._rset_attr(obj.__getattribute__(attr_), nattr, value)
            else:
                raise AttributeError('No attribute: "{}" in {}.'.format(attr_, obj))

