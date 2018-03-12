import os
import re
import shutil
import struct
from zlib import crc32
from collections import defaultdict

from .parser import SourceBuilder, TLParser, TLObject
AUTO_GEN_NOTICE = \
    '"""File generated by TLObjects\' generator. All changes will be ERASED"""'


AUTO_CASTS = {
    'InputPeer': 'utils.get_input_peer(client.get_input_entity({}))',
    'InputChannel': 'utils.get_input_channel(client.get_input_entity({}))',
    'InputUser': 'utils.get_input_user(client.get_input_entity({}))',
    'InputMedia': 'utils.get_input_media({})',
    'InputPhoto': 'utils.get_input_photo({})'
}


class TLGenerator:
    def __init__(self, output_dir):
        self.output_dir = output_dir

    def _get_file(self, *paths):
        return os.path.join(self.output_dir, *paths)

    def _rm_if_exists(self, filename):
        file = self._get_file(filename)
        if os.path.exists(file):
            if os.path.isdir(file):
                shutil.rmtree(file)
            else:
                os.remove(file)

    def tlobjects_exist(self):
        """Determines whether the TLObjects were previously
           generated (hence exist) or not
        """
        return os.path.isfile(self._get_file('all_tlobjects.py'))

    def clean_tlobjects(self):
        """Cleans the automatically generated TLObjects from disk"""
        for name in ('functions', 'types', 'all_tlobjects.py'):
            self._rm_if_exists(name)

    def generate_tlobjects(self, scheme_file, import_depth):
        """Generates all the TLObjects from scheme.tl to
           tl/functions and tl/types
        """

        # First ensure that the required parent directories exist
        os.makedirs(self._get_file('functions'), exist_ok=True)
        os.makedirs(self._get_file('types'), exist_ok=True)

        # Step 0: Cache the parsed file on a tuple
        tlobjects = tuple(TLParser.parse_file(scheme_file, ignore_core=True))

        # Step 1: Group everything by {namespace: [tlobjects]} so we can
        # easily generate __init__.py files with all the TLObjects on them.
        namespace_functions = defaultdict(list)
        namespace_types = defaultdict(list)

        # Make use of this iteration to also store 'Type: [Constructors]',
        # used when generating the documentation for the classes.
        type_constructors = defaultdict(list)
        for tlobject in tlobjects:
            if tlobject.is_function:
                namespace_functions[tlobject.namespace].append(tlobject)
            else:
                namespace_types[tlobject.namespace].append(tlobject)
                type_constructors[tlobject.result].append(tlobject)

        # Step 2: Generate the actual code
        self._write_init_py(
            self._get_file('functions'), import_depth,
            namespace_functions, type_constructors
        )
        self._write_init_py(
            self._get_file('types'), import_depth,
            namespace_types, type_constructors
        )

        # Step 4: Once all the objects have been generated,
        #         we can now group them in a single file
        filename = os.path.join(self._get_file('all_tlobjects.py'))
        with open(filename, 'w', encoding='utf-8') as file:
            with SourceBuilder(file) as builder:
                builder.writeln(AUTO_GEN_NOTICE)
                builder.writeln()

                builder.writeln('from . import types, functions')
                builder.writeln()

                # Create a constant variable to indicate which layer this is
                builder.writeln('LAYER = {}'.format(
                    TLParser.find_layer(scheme_file))
                )
                builder.writeln()

                # Then create the dictionary containing constructor_id: class
                builder.writeln('tlobjects = {')
                builder.current_indent += 1

                # Fill the dictionary (0x1a2b3c4f: tl.full.type.path.Class)
                for tlobject in tlobjects:
                    constructor = hex(tlobject.id)
                    if len(constructor) != 10:
                        # Make it a nice length 10 so it fits well
                        constructor = '0x' + constructor[2:].zfill(8)

                    builder.write('{}: '.format(constructor))
                    builder.write(
                        'functions' if tlobject.is_function else 'types')

                    if tlobject.namespace:
                        builder.write('.' + tlobject.namespace)

                    builder.writeln('.{},'.format(tlobject.class_name()))

                builder.current_indent -= 1
                builder.writeln('}')

    @staticmethod
    def _write_init_py(out_dir, depth, namespace_tlobjects, type_constructors):
        # namespace_tlobjects: {'namespace', [TLObject]}
        os.makedirs(out_dir, exist_ok=True)
        for ns, tlobjects in namespace_tlobjects.items():
            file = os.path.join(out_dir, ns + '.py' if ns else '__init__.py')
            with open(file, 'w', encoding='utf-8') as f, \
                    SourceBuilder(f) as builder:
                builder.writeln(AUTO_GEN_NOTICE)

                # Both types and functions inherit from the TLObject class
                # so they all can be serialized and sent, however, only the
                # functions are "content_related".
                builder.writeln(
                    'from {}.tl.tlobject import TLObject'.format('.' * depth)
                )
                builder.writeln('from typing import Optional, List, Union, TYPE_CHECKING')

                # Add the relative imports to the namespaces,
                # unless we already are in a namespace.
                if not ns:
                    builder.writeln('from . import {}'.format(', '.join(
                        x for x in namespace_tlobjects.keys() if x
                    )))

                # Import 'os' for those needing access to 'os.urandom()'
                # Currently only 'random_id' needs 'os' to be imported,
                # for all those TLObjects with arg.can_be_inferred.
                builder.writeln('import os')

                # Import struct for the .__bytes__(self) serialization
                builder.writeln('import struct')

                tlobjects.sort(key=lambda x: x.name)

                type_names = set()
                type_defs = []

                # Find all the types in this file and generate type definitions
                # based on the types. The type definitions are written to the
                # file at the end.
                for t in tlobjects:
                    if not t.is_function:
                        type_name = t.result
                        if '.' in type_name:
                            type_name = type_name[type_name.rindex('.'):]
                        if type_name in type_names:
                            continue
                        type_names.add(type_name)
                        constructors = type_constructors[type_name]
                        if not constructors:
                            pass
                        elif len(constructors) == 1:
                            type_defs.append('Type{} = {}'.format(
                                type_name, constructors[0].class_name()))
                        else:
                            type_defs.append('Type{} = Union[{}]'.format(
                                type_name, ','.join(c.class_name()
                                                    for c in constructors)))

                imports = {}
                primitives = ('int', 'long', 'int128', 'int256', 'string',
                              'date', 'bytes', 'true')
                # Find all the types in other files that are used in this file
                # and generate the information required to import those types.
                for t in tlobjects:
                    for arg in t.args:
                        name = arg.type
                        if not name or name in primitives:
                            continue

                        import_space = '{}.tl.types'.format('.' * depth)
                        if '.' in name:
                            namespace = name.split('.')[0]
                            name = name.split('.')[1]
                            import_space += '.{}'.format(namespace)

                        if name not in type_names:
                            type_names.add(name)
                            if name == 'date':
                                imports['datetime'] = ['datetime']
                                continue
                            elif not import_space in imports:
                                imports[import_space] = set()
                            imports[import_space].add('Type{}'.format(name))

                # Add imports required for type checking.
                builder.writeln('if TYPE_CHECKING:')
                for namespace, names in imports.items():
                    builder.writeln('from {} import {}'.format(
                        namespace, ', '.join(names)))
                else:
                    builder.writeln('pass')
                builder.end_block()

                # Generate the class for every TLObject
                for t in tlobjects:
                    TLGenerator._write_source_code(
                        t, builder, depth, type_constructors
                    )
                    builder.current_indent = 0

                # Write the type definitions generated earlier.
                builder.writeln('')
                for line in type_defs:
                    builder.writeln(line)


    @staticmethod
    def _write_source_code(tlobject, builder, depth, type_constructors):
        """Writes the source code corresponding to the given TLObject
           by making use of the 'builder' SourceBuilder.

           Additional information such as file path depth and
           the Type: [Constructors] must be given for proper
           importing and documentation strings.
        """
        builder.writeln()
        builder.writeln()
        builder.writeln('class {}(TLObject):'.format(tlobject.class_name()))

        # Class-level variable to store its Telegram's constructor ID
        builder.writeln('CONSTRUCTOR_ID = {}'.format(hex(tlobject.id)))
        builder.writeln('SUBCLASS_OF_ID = {}'.format(
            hex(crc32(tlobject.result.encode('ascii'))))
        )
        builder.writeln()

        # Flag arguments must go last
        args = [
            a for a in tlobject.sorted_args()
            if not a.flag_indicator and not a.generic_definition
        ]

        # Convert the args to string parameters, flags having =None
        args = [
            (a.name if not a.is_flag and not a.can_be_inferred
             else '{}=None'.format(a.name))
            for a in args
        ]

        # Write the __init__ function
        if args:
            builder.writeln(
                'def __init__(self, {}):'.format(', '.join(args))
            )
        else:
            builder.writeln('def __init__(self):')

        # Now update args to have the TLObject arguments, _except_
        # those which are calculated on send or ignored, this is
        # flag indicator and generic definitions.
        #
        # We don't need the generic definitions in Python
        # because arguments can be any type
        args = [arg for arg in tlobject.args
                if not arg.flag_indicator and
                not arg.generic_definition]

        if args:
            # Write the docstring, to know the type of the args
            builder.writeln('"""')
            for arg in args:
                if not arg.flag_indicator:
                    builder.writeln(':param {} {}:'.format(
                        arg.doc_type_hint(), arg.name
                    ))
                    builder.current_indent -= 1  # It will auto-indent (':')

            # We also want to know what type this request returns
            # or to which type this constructor belongs to
            builder.writeln()
            if tlobject.is_function:
                builder.write(':returns {}: '.format(tlobject.result))
            else:
                builder.write('Constructor for {}: '.format(tlobject.result))

            constructors = type_constructors[tlobject.result]
            if not constructors:
                builder.writeln('This type has no constructors.')
            elif len(constructors) == 1:
                builder.writeln('Instance of {}.'.format(
                    constructors[0].class_name()
                ))
            else:
                builder.writeln('Instance of either {}.'.format(
                    ', '.join(c.class_name() for c in constructors)
                ))

            builder.writeln('"""')

        builder.writeln('super().__init__()')
        # Functions have a result object and are confirmed by default
        if tlobject.is_function:
            builder.writeln('self.result = None')
            builder.writeln(
                'self.content_related = True')

        # Set the arguments
        if args:
            # Leave an empty line if there are any args
            builder.writeln()

        for arg in args:
            if not arg.can_be_inferred:
                builder.writeln('self.{0} = {0}  # type: {1}'.format(
                    arg.name, arg.python_type_hint()))
                continue

            # Currently the only argument that can be
            # inferred are those called 'random_id'
            if arg.name == 'random_id':
                # Endianness doesn't really matter, and 'big' is shorter
                code = "int.from_bytes(os.urandom({}), 'big', signed=True)" \
                    .format(8 if arg.type == 'long' else 4)

                if arg.is_vector:
                    # Currently for the case of "messages.forwardMessages"
                    # Ensure we can infer the length from id:Vector<>
                    if not next(
                            a for a in args if a.name == 'id').is_vector:
                        raise ValueError(
                            'Cannot infer list of random ids for ', tlobject
                        )
                    code = '[{} for _ in range(len(id))]'.format(code)

                builder.writeln(
                    "self.random_id = random_id if random_id "
                    "is not None else {}".format(code)
                )
            else:
                raise ValueError('Cannot infer a value for ', arg)

        builder.end_block()

        # Write the resolve(self, client, utils) method
        if any(arg.type in AUTO_CASTS for arg in args):
            builder.writeln('def resolve(self, client, utils):')
            for arg in args:
                ac = AUTO_CASTS.get(arg.type, None)
                if ac:
                    TLGenerator._write_self_assign(builder, arg, ac)
            builder.end_block()

        # Write the to_dict(self) method
        builder.writeln('def to_dict(self):')
        builder.writeln('return {')
        builder.current_indent += 1

        base_types = ('string', 'bytes', 'int', 'long', 'int128',
                      'int256', 'double', 'Bool', 'true', 'date')

        builder.write("'_': '{}'".format(tlobject.class_name()))
        for arg in args:
            builder.writeln(',')
            builder.write("'{}': ".format(arg.name))
            if arg.type in base_types:
                if arg.is_vector:
                    builder.write('[] if self.{0} is None else self.{0}[:]'
                                  .format(arg.name))
                else:
                    builder.write('self.{}'.format(arg.name))
            else:
                if arg.is_vector:
                    builder.write(
                        '[] if self.{0} is None else [None '
                        'if x is None else x.to_dict() for x in self.{0}]'
                        .format(arg.name)
                    )
                else:
                    builder.write(
                        'None if self.{0} is None else self.{0}.to_dict()'
                        .format(arg.name)
                    )

        builder.writeln()
        builder.current_indent -= 1
        builder.writeln("}")

        builder.end_block()

        # Write the .__bytes__() function
        builder.writeln('def __bytes__(self):')

        # Some objects require more than one flag parameter to be set
        # at the same time. In this case, add an assertion.
        repeated_args = defaultdict(list)
        for arg in tlobject.args:
            if arg.is_flag:
                repeated_args[arg.flag_index].append(arg)

        for ra in repeated_args.values():
            if len(ra) > 1:
                cnd1 = ('(self.{0} or self.{0} is not None)'
                        .format(a.name) for a in ra)
                cnd2 = ('(self.{0} is None or self.{0} is False)'
                        .format(a.name) for a in ra)
                builder.writeln(
                    "assert ({}) or ({}), '{} parameters must all "
                    "be False-y (like None) or all me True-y'".format(
                        ' and '.join(cnd1), ' and '.join(cnd2),
                        ', '.join(a.name for a in ra)
                    )
                )

        builder.writeln("return b''.join((")
        builder.current_indent += 1

        # First constructor code, we already know its bytes
        builder.writeln('{},'.format(repr(struct.pack('<I', tlobject.id))))

        for arg in tlobject.args:
            if TLGenerator.write_to_bytes(builder, arg, tlobject.args):
                builder.writeln(',')

        builder.current_indent -= 1
        builder.writeln('))')
        builder.end_block()

        # Write the static from_reader(reader) function
        builder.writeln('@staticmethod')
        builder.writeln('def from_reader(reader):')
        for arg in tlobject.args:
            TLGenerator.write_read_code(
                builder, arg, tlobject.args, name='_' + arg.name
            )

        builder.writeln('return {}({})'.format(
            tlobject.class_name(), ', '.join(
                '{0}=_{0}'.format(a.name) for a in tlobject.sorted_args()
                if not a.flag_indicator and not a.generic_definition
            )
        ))

        # Only requests can have a different response that's not their
        # serialized body, that is, we'll be setting their .result.
        #
        # The default behaviour is reading a TLObject too, so no need
        # to override it unless necessary.
        if tlobject.is_function and not TLGenerator._is_boxed(tlobject.result):
            builder.end_block()
            builder.writeln('def on_response(self, reader):')
            TLGenerator.write_request_result_code(builder, tlobject)

    @staticmethod
    def _is_boxed(type_):
        # https://core.telegram.org/mtproto/serialize#boxed-and-bare-types
        # TL;DR; boxed types start with uppercase always, so we can use
        # this to check whether everything in it is boxed or not.
        #
        # The API always returns a boxed type, but it may inside a Vector<>
        # or a namespace, and the Vector may have a not-boxed type. For this
        # reason we find whatever index, '<' or '.'. If neither are present
        # we will get -1, and the 0th char is always upper case thus works.
        # For Vector types and namespaces, it will check in the right place.
        check_after = max(type_.find('<'), type_.find('.'))
        return type_[check_after + 1].isupper()

    @staticmethod
    def _write_self_assign(builder, arg, get_input_code):
        """Writes self.arg = input.format(self.arg), considering vectors"""
        if arg.is_vector:
            builder.write('self.{0} = [{1} for _x in self.{0}]'
                          .format(arg.name, get_input_code.format('_x')))
        else:
            builder.write('self.{} = {}'.format(
                arg.name, get_input_code.format('self.' + arg.name)))

        builder.writeln(
            ' if self.{} else None'.format(arg.name) if arg.is_flag else ''
        )

    @staticmethod
    def get_file_name(tlobject, add_extension=False):
        """Gets the file name in file_name_format.py for the given TLObject"""

        # Courtesy of http://stackoverflow.com/a/1176023/4759433
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', tlobject.name)
        result = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        if add_extension:
            return result + '.py'
        else:
            return result

    @staticmethod
    def write_to_bytes(builder, arg, args, name=None):
        """
        Writes the .__bytes__() code for the given argument
        :param builder: The source code builder
        :param arg: The argument to write
        :param args: All the other arguments in TLObject same __bytes__.
                     This is required to determine the flags value
        :param name: The name of the argument. Defaults to "self.argname"
                     This argument is an option because it's required when
                     writing Vectors<>
        """
        if arg.generic_definition:
            return  # Do nothing, this only specifies a later type

        if name is None:
            name = 'self.{}'.format(arg.name)

        # The argument may be a flag, only write if it's not None AND
        # if it's not a True type.
        # True types are not actually sent, but instead only used to
        # determine the flags.
        if arg.is_flag:
            if arg.type == 'true':
                return  # Exit, since True type is never written
            elif arg.is_vector:
                # Vector flags are special since they consist of 3 values,
                # so we need an extra join here. Note that empty vector flags
                # should NOT be sent either!
                builder.write("b'' if {0} is None or {0} is False "
                              "else b''.join((".format(name))
            else:
                builder.write("b'' if {0} is None or {0} is False "
                              "else (".format(name))

        if arg.is_vector:
            if arg.use_vector_id:
                # vector code, unsigned 0x1cb5c415 as little endian
                builder.write(r"b'\x15\xc4\xb5\x1c',")

            builder.write("struct.pack('<i', len({})),".format(name))

            # Cannot unpack the values for the outer tuple through *[(
            # since that's a Python >3.5 feature, so add another join.
            builder.write("b''.join(")

            # Temporary disable .is_vector, not to enter this if again
            # Also disable .is_flag since it's not needed per element
            old_flag = arg.is_flag
            arg.is_vector = arg.is_flag = False
            TLGenerator.write_to_bytes(builder, arg, args, name='x')
            arg.is_vector = True
            arg.is_flag = old_flag

            builder.write(' for x in {})'.format(name))

        elif arg.flag_indicator:
            # Calculate the flags with those items which are not None
            if not any(f.is_flag for f in args):
                # There's a flag indicator, but no flag arguments so it's 0
                builder.write(r"b'\0\0\0\0'")
            else:
                builder.write("struct.pack('<I', ")
                builder.write(
                    ' | '.join('(0 if {0} is None or {0} is False else {1})'
                               .format('self.{}'.format(flag.name),
                                       1 << flag.flag_index)
                               for flag in args if flag.is_flag)
                )
                builder.write(')')

        elif 'int' == arg.type:
            # struct.pack is around 4 times faster than int.to_bytes
            builder.write("struct.pack('<i', {})".format(name))

        elif 'long' == arg.type:
            builder.write("struct.pack('<q', {})".format(name))

        elif 'int128' == arg.type:
            builder.write("{}.to_bytes(16, 'little', signed=True)".format(name))

        elif 'int256' == arg.type:
            builder.write("{}.to_bytes(32, 'little', signed=True)".format(name))

        elif 'double' == arg.type:
            builder.write("struct.pack('<d', {})".format(name))

        elif 'string' == arg.type:
            builder.write('TLObject.serialize_bytes({})'.format(name))

        elif 'Bool' == arg.type:
            # 0x997275b5 if boolean else 0xbc799737
            builder.write(
                r"b'\xb5ur\x99' if {} else b'7\x97y\xbc'".format(name)
            )

        elif 'true' == arg.type:
            pass  # These are actually NOT written! Only used for flags

        elif 'bytes' == arg.type:
            builder.write('TLObject.serialize_bytes({})'.format(name))

        elif 'date' == arg.type:  # Custom format
            builder.write('TLObject.serialize_datetime({})'.format(name))

        else:
            # Else it may be a custom type
            builder.write('bytes({})'.format(name))

        if arg.is_flag:
            builder.write(')')
            if arg.is_vector:
                builder.write(')')  # We were using a tuple

        return True  # Something was written

    @staticmethod
    def write_read_code(builder, arg, args, name):
        """
        Writes the read code for the given argument, setting the
        arg.name variable to its read value.

        :param builder: The source code builder
        :param arg: The argument to write
        :param args: All the other arguments in TLObject same on_send.
                     This is required to determine the flags value
        :param name: The name of the argument. Defaults to "self.argname"
                     This argument is an option because it's required when
                     writing Vectors<>
        """

        if arg.generic_definition:
            return  # Do nothing, this only specifies a later type

        # The argument may be a flag, only write that flag was given!
        was_flag = False
        if arg.is_flag:
            # Treat 'true' flags as a special case, since they're true if
            # they're set, and nothing else needs to actually be read.
            if 'true' == arg.type:
                builder.writeln(
                    '{} = bool(flags & {})'.format(name, 1 << arg.flag_index)
                )
                return

            was_flag = True
            builder.writeln('if flags & {}:'.format(
                1 << arg.flag_index
            ))
            # Temporary disable .is_flag not to enter this if
            # again when calling the method recursively
            arg.is_flag = False

        if arg.is_vector:
            if arg.use_vector_id:
                # We have to read the vector's constructor ID
                builder.writeln("reader.read_int()")

            builder.writeln('{} = []'.format(name))
            builder.writeln('for _ in range(reader.read_int()):')
            # Temporary disable .is_vector, not to enter this if again
            arg.is_vector = False
            TLGenerator.write_read_code(builder, arg, args, name='_x')
            builder.writeln('{}.append(_x)'.format(name))
            arg.is_vector = True

        elif arg.flag_indicator:
            # Read the flags, which will indicate what items we should read next
            builder.writeln('flags = reader.read_int()')
            builder.writeln()

        elif 'int' == arg.type:
            builder.writeln('{} = reader.read_int()'.format(name))

        elif 'long' == arg.type:
            builder.writeln('{} = reader.read_long()'.format(name))

        elif 'int128' == arg.type:
            builder.writeln(
                '{} = reader.read_large_int(bits=128)'.format(name)
            )

        elif 'int256' == arg.type:
            builder.writeln(
                '{} = reader.read_large_int(bits=256)'.format(name)
            )

        elif 'double' == arg.type:
            builder.writeln('{} = reader.read_double()'.format(name))

        elif 'string' == arg.type:
            builder.writeln('{} = reader.tgread_string()'.format(name))

        elif 'Bool' == arg.type:
            builder.writeln('{} = reader.tgread_bool()'.format(name))

        elif 'true' == arg.type:
            # Arbitrary not-None value, don't actually read "true" flags
            builder.writeln('{} = True'.format(name))

        elif 'bytes' == arg.type:
            builder.writeln('{} = reader.tgread_bytes()'.format(name))

        elif 'date' == arg.type:  # Custom format
            builder.writeln('{} = reader.tgread_date()'.format(name))

        else:
            # Else it may be a custom type
            if not arg.skip_constructor_id:
                builder.writeln('{} = reader.tgread_object()'.format(name))
            else:
                # Import the correct type inline to avoid cyclic imports.
                # There may be better solutions so that we can just access
                # all the types before the files have been parsed, but I
                # don't know of any.
                sep_index = arg.type.find('.')
                if sep_index == -1:
                    ns, t = '.', arg.type
                else:
                    ns, t = '.' + arg.type[:sep_index], arg.type[sep_index+1:]
                class_name = TLObject.class_name_for(t)

                # There would be no need to import the type if we're in the
                # file with the same namespace, but since it does no harm
                # and we don't have information about such thing in the
                # method we just ignore that case.
                builder.writeln('from {} import {}'.format(ns, class_name))
                builder.writeln('{} = {}.from_reader(reader)'.format(
                    name, class_name
                ))

        # End vector and flag blocks if required (if we opened them before)
        if arg.is_vector:
            builder.end_block()

        if was_flag:
            builder.current_indent -= 1
            builder.writeln('else:')
            builder.writeln('{} = None'.format(name))
            builder.current_indent -= 1
            # Restore .is_flag
            arg.is_flag = True

    @staticmethod
    def write_request_result_code(builder, tlobject):
        """
        Writes the receive code for the given function

        :param builder: The source code builder
        :param tlobject: The TLObject for which the 'self.result = '
                         will be written
        """
        if tlobject.result.startswith('Vector<'):
            # Vector results are a bit special since they can also be composed
            # of integer values and such; however, the result of requests is
            # not parsed as arguments are and it's a bit harder to tell which
            # is which.
            if tlobject.result == 'Vector<int>':
                builder.writeln('reader.read_int()  # Vector ID')
                builder.writeln('count = reader.read_int()')
                builder.writeln(
                    'self.result = [reader.read_int() for _ in range(count)]'
                )
            elif tlobject.result == 'Vector<long>':
                builder.writeln('reader.read_int()  # Vector ID')
                builder.writeln('count = reader.read_long()')
                builder.writeln(
                    'self.result = [reader.read_long() for _ in range(count)]'
                )
            else:
                builder.writeln('self.result = reader.tgread_vector()')
        else:
            builder.writeln('self.result = reader.tgread_object()')
