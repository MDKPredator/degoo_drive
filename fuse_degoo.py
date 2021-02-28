#!/usr/bin/env python3

import os
import sys
import time
from pathlib import Path

import pyfuse3
from argparse import ArgumentParser
import errno
import logging
import stat as stat_m
from pyfuse3 import FUSEError
from os import fsencode, fsdecode
from collections import defaultdict
import trio
import faulthandler
import urllib3
import datetime
import tempfile
import threading
import mimetypes
import degoo

# to load the module from there first.
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
        os.path.exists(os.path.join(basedir, 'src', 'pyfuse3.pyx'))):
    sys.path.insert(0, os.path.join(basedir, 'src'))

faulthandler.enable()

log = logging.getLogger(__name__)

degoo_tree_content = {}

ROOT_DEGOO = 0

PATH_ROOT_DEGOO = '/'

cache_thread_running = False

class Operations(pyfuse3.Operations):
    enable_writeback_cache = True

    def __init__(self, source, cache_size):
        super().__init__()
        root_id = self._get_degoo_id(source)
        self._inode_path_map = {pyfuse3.ROOT_INODE: source, root_id: source}
        self._lookup_cnt = defaultdict(lambda: 0)
        self._fd_inode_map = dict()
        self._inode_fd_map = dict()
        self._fd_open_count = dict()
        self._degoo_path = dict()
        self._fd_buffer_length = dict()
        self._cache_size = cache_size

    def _inode_to_path(self, inode, fullpath=False):
        try:
            val = self._inode_path_map[inode]
        except KeyError:
            raise FUSEError(errno.ENOENT)

        if pyfuse3.ROOT_INODE == inode:
            return val

        if '/' in val and not fullpath:
            val = val[val.rfind('/') + 1:]

        if isinstance(val, set):
            # In case of hardlinks, pick any path
            val = next(iter(val))
        return val

    def _add_path(self, inode, path):
        log.debug('_add_path for %d, %s', inode, path)
        self._lookup_cnt[inode] += 1

        # With hardlinks, one inode may map to multiple paths.
        if inode not in self._inode_path_map:
            self._inode_path_map[inode] = path
            return

        val = self._inode_path_map[inode]
        if isinstance(val, set):
            val.add(path)
        elif val != path:
            self._inode_path_map[inode] = {path, val}

    async def forget(self, inode_list):
        for (inode, nlookup) in inode_list:
            if self._lookup_cnt[inode] > nlookup:
                self._lookup_cnt[inode] -= nlookup
                continue
            log.debug('forgetting about inode %d', inode)
            assert inode not in self._inode_fd_map
            del self._lookup_cnt[inode]
            try:
                del self._inode_path_map[inode]
            except KeyError:  # may have been deleted
                pass

    async def lookup(self, inode_p, name, ctx=None):
        # Avoid caching of files
        raise pyfuse3.FUSEError(errno.ENOENT)

    async def getattr(self, inode, ctx=None):
        if inode in self._inode_fd_map:
            return self._getattr(fd=self._inode_fd_map[inode])
        else:
            return self._get_degoo_attrs(self._inode_to_path(inode))

    async def readlink(self, inode, ctx):
        path = self._inode_to_path(inode)
        try:
            target = os.readlink(path)
        except OSError as exc:
            raise FUSEError(exc.errno)
        return fsencode(target)

    async def opendir(self, inode, ctx):
        return inode

    def _get_degoo_id(self, name):
        folder_id = None
        attr = 'FilePath' if '/' in name else 'Name'
        for idx, degoo_element in degoo_tree_content.items():
            if degoo_element[attr] == name:
                folder_id = degoo_element['ID']
                break
        return folder_id

    def _get_degoo_element(self, name):
        element_id = self._get_degoo_id(name)
        element = None
        if element_id is not None:
            element = self._get_degoo_element_by_id(element_id)
        return element

    def _get_degoo_element_by_id(self, element_id):
        if element_id == pyfuse3.ROOT_INODE:
            element_id = ROOT_DEGOO
        return degoo_tree_content[element_id]

    def _get_degoo_element_path_by_id(self, element_id):
        element = self._get_degoo_element_by_id(element_id)
        return element['FilePath']

    def _get_degoo_element_path_by_name(self, name):
        element = self._get_degoo_element(name)
        return element['FilePath']

    def _get_degoo_childs(self, parent_id):
        childs = []
        if parent_id is not None:
            for idx, degoo_element in degoo_tree_content.items():
                if degoo_element['ParentID'] == parent_id:
                    childs.append(degoo_element)
        return childs

    def _get_degoo_attrs(self, name):
        element = self._get_degoo_element(name)
        if not element:
            raise FUSEError(2)

        entry = pyfuse3.EntryAttributes()

        if int(element['ID']) == ROOT_DEGOO or element['isFolder']:
            entry.st_size = 0
            entry.st_mode = (stat_m.S_IFDIR | 0o755)
        else:
            entry.st_size = int(element['Size'])
            entry.st_mode = (stat_m.S_IFREG | 0o664)

        entry.st_ino = int(element['ID'])
        entry.st_uid = os.getuid()
        entry.st_gid = os.getgid()
        entry.st_blksize = 512
        entry.st_blocks = ((entry.st_size+entry.st_blksize-1) // entry.st_blksize)

        timestamp = int(1438467123.985654)

        try:
            entry.st_atime_ns = int(element['LastUploadTime']) * 1e9
        except KeyError:
            entry.st_atime_ns = timestamp
        try:
            entry.st_ctime_ns = int(element['LastModificationTime']) * 1e9
        except KeyError:
            entry.st_ctime_ns = timestamp
        try:
            creation_time = datetime.datetime.fromisoformat(element['CreationTime'])
            entry.st_mtime_ns = creation_time.timestamp() * 1e9
        except KeyError:
            entry.st_mtime_ns = timestamp

        return entry

    def _getattr(self, path=None, fd=None):
        assert fd is None or path is None
        assert not(fd is None and path is None)
        try:
            if fd is None:
                stat = os.lstat(path)
            else:
                stat = os.fstat(fd)
        except OSError as exc:
            raise FUSEError(exc.errno)

        entry = pyfuse3.EntryAttributes()
        for attr in ('st_ino', 'st_mode', 'st_nlink', 'st_uid', 'st_gid',
                     'st_rdev', 'st_size', 'st_atime_ns', 'st_mtime_ns',
                     'st_ctime_ns'):
            setattr(entry, attr, getattr(stat, attr))
        entry.generation = 0
        entry.entry_timeout = 0
        entry.attr_timeout = 0
        entry.st_blksize = 512
        entry.st_blocks = ((entry.st_size+entry.st_blksize-1) // entry.st_blksize)

        return entry

    async def readdir(self, inode, off, token):
        path = self._inode_to_path(inode)
        log.debug('reading %s', path)

        path_aux = path
        if '/' in path and pyfuse3.ROOT_INODE != inode:
            path_aux = path[path.index('/') + 1:]

        parent_id = self._get_degoo_id(path_aux)
        childs = self._get_degoo_childs(parent_id)
        entries = []
        for element in childs:
            attr = self._get_degoo_attrs(element['Name'])
            entries.append((attr.st_ino, element['Name'], attr))

        for (ino, name, attr) in sorted(entries):
            if ino <= off:
                continue
            if not pyfuse3.readdir_reply(
                    token, fsencode(name), attr, ino):
                break
            self._add_path(attr.st_ino, self._get_degoo_element_path_by_id(attr.st_ino))

    async def unlink(self, inode_p, name, ctx):
        name = fsdecode(name)
        parent = self._inode_to_path(inode_p, fullpath=True)
        path = parent + '/' + name

        # Get the id to file to delete to avoid duplicates with name
        file_id = self._get_degoo_id(path)
        degoo.rm(file_id)

        if file_id in self._lookup_cnt:
            self._forget_path(file_id, path)

    async def rmdir(self, inode_p, name, ctx):
        name = fsdecode(name)
        parent = self._inode_to_path(inode_p, fullpath=True)
        path = parent + '/' + name

        # Get the id to file to delete to avoid duplicates with name
        file_id = self._get_degoo_id(path)
        degoo.rm(file_id)

        if file_id in self._lookup_cnt:
            self._forget_path(file_id, path)

    def _forget_path(self, inode, path):
        log.debug('forget %s for %d', path, inode)
        val = self._inode_path_map[inode]
        if isinstance(val, set):
            val.remove(path)
            if len(val) == 1:
                self._inode_path_map[inode] = next(iter(val))
        else:
            del self._inode_path_map[inode]

        refresh_degoo_content()

    async def rename(self, inode_p_old, name_old, inode_p_new, name_new,
                     flags, ctx):
        if flags != 0:
            raise FUSEError(errno.EINVAL)

        name_old = fsdecode(name_old)
        name_new = fsdecode(name_new)

        path = self._inode_to_path(inode_p_old, fullpath=True)
        path_old = path + '/' + name_old

        inode = self._get_degoo_id(path_old)

        # It is a rename
        if inode_p_old == inode_p_new:
            degoo.rename(path_old, name_new)
        else:
            # If name it is different, it is a move with rename
            if name_old != name_new:
                degoo.rename(path_old, name_new)
                refresh_degoo_content()
                path_old = path + '/' + name_new
            path = self._inode_to_path(inode_p_new, fullpath=True)
            degoo.mv(path_old, path)

        path_new = path + '/' + name_new

        val = self._inode_path_map[inode]
        if isinstance(val, set):
            assert len(val) > 1
            val.add(path_new)
            val.remove(path_old)
        else:
            del self._inode_path_map[inode]
            self._inode_path_map[inode] = path_new

        refresh_degoo_content()

    async def mkdir(self, inode_p, name, mode, ctx):
        name = fsdecode(name)
        # Obtains the path to the directory where the new directory is to be created
        base_path = self._inode_to_path(inode_p)
        # Obtains the id of the directory where it will be created.
        element_id = self._get_degoo_id(base_path)

        log.debug('Creating directory \'%s\' in Degoo path \'%s\'', name, base_path)

        # It is created in Degoo
        new_dir_id = degoo.mkdir(name, element_id)

        new_dir_element = self._get_degoo_element_by_id(new_dir_id)

        # Get the attributes of new directory
        attr = self._get_degoo_attrs(new_dir_element['Name'])
        self._add_path(attr.st_ino, new_dir_element['FilePath'])

        return attr

    async def open(self, inode, flags, ctx):
        return pyfuse3.FileInfo(fh=inode)

    async def create(self, inode_p, name, mode, flags, ctx):
        path = self._get_temp_directory() + fsdecode(name)
        try:
            if os.path.exists(path):
                os.remove(path)
            fd = os.open(path, flags | os.O_CREAT | os.O_TRUNC)
        except OSError as exc:
            raise FUSEError(exc.errno)
        attr = self._getattr(fd=fd)
        self._add_path(attr.st_ino, path)
        self._inode_fd_map[attr.st_ino] = fd
        self._fd_inode_map[fd] = attr.st_ino
        self._fd_open_count[fd] = 1
        self._degoo_path[attr.st_ino] = self._inode_to_path(inode_p, fullpath=True)
        return pyfuse3.FileInfo(fh=fd, direct_io=True), attr

    async def read(self, fd, offset, length):
        global cache_thread_running

        path_file = self._inode_to_path(fd, fullpath=True)

        # Se obtiene el tipo del fichero
        is_media = self._is_media(path_file)

        if not is_media:
            url = degoo.get_url_file(path_file)

            if not url:
                raise pyfuse3.FUSEError(errno.ENOENT)

            http = urllib3.PoolManager()
            http.headers['Range'] = 'bytes=%s-%s' % (offset, offset + length - 1)
            r = http.request('GET', url)

            return r.data
        else:
            size_to_read = offset + length
            # Get the filename
            temp_filename = self._get_cached_filename(path_file, offset)

            # If it exists, it is cached
            if not os.path.isfile(temp_filename):
                self._cache_file(path_file, size_to_read)

            result = size_to_read // self._cache_size
            size = self._calculate_chunk_size(size_to_read)
            # It is checked if it is necessary to download a new part of the video to buffer it
            if size_to_read > size - (self._cache_size / 2):
                # If the thread has not been launched, it is executed
                if not cache_thread_running:
                    cache_thread_running = True
                    t1 = threading.Thread(target=self._cache_file, args=(path_file, size, ))
                    t1.start()

            file_descriptor = os.open(temp_filename, os.O_RDONLY)
            # If the reading is done from the same file
            if offset - (result * self._cache_size) >= 0:
                os.lseek(file_descriptor, offset - (result * self._cache_size), os.SEEK_SET)
                byte = os.read(file_descriptor, length)
            else:
                # Otherwise, there is a part that is read from one file, and the next from another
                part_offset = self._cache_size - ((result * self._cache_size) - offset)
                os.lseek(file_descriptor, part_offset, os.SEEK_SET)
                # The reading is made from where it corresponds to the end of the file
                byte = os.read(file_descriptor, self._cache_size - length)

                temp_filename = self._get_cached_filename(path_file, size_to_read)

                # All files are deleted, except the one to be read
                self._clear_files(path_file, skip_filename=temp_filename)

                # If the second file does not exist, it is downloaded before reading it
                if not os.path.isfile(temp_filename):
                    self._cache_file(path_file, size_to_read)

                # The rest of the contents of the following file are read
                file_descriptor = os.open(temp_filename, os.O_RDONLY)
                os.lseek(file_descriptor, 0, os.SEEK_SET)
                byte += os.read(file_descriptor, length - len(byte))

            return byte

    async def write(self, fd, offset, buf):
        os.lseek(fd, offset, os.SEEK_SET)
        length = os.write(fd, buf)

        if fd not in self._fd_buffer_length:
            self._fd_buffer_length[fd] = length

        if length != self._fd_buffer_length[fd]:
            inode = self._fd_inode_map[fd]
            source_file = self._inode_to_path(inode, fullpath=True)
            filename = source_file[source_file.rfind('/') + 1:]

            target_path = self._degoo_path[inode]
            log.debug('Uploading file [%s] to Degoo path [%s]', filename, target_path)

            degoo_id, path, URL = degoo.put(source_file, target_path)

            log.debug('Upload of file [%s] finished. Id [%s] Url [%s]', filename, degoo_id, URL)

            # Get the attributes of the new directory
            attr = self._get_degoo_attrs(path)
            self._add_path(attr.st_ino, path)

        return length

    async def release(self, fd):
        try:
            element = self._get_degoo_element_by_id(fd)
            filename = element['FilePath']
            if '/' in filename:
                filename = filename[filename.rfind('/') + 1:]

            self._clear_files(filename)

            return
        except:
            pass

        if self._fd_open_count[fd] > 1:
            self._fd_open_count[fd] -= 1
            return

        if fd in self._fd_buffer_length:
            del self._fd_buffer_length[fd]

        del self._fd_open_count[fd]
        inode = self._fd_inode_map[fd]
        del self._inode_fd_map[inode]
        del self._fd_inode_map[fd]
        try:
            os.close(fd)
        except OSError as exc:
            raise FUSEError(exc.errno)

        source_file = self._inode_to_path(inode, fullpath=True)

        del self._inode_path_map[inode]
        os.remove(source_file)

    def _is_media(self, filename):
        is_media_type = False
        if '/' in filename:
            filename = filename[filename.rfind('/') + 1:]

        mimetype_file = mimetypes.guess_type(filename)[0]
        if mimetype_file is not None:
            mimetype_file = mimetype_file.split('/')[0]

            if mimetype_file == 'audio' or mimetype_file == 'video':
                is_media_type = True
        return is_media_type

    def _calculate_chunk_size(self, size_to_read):
        result = size_to_read // self._cache_size
        return (result + 1) * self._cache_size

    def _get_cached_filename(self, degoo_path_file, size_to_read):
        size = self._calculate_chunk_size(size_to_read)
        return self._get_temp_file(degoo_path_file, size)

    def _cache_file(self, degoo_path_file, size_to_read):
        global cache_thread_running
        url = degoo.get_url_file(degoo_path_file)

        if not url:
            raise pyfuse3.FUSEError(errno.ENOENT)

        result = size_to_read // self._cache_size
        size = self._calculate_chunk_size(size_to_read)

        temp_filename = self._get_temp_file(degoo_path_file, size)
        # If the file has already been downloaded we exit
        if os.path.isfile(temp_filename):
            cache_thread_running = False
            return

        log.debug('Downloading [%s] filepart %d-%d', temp_filename, result * self._cache_size, size - 1)
        http = urllib3.PoolManager()
        http.headers['Range'] = 'bytes=%s-%s' % (result * self._cache_size, size - 1)
        r = http.request('GET', url)

        with open(temp_filename, 'wb') as out:
            out.write(r.data)

        cache_thread_running = False

    def _get_temp_directory(self):
        return tempfile.gettempdir() + os.sep

    def _get_temp_file(self, degoo_path_file, last_byte):
        filename = degoo_path_file
        if '/' in filename:
            filename = filename[filename.rfind('/') + 1:]
        name = filename[:filename.rfind('.')]
        extension = filename[filename.rfind('.') + 1:]
        temp_filename = name + '_' + str(last_byte) + '.' + extension

        return self._get_temp_directory() + temp_filename

    def _clear_files(self, filename, skip_filename=None):
        if '/' in filename:
            filename = filename[filename.rfind('/') + 1:]

        if '.' in filename:
            filename = filename[:filename.rfind('.')]

        if skip_filename and '/' in skip_filename:
            skip_filename = skip_filename[skip_filename.rfind('/') + 1:]

        for p in Path(self._get_temp_directory()).glob(filename + "*"):
            if p.name != skip_filename:
                log.debug('Removing part %s', p.name)
                p.unlink()


def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)


def parse_args(args):
    """Parse command line"""

    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('--degoo-path', type=str, default=PATH_ROOT_DEGOO,
                        help='Absolute path from Degoo. Default is ' + PATH_ROOT_DEGOO)
    parser.add_argument('--cache-size', type=int, default=10,
                        help='Size of downloaded piece of media files')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')
    parser.add_argument('--allow-other', action='store_true',
                        help='Allow access to another users')
    parser.add_argument('--refresh-interval', type=int, default=10,
                        help='Allow access to another users')
    parser.add_argument('--disable-refresh', action='store_true', default=False,
                        help='Disable automatic refresh')

    return parser.parse_args(args)


def get_degoo_content(refresh_interval):
    while True:
        time.sleep(refresh_interval)
        print('Loading Degoo content')
        refresh_degoo_content()


def refresh_degoo_content():
    global degoo_tree_content
    degoo_tree_content = degoo.tree_cache()


def main():
    options = parse_args(sys.argv[1:])
    init_logging(options.debug)

    cache_size = options.cache_size * 1024 * 1024
    degoo_path = options.degoo_path
    refresh_interval = options.refresh_interval * 60
    disable_refresh = options.disable_refresh

    log.debug('##### Initializating Degoo drive #####')
    log.debug('Cache size:          %s', str(cache_size) + ' MB')
    log.debug('Root Degoo path:     %s', degoo_path)
    log.debug('Refresh interval:    %s', 'Disabled' if disable_refresh else str(refresh_interval) + ' seconds')
    if options.allow_other:
        log.debug('User access:         %s', options.allow_other)

    log.debug('Reading Degoo content from directory %s', degoo_path)
    refresh_degoo_content()

    operations = Operations(degoo_path, cache_size)

    log.debug('Mounting...')
    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=fusedegoo')

    if options.allow_other:
        fuse_options.add('allow_other')

    mimetypes.init()
    if options.debug_fuse:
        fuse_options.add('debug')
    pyfuse3.init(operations, options.mountpoint, fuse_options)

    if not disable_refresh:
        t1 = threading.Thread(target=get_degoo_content, args=(refresh_interval, ))
        t1.start()

    try:
        log.debug('Entering main loop..')
        trio.run(pyfuse3.main)
    except:
        pyfuse3.close(unmount=True)
        raise

    log.debug('Unmounting..')
    pyfuse3.close()


if __name__ == '__main__':
    main()
