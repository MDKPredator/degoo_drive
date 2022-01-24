#!/usr/bin/env python3

import datetime
import errno
import faulthandler
import glob
import logging
import mimetypes
import os
import stat as stat_m
import sys
import tempfile
import threading
import time
from argparse import ArgumentParser
from collections import defaultdict
from os import fsencode, fsdecode
from urllib.parse import urlparse
from pathlib import Path

import pyfuse3
import requests
import trio
import urllib3
from pyfuse3 import FUSEError

import degoo

# to load the module from there first.
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
        os.path.exists(os.path.join(basedir, 'src', 'pyfuse3.pyx'))):
    sys.path.insert(0, os.path.join(basedir, 'src'))

faulthandler.enable()

log = logging.getLogger(__name__)

degoo_tree_content = {}

LOCAL_PATH_DEGOO = '/home/degoo'

PATH_ROOT_DEGOO = '/'

DEGOO_HOSTNAME_EU = 'c.degoo.eu'

percentage_read = 25

is_refresh_enabled = True

cache_thread_running = False

caching_file_list = []

threadLock = threading.Lock()

requests_control = []


class Operations(pyfuse3.Operations):
    enable_writeback_cache = True

    def __init__(self, source, cache_size, flood_sleep_time, flood_time_to_check, flood_max_requests,
                 enable_flood_control, change_hostname, mode):
        super().__init__()
        self._inode_path_map = {pyfuse3.ROOT_INODE: source}
        self._source = source
        self._lookup_cnt = defaultdict(lambda: 0)
        self._fd_inode_map = dict()
        self._inode_fd_map = dict()
        self._fd_open_count = dict()
        self._degoo_path = dict()
        self._fd_buffer_length = dict()
        self._cache_size = cache_size
        self._min_size_read_next_part = (percentage_read * self._cache_size) / 100
        # Waiting time before resuming requests once the maximum has been reached
        self._flood_sleep_time = flood_sleep_time
        # Request control period
        self._flood_time_to_check = flood_time_to_check
        # Maximum number of requests in the period set by the variable "_flood_time_to_check"
        self._flood_max_requests = flood_max_requests
        # Disable flood control
        self._enable_flood_control = enable_flood_control
        # Change hostname sent by Degoo for .eu
        self._change_hostname = change_hostname
        self._mode = mode

    def _set_id_root_degoo(self, id_degoo):
        self._id_root_degoo = id_degoo

    def _get_id_root_degoo(self):
        return self._id_root_degoo

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
        name = fsdecode(name)
        log.debug('lookup for %s in %d', name, inode_p)

        if inode_p == pyfuse3.ROOT_INODE:
            inode_p = self._get_id_root_degoo()

        children = self._get_degoo_childs(inode_p)
        attr = None

        for element in children:
            if name == element['Name']:
                attr = self._get_degoo_attrs(element['FilePath'])
                break

        if attr:
            return attr
        else:
            raise FUSEError(errno.ENOENT)

    async def getattr(self, inode, ctx=None):
        if inode in self._inode_fd_map:
            return self._getattr(fd=self._inode_fd_map[inode])
        else:
            return self._get_degoo_attrs(self._inode_to_path(inode, fullpath=True))

    async def setattr(self, inode, attr, fields, fh, ctx):
        return self._get_degoo_attrs(self._inode_to_path(inode, fullpath=True))

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
            element_id = self._get_id_root_degoo()
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

        if int(element['ID']) == self._get_id_root_degoo() or element['isFolder']:
            entry.st_size = 0
            entry.st_mode = (stat_m.S_IFDIR | 0o755)
        else:
            entry.st_size = int(element['Size'])
            entry.st_mode = (stat_m.S_IFREG | 0o664)

        entry.st_ino = int(element['ID'])
        entry.st_uid = os.getuid()
        entry.st_gid = os.getgid()
        entry.st_blksize = 512
        entry.st_blocks = ((entry.st_size + entry.st_blksize - 1) // entry.st_blksize)

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
        assert not (fd is None and path is None)
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
        entry.st_blocks = ((entry.st_size + entry.st_blksize - 1) // entry.st_blksize)

        return entry

    async def readdir(self, inode, off, token):
        path = self._inode_to_path(inode, fullpath=True)
        log.debug('reading %s', path)

        parent_id = self._get_degoo_id(path)
        children = self._get_degoo_childs(parent_id)

        # If dir has not children and it is lazy mode, degoo it is called to get the content
        if self._mode == 'lazy' and len(children) == 0:
            degoo.tree(dir_id=inode, mode=self._mode)
            self._refresh_path()
            children = self._get_degoo_childs(parent_id)

        entries = []
        for element in children:
            attr = self._get_degoo_attrs(element['FilePath'])
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

    async def rename(self, inode_p_old, name_old, inode_p_new, name_new,
                     flags, ctx):
        if flags != 0:
            raise FUSEError(errno.EINVAL)

        name_old = fsdecode(name_old)
        name_new = fsdecode(name_new)

        path = self._inode_to_path(inode_p_old, fullpath=True)
        path_old = path + '/' + name_old

        inode = self._get_degoo_id(path_old)

        if self._mode == 'lazy' and len(self._get_degoo_childs(inode_p_new)) == 0:
            degoo.tree(dir_id=inode_p_new, mode=self._mode)
            self._refresh_path()

        # It is a rename
        if inode_p_old == inode_p_new:
            degoo.rename(path_old, name_new)
        else:
            # If name it is different, it is a move with rename
            if name_old != name_new:
                degoo.rename(path_old, name_new)
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

            if isinstance(url, bytes):
                # If are bytes, then it is the content of the file
                return url

            try:
                response = requests.get(url, headers={
                    'Range': 'bytes=%s-%s' % (offset, offset + length - 1)
                })
                if response.status_code < 400:
                    return response.content
                else:
                    log.debug('Error trying to download file [%s]. Code [%s] Message [%s]', path_file,
                              str(response.status_code), response.content)
                    raise pyfuse3.FUSEError(errno.ENOENT)
            except urllib3.exceptions.HTTPError as e:
                log.debug('Error getting info for file [%s]: %s', path_file, str(e))
            except urllib3.exceptions.ReadTimeoutError as e:
                log.debug('Timeout for file [%s]: %s', path_file, str(e))
        else:
            degoo_file_size = self._get_degoo_element_by_id(fd)['Size']
            size_to_read = offset + length

            first_file_part = offset // self._cache_size
            second_file_part = first_file_part + 1

            # Get the filename
            temp_filename = self._get_temp_file(path_file, first_file_part)

            if not os.path.isfile(temp_filename):
                self._check_requests()
                self._cache_file(path_file, first_file_part, degoo_file_size)

            result = size_to_read // self._cache_size

            next_temp_filename = self._get_temp_file(path_file, second_file_part)

            # 1 - Check that at least a percentage of the file has been read before downloading the next piece of data
            # 2 - Check that the size to be read is smaller than the size of the file
            if self._min_size_read_next_part < size_to_read < degoo_file_size \
                    and not cache_thread_running and not os.path.isfile(next_temp_filename):
                self._check_requests()

                log.debug('Preparing to download next file [%s]', next_temp_filename)
                cache_thread_running = True
                caching_file_list.append(next_temp_filename)
                t1 = threading.Thread(target=self._cache_file, args=(path_file, second_file_part, degoo_file_size,))
                t1.start()

            if not os.path.isfile(temp_filename):
                raise pyfuse3.FUSEError(errno.ENOENT)

            file_descriptor = os.open(temp_filename, os.O_RDONLY)
            # If the reading is done from the same file
            if offset - (result * self._cache_size) >= 0:
                os.lseek(file_descriptor, offset - (result * self._cache_size), os.SEEK_SET)
                byte = os.read(file_descriptor, length)
                os.close(file_descriptor)
            else:
                log.debug('Reading first part from two files. File 1 [%s]', temp_filename)

                # Otherwise, there is a part that is read from one file, and the next from another
                part_offset = self._cache_size - ((result * self._cache_size) - offset)
                os.lseek(file_descriptor, part_offset, os.SEEK_SET)
                # The reading is made from where it corresponds to the end of the file
                byte = os.read(file_descriptor, self._cache_size - length)
                os.close(file_descriptor)

                log.debug('Reading second part from two files. File 2 [%s]', next_temp_filename)

                # All files are deleted, except the one to be read
                self._clear_files(path_file, skip_filename=next_temp_filename)

                retries = 0
                while next_temp_filename in caching_file_list and retries < 10:
                    log.debug('Waiting to read second part file [%s]', next_temp_filename)
                    retries += 1
                    time.sleep(0.5)

                if not os.path.isfile(next_temp_filename):
                    raise pyfuse3.FUSEError(errno.ENOENT)

                # The rest of the contents of the following file are read
                file_descriptor = os.open(next_temp_filename, os.O_RDONLY)
                os.lseek(file_descriptor, 0, os.SEEK_SET)
                byte += os.read(file_descriptor, length - len(byte))
                os.close(file_descriptor)

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
            if not URL:
                log.debug('WARN: file [%s] has not been uploaded successfully', filename)

            # Get the attributes of the new directory
            attr = self._get_degoo_attrs(path)
            self._add_path(attr.st_ino, path)

        return length

    async def release(self, fd):
        try:
            element = self._get_degoo_element_by_id(fd)
            filename = self._get_filename(element['FilePath'])

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

    def _check_requests(self):
        if self._enable_flood_control:
            global requests_control
            requests_control.append(datetime.datetime.now())
            self._control_requests_flood()

    def _control_requests_flood(self):
        global requests_control

        last_minute = datetime.datetime.now() - datetime.timedelta(minutes=self._flood_time_to_check)
        requests_control = [x for x in requests_control if x >= last_minute]
        number_of_requests = len(requests_control)
        log.debug('Number of requests made in %s minute(s): %s', str(self._flood_time_to_check),
                  str(number_of_requests))
        if number_of_requests > self._flood_max_requests:
            log.debug('Reached max of requests %s in %s minutes. Waiting %s seconds',
                      str(self._flood_max_requests), str(self._flood_time_to_check), str(self._flood_sleep_time))
            time.sleep(self._flood_sleep_time)

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

    def _cache_file(self, degoo_path_file, file_part, degoo_file_size):
        global cache_thread_running
        global caching_file_list

        url = degoo.get_url_file(degoo_path_file)

        if not url:
            raise pyfuse3.FUSEError(errno.ENOENT)

        url_parsed = urlparse(url)
        # It seems to go faster with the .eu domain
        if self._change_hostname and url_parsed.hostname != DEGOO_HOSTNAME_EU:
            log.debug('Changing hostname [%s] to [%s]', url_parsed.hostname, DEGOO_HOSTNAME_EU)
            url = url_parsed._replace(netloc=DEGOO_HOSTNAME_EU).geturl()

        size = (file_part * self._cache_size) + self._cache_size

        # For end of file, must by "degoo_file_size - (file_part * self._cache_size)", but Degoo returns 416.
        # With empty size returns from "file_part * self._cache_size" until the end
        size = size - 1 if size < degoo_file_size else ''

        temp_filename = self._get_temp_file(degoo_path_file, file_part)

        if not os.path.isfile(temp_filename):
            log.debug('Downloading [%s] filepart %d-%s', temp_filename, file_part * self._cache_size, str(size))
            try:
                response = requests.get(url, stream=True, headers={
                    'Range': 'bytes=%s-%s' % (file_part * self._cache_size, size)
                })

                if response.status_code < 400:
                    if response.status_code == 206:
                        read_bytes = response.content
                    else:
                        log.debug('WARN. File is returned completely by Degoo. Ignoring')
                        raise pyfuse3.FUSEError(errno.ENOENT)

                    with open(temp_filename, 'wb') as out:
                        out.write(read_bytes)

                    log.debug('Downloaded file [%s]', temp_filename)
                else:
                    log.debug('Error trying to download file [%s]. Code [%s] Message [%s]', temp_filename,
                              str(response.status_code), response.content)
            except requests.exceptions.ConnectionError as e:
                log.debug('Error getting info for file [%s]: %s', temp_filename, str(e))
            except urllib3.exceptions.ReadTimeoutError as e:
                log.debug('Timeout for file [%s]: %s', temp_filename, str(e))

        if temp_filename in caching_file_list:
            caching_file_list.remove(temp_filename)
        cache_thread_running = False

    def _get_temp_directory(self):
        return tempfile.gettempdir() + os.sep

    def _get_filename(self, path_file):
        filename = path_file
        if '/' in filename:
            filename = filename[filename.rfind('/') + 1:]

        return filename

    def _get_temp_file(self, degoo_path_file, filepart):
        filename = self._get_filename(degoo_path_file)
        name = filename[:filename.rfind('.')]
        extension = filename[filename.rfind('.') + 1:]
        temp_filename = name + '_' + str(filepart) + '.' + extension

        return self._get_temp_directory() + temp_filename

    def _clear_files(self, filename, skip_filename=None):
        filename = self._get_filename(filename)

        if '.' in filename:
            filename = filename[:filename.rfind('.')]

        if skip_filename and '/' in skip_filename:
            skip_filename = skip_filename[skip_filename.rfind('/') + 1:]

        filename = glob.escape(filename)
        for file in glob.glob(self._get_temp_directory() + filename + "*", recursive=False):
            if self._get_filename(file) != skip_filename:
                log.debug('Removing part %s', file)
                os.remove(file)

    def _refresh_path(self):
        for idx, degoo_element in degoo_tree_content.items():
            if self._source in degoo_element['FilePath']:
                attr = self._get_degoo_attrs(degoo_element['FilePath'])
                inode = attr.st_ino
                path = degoo_element['FilePath']

                # If path does not exist, it is added
                if inode not in self._inode_path_map:
                    self._add_path(inode, path)
                elif inode in self._inode_path_map and self._inode_path_map[inode] != path:
                    # If the element exists, but has changed its path
                    del self._inode_path_map[inode]
                    self._add_path(inode, path)

    def refresh_degoo_content(self, refresh_interval):
        while is_refresh_enabled:
            time.sleep(refresh_interval)
            log.debug('Loading Degoo content')
            self.load_degoo_content()
        log.debug('Refresh content finished')

    def load_degoo_content(self):
        threadLock.acquire()

        global degoo_tree_content
        degoo_tree_content = degoo.tree_cache(mode=self._mode)

        id_root_degoo = self._get_degoo_id(self._source)
        self._set_id_root_degoo(id_root_degoo)

        self._refresh_path()

        threadLock.release()


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

    parser.add_argument('--mountpoint', type=str, default=LOCAL_PATH_DEGOO,
                        help='Where to mount the file system. Default is ' + LOCAL_PATH_DEGOO)
    parser.add_argument('--degoo-email', type=str,
                        help='Email to login in Degoo')
    parser.add_argument('--degoo-pass', type=str,
                        help='Password to login in Degoo')
    parser.add_argument('--degoo-token', type=str,
                        help='Token for requests. Alternative if login fails')
    parser.add_argument('--degoo-refresh-token', type=str,
                        help='Used when token expires. Alternative if login fails')
    parser.add_argument('--degoo-path', type=str, default=PATH_ROOT_DEGOO,
                        help='Absolute path from Degoo. Default is ' + PATH_ROOT_DEGOO)
    parser.add_argument('--cache-size', type=int, default=15,
                        help='Size of downloaded piece of media files')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')
    parser.add_argument('--allow-other', action='store_true',
                        help='Allow access to another users')
    parser.add_argument('--refresh-interval', type=int, default=10,
                        help='Refresh deego content interval (default: 1 * 60sec')
    parser.add_argument('--disable-refresh', action='store_true', default=False,
                        help='Disable automatic refresh')
    parser.add_argument('--flood-sleep-time', action='store_true', default=60,
                        help='Waiting time, in seconds, before resuming requests once the maximum has been reached')
    parser.add_argument('--flood-max-requests', action='store_true', default=20,
                        help='Maximum number of requests in the period')
    parser.add_argument('--flood-time-to-check', action='store_true', default=1,
                        help='Request control period, in minutes')
    parser.add_argument('--enable-flood-control', action='store_true', default=False,
                        help='Disable flood control')
    parser.add_argument('--change-hostname', action='store_true', default=False,
                        help='Disable change domain for media files')
    parser.add_argument('--mode', type=str, default='lazy',
                        help='How content is read. Default is lazy')
    parser.add_argument('--config-path', type=str,
                        help='Path to the configuration files. Default is ~/.config/degoo/ (useful for setting up '
                             'multiple accounts at the same time, for example)')

    return parser.parse_args(args)


def main():
    options = parse_args(sys.argv[1:])
    init_logging(options.debug)

    cache_size = options.cache_size * 1024 * 1024
    degoo_email = options.degoo_email
    degoo_pass = options.degoo_pass
    degoo_token = options.degoo_token
    degoo_refresh_token = options.degoo_refresh_token
    degoo_path = options.degoo_path
    refresh_interval = options.refresh_interval * 60
    disable_refresh = options.disable_refresh
    enable_flood_control = options.enable_flood_control
    change_hostname = options.change_hostname
    mode = options.mode
    config_path = options.config_path

    log.debug('##### Initializating Degoo drive #####')
    log.debug('Local mount point:   %s', options.mountpoint)
    log.debug('Cache size:          %s', str(cache_size) + ' kb')
    if degoo_email and degoo_pass:
        log.debug('Degoo email:         %s', degoo_email)
        log.debug('Degoo pass:          %s', '*'*len(degoo_pass))
    if degoo_token and degoo_refresh_token:
        log.debug('Degoo token:         %s', '*'*len(degoo_token[:10]))
        log.debug('Degoo refresh token: %s', '*'*len(degoo_refresh_token[:10]))
    log.debug('Root Degoo path:     %s', degoo_path)
    log.debug('Refresh interval:    %s', 'Disabled' if disable_refresh else str(refresh_interval) + ' seconds')
    log.debug('Flood control:       %s', 'Enabled' if enable_flood_control else 'Disabled')
    if enable_flood_control:
        log.debug('Flood sleep time:    %s seconds', str(options.flood_sleep_time))
        log.debug('Flood max requests:  %s', str(options.flood_max_requests))
        log.debug('Flood time check:    %s minute(s)', str(options.flood_time_to_check))
    log.debug('Change hostname:     %s', 'Disabled' if not change_hostname else DEGOO_HOSTNAME_EU)
    log.debug('Mode:                %s', mode)
    if config_path:
        log.debug('Configuration path:  %s', config_path)

    if options.allow_other:
        log.debug('User access:         %s', options.allow_other)

    Path(options.mountpoint).mkdir(parents=True, exist_ok=True)

    operations = Operations(source=degoo_path, cache_size=cache_size, flood_sleep_time=options.flood_sleep_time,
                            flood_time_to_check=options.flood_time_to_check,
                            flood_max_requests=options.flood_max_requests, enable_flood_control=enable_flood_control,
                            change_hostname=change_hostname, mode=mode)

    log.debug('Reading Degoo content from directory %s', degoo_path)

    degoo.DegooConfig(config_path, email=degoo_email, password=degoo_pass,
                      token=degoo_token, refresh_token=degoo_refresh_token)
    degoo.API()
    operations.load_degoo_content()

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
        t1 = threading.Thread(target=operations.refresh_degoo_content, args=(refresh_interval,))
        t1.start()

    try:
        log.debug('Entering main loop..')
        trio.run(pyfuse3.main)
    except:
        print('Unexpected error: ', sys.exc_info()[0])
        global is_refresh_enabled
        is_refresh_enabled = False
        pyfuse3.close(unmount=True)
        raise

    log.debug('Unmounting..')
    pyfuse3.close()


if __name__ == '__main__':
    main()
